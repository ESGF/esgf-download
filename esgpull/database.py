from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field
from pathlib import Path
from typing import TypeVar

import alembic.command
import sqlalchemy as sa
import sqlalchemy.orm
from alembic.config import Config as AlembicConfig
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.orm import Session, joinedload, make_transient

from esgpull import __file__
from esgpull.config import Config
from esgpull.models import File, Query, Table, sql
from esgpull.version import __version__

# from esgpull.exceptions import NoClauseError

T = TypeVar("T")


@dataclass
class Database:
    """
    Main class to interact with esgpull's sqlite db.
    """

    url: str
    run_migrations: InitVar[bool] = True
    _engine: sa.Engine = field(init=False)
    session: Session = field(init=False)
    version: str | None = field(init=False, default=None)

    @staticmethod
    def from_config(config: Config, run_migrations: bool = True) -> Database:
        url = f"sqlite:///{config.paths.db / config.db.filename}"
        return Database(url, run_migrations=run_migrations)

    def _setup_sqlite(self, conn, record):
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode = WAL;")
        cursor.execute("PRAGMA synchronous = NORMAL;")
        cursor.execute("PRAGMA cache_size = 20000;")
        cursor.close()

    def __post_init__(self, run_migrations: bool) -> None:
        self._engine = sa.create_engine(self.url)
        sa.event.listen(self._engine, "connect", self._setup_sqlite)
        self.session = Session(self._engine)
        if run_migrations:
            self._update()

    def _update(self) -> None:
        alembic_config = AlembicConfig()
        migrations_path = Path(__file__).parent / "migrations"
        alembic_config.set_main_option("script_location", str(migrations_path))
        alembic_config.attributes["connection"] = self._engine
        script = ScriptDirectory.from_config(alembic_config)
        head = script.get_current_head()
        with self._engine.begin() as conn:
            opts = {"version_table": "version"}
            ctx = MigrationContext.configure(conn, opts=opts)
            self.version = ctx.get_current_revision()
        if head is not None and self.version != head:
            alembic.command.upgrade(alembic_config, head)
            self.version = head
        if "+dev" not in __version__ and self.version != __version__:
            alembic.command.revision(
                alembic_config,
                message="update tables",
                autogenerate=True,
                rev_id=__version__,
            )
            self.version = __version__

    @property
    @contextmanager
    def safe(self) -> Iterator[None]:
        try:
            yield
        except (sa.exc.SQLAlchemyError, KeyboardInterrupt):
            self.session.rollback()
            raise

    @contextmanager
    def commit_context(self) -> Iterator[None]:
        with self.safe:
            yield
            self.session.commit()

    def get(
        self,
        table: type[Table],
        sha: str,
        lazy: bool = True,
        detached: bool = False,
    ) -> Table | None:
        if lazy:
            result = self.session.get(table, sha)
        else:
            stmt = sa.select(table).filter_by(sha=sha)
            match self.scalars(stmt.options(joinedload("*")), unique=True):
                case [result]:
                    ...
                case []:
                    result = None
                case [*many]:
                    raise ValueError(f"{len(many)} found, expected 1.")
        if detached and result is not None:
            result = table(**result.asdict())
        return result

    def scalars(
        self, statement: sa.Select[tuple[T]], unique: bool = False
    ) -> Sequence[T]:
        with self.safe:
            result = self.session.scalars(statement)
            if unique:
                result = result.unique()
            return result.all()

    SomeTuple = TypeVar("SomeTuple", bound=tuple)

    def rows(self, statement: sa.Select[SomeTuple]) -> list[sa.Row[SomeTuple]]:
        with self.safe:
            return list(self.session.execute(statement).all())

    def add(self, *items: Table) -> None:
        with self.safe:
            self.session.add_all(items)
            self.session.commit()
            for item in items:
                self.session.refresh(item)

    def delete(self, *items: Table) -> None:
        with self.safe:
            for item in items:
                self.session.delete(item)
            self.session.commit()
        for item in items:
            make_transient(item)

    def link(self, query: Query, file: File):
        self.session.execute(sql.query_file.link(query, file))

    def unlink(self, query: Query, file: File):
        self.session.execute(sql.query_file.unlink(query, file))

    def __contains__(self, item: Table) -> bool:
        return self.scalars(sql.count(item))[0] > 0

    def has_file_id(self, file: File) -> bool:
        return len(self.scalars(sql.file.with_file_id(file.file_id))) == 1

    def merge(self, item: Table, commit: bool = False) -> Table:
        with self.safe:
            result = self.session.merge(item)
            if commit:
                self.session.commit()
        return result

    def get_deprecated_files(self) -> list[File]:
        duplicates = self.scalars(sql.file.duplicates())
        duplicates_dict: dict[str, list[File]] = {}
        for file in duplicates:
            duplicates_dict.setdefault(file.master_id, [])
            duplicates_dict[file.master_id].append(file)
        deprecated: list[File] = []
        for files in duplicates_dict.values():
            versions = [int(f.version[1:]) for f in files]
            latest_version = "v" + str(max(versions))
            for file in files:
                if file.version != latest_version:
                    deprecated.append(file)
        return deprecated
