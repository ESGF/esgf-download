from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

import alembic.command
import sqlalchemy as sa
import sqlalchemy.orm
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from attrs import define, field

from esgpull import __file__
from esgpull.db.models import File, FileStatus, Table
from esgpull.db.utils import SelectContext, Session
from esgpull.query import Query
from esgpull.settings import Settings
from esgpull.version import __version__


@define
class Database:
    """
    Main class to interact with esgpull's sqlite db.
    """

    url: str
    verbosity: int = 0
    dry_run: bool = False
    version: str | None = field(init=False, default=None)
    engine: sa.Engine = field(init=False)
    _session: sa.orm.Session = field(init=False)

    @staticmethod
    def from_settings(settings: Settings, dry_run: bool = False) -> Database:
        return Database.from_path(
            settings.core.paths.db,
            settings.core.db_filename,
            settings.db.verbosity,
            dry_run,
        )

    @staticmethod
    def from_path(
        path: Path,
        filename: str = "esgpull.db",
        verbosity: int = 0,
        dry_run: bool = False,
    ) -> Database:
        url = f"sqlite:///{path / filename}"
        return Database(url, verbosity, dry_run)

    def __attrs_post_init__(self) -> None:
        self.apply_verbosity(self.verbosity)
        self.engine = sa.create_engine(self.url, future=True)
        sessionmaker: sa.orm.sessionmaker = sa.orm.sessionmaker(
            bind=self.engine, future=True
        )
        self._session = sessionmaker()
        if not self.dry_run:
            self.update()

    def apply_verbosity(self, verbosity: int) -> None:
        logging.basicConfig()
        engine = logging.getLogger("sqlalchemy.engine")
        if verbosity == 1:
            engine_lvl = logging.INFO
        elif verbosity == 2:
            engine_lvl = logging.DEBUG
        else:
            engine_lvl = logging.NOTSET
        engine.setLevel(engine_lvl)

    def update(self) -> None:
        config = Config()
        migrations_path = Path(__file__).parent / "db/migrations"
        config.set_main_option("script_location", str(migrations_path))
        config.attributes["connection"] = self.engine
        script = ScriptDirectory.from_config(config)
        head = script.get_current_head()
        with self.engine.begin() as conn:
            opts = {"version_table": "version"}
            ctx = MigrationContext.configure(conn, opts=opts)
            self.version = ctx.get_current_revision()
        if self.version != head:
            alembic.command.upgrade(config, __version__)
            self.version = head
        if self.version != __version__:
            alembic.command.revision(
                config,
                message="update tables",
                autogenerate=True,
                rev_id=__version__,
            )
            self.version = __version__

    @contextmanager
    def get_session(self) -> Iterator[Session]:
        try:
            yield self._session
        except (Exception, KeyboardInterrupt):
            self._session.rollback()
            raise
        else:
            self._session.commit()

    @contextmanager
    def select(self, *selectable):
        with self.get_session() as session:
            try:
                yield SelectContext(session, *selectable)
            finally:
                ...

    def add(self, *items: Table) -> None:
        with self.get_session() as session:
            for item in items:
                session.add(item)

    def delete(self, *items: Table) -> None:
        with self.get_session() as session:
            for item in items:
                session.delete(item)
        for item in items:
            sa.orm.session.make_transient(item)

    def has(
        self,
        /,
        file: File | None = None,
        filepath: str | Path | None = None,
    ) -> bool:
        if file is not None:
            clause = File.file_id == file.file_id
        elif filepath is not None:
            if isinstance(filepath, str):
                filepath = Path(filepath)
            # TODO: verify format assumption: a/b/c/../<version>/<filename>
            filename = filepath.name
            version = filepath.parent.name
            clause = (File.filename == filename) & (File.version == version)
        else:
            raise ValueError("TODO: custom error")
        with self.select(File) as sel:
            matching = sel.where(clause).scalars
        return any(matching)

    def search(
        self,
        query: Query | None = None,
        statuses: Sequence[FileStatus] | None = None,
        file_ids: Sequence[int] | None = None,
    ) -> list[File]:
        clauses: list[sa.ColumnElement] = []
        if query is None and statuses is None and file_ids is None:
            raise ValueError("TODO: custom error")
        if statuses is not None:
            clauses.append(File.status.in_(statuses))
        if query is not None:
            for q in query.flatten():
                query_clauses = []
                for facet in q:
                    # values are in a list, to keep support for CMIP5
                    # search by first value only is supported for now
                    facet_clause = sa.func.json_extract(
                        File.raw, f"$.{facet.name}[0]"
                    ).in_(list(facet.values))
                    query_clauses.append(facet_clause)
                if query_clauses:
                    clauses.append(sa.and_(*query_clauses))
        if file_ids is not None:
            clauses.append(File.id.in_(file_ids))
        if clauses:
            with self.select(File) as sel:
                result = sel.where(sa.or_(*clauses)).scalars
        else:
            result = []
        return result

    def get_deprecated_files(self) -> list[File]:
        with (self.select(File) as query, self.select(File) as subquery):
            subquery.group_by(File.master_id)
            subquery.having(sa.func.count("*") > 1).alias()
            join_clause = File.master_id == subquery.stmt.c.master_id
            duplicates = query.join(subquery.stmt, join_clause).scalars
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


__all__ = ["Database"]
