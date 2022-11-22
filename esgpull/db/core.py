from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Sequence

import alembic.command
import sqlalchemy as sa
import sqlalchemy.orm
from alembic.config import Config as AlembicConfig
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from attrs import define, field
from sqlalchemy.orm import Session

from esgpull import __file__
from esgpull.config import Config
from esgpull.db.models import File, FileStatus, Table
from esgpull.db.utils import SelectContext
from esgpull.exceptions import NoClauseError
from esgpull.query import Query
from esgpull.version import __version__


@define
class Database:
    """
    Main class to interact with esgpull's sqlite db.
    """

    url: str
    run_migrations: bool = True
    version: str | None = field(init=False, default=None)
    engine: sa.Engine = field(init=False)

    @staticmethod
    def from_config(config: Config, run_migrations: bool = True) -> Database:
        url = f"sqlite:///{config.paths.db / config.db.filename}"
        return Database(url, run_migrations)

    def __attrs_post_init__(self) -> None:
        self.engine = sa.create_engine(self.url)
        if self.run_migrations:
            self.update()

    def update(self) -> None:
        config = AlembicConfig()
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
    def session(self) -> Iterator[Session]:
        with Session(self.engine) as s:
            try:
                yield s
            except (Exception, KeyboardInterrupt):
                s.rollback()
                raise

    @contextmanager
    def select(self, *selectable):
        with self.session() as session:
            yield SelectContext(session, *selectable)

    def add(self, *items: Table) -> None:
        with self.session() as session:
            for item in items:
                session.add(item)
            session.commit()
            for item in items:
                session.refresh(item)

    def delete(self, *items: Table) -> None:
        with self.session() as session:
            for item in items:
                session.delete(item)
            session.commit()
        # for item in items:
        #     sa.orm.session.make_transient(item)

    def has(
        self,
        /,
        file: File | None = None,
        filepath: Path | None = None,
    ) -> bool:
        if file is not None:
            clause = File.file_id == file.file_id
        elif filepath is not None:
            local_path = str(filepath.parent)
            filename = filepath.name
            local_path_clause = File.local_path == local_path
            filename_clause = File.filename == filename
            clause = local_path_clause & filename_clause
        else:
            raise ValueError("TODO: custom error")
        with self.select(File) as sel:
            matching = sel.where(clause).scalars
        return any(matching)

    def search(
        self,
        query: Query | None = None,
        statuses: Sequence[FileStatus] | None = None,
        ids: Sequence[int] | None = None,
    ) -> list[File]:
        clauses: list[sa.ColumnElement] = []
        if not statuses and not query and not ids:
            raise ValueError("TODO: custom error")
        if statuses:
            clauses.append(File.status.in_(statuses))
        if query:
            query_clauses = []
            for flat in query.flatten():
                flat_clauses = []
                for facet in flat:
                    # values are in a list, to keep support for CMIP5
                    # search by first value only is supported for now
                    facet_clause = sa.func.json_extract(
                        File.raw, f"$.{facet.name}[0]"
                    ).in_(list(facet.values))
                    flat_clauses.append(facet_clause)
                if flat_clauses:
                    query_clauses.append(sa.and_(*flat_clauses))
            if query_clauses:
                clauses.append(sa.or_(*query_clauses))
        if ids:
            clauses.append(File.id.in_(ids))
        if not clauses:
            raise NoClauseError()
        with self.select(File) as sel:
            return sel.where(sa.and_(*clauses)).scalars

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
