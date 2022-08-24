from __future__ import annotations
from typing import Any, Callable, Type, TypeAlias, Optional

# import os
import logging
from pathlib import Path
from functools import reduce
from contextlib import contextmanager

import sqlalchemy as sa
import sqlalchemy.orm
import alembic.config
import alembic.command
from alembic.migration import MigrationContext

import esgpull
from esgpull.query import Query
from esgpull.types import (
    Status,
    Version,
    Param,
    File,
    Table,
)


Row: TypeAlias = sa.engine.row.Row
Registry: TypeAlias = sa.orm.registry
Engine: TypeAlias = sa.future.engine.Engine
Session: TypeAlias = sa.orm.session.Session
Result: TypeAlias = sa.engine.result.Result
Columns: TypeAlias = Optional[list[sa.Column | sa.UniqueConstraint]]
SelectStmt: TypeAlias = sa.sql.selectable.Select

Mapper = sa.orm.registry()

TABLES: dict[str, list[sa.Column | sa.Constraint]] = dict(
    version=[sa.Column("version_num", sa.String(32), primary_key=True)],
    param=[
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(50), nullable=False),
        sa.Column("value", sa.String(255), nullable=False),
        sa.Column(
            "last_updated",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
        sa.UniqueConstraint("name", "value"),
    ],
    file=[
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("file_id", sa.Text, unique=True, nullable=False),
        sa.Column("dataset_id", sa.Text),
        sa.Column("url", sa.Text),
        sa.Column("version", sa.String(16)),
        sa.Column("filename", sa.String(255)),
        sa.Column("local_path", sa.String(255)),
        sa.Column("data_node", sa.String(40)),
        sa.Column("checksum", sa.String(64)),
        sa.Column("checksum_type", sa.String(16)),
        sa.Column("size", sa.Integer),
        sa.Column("status", sa.Enum(Status)),
        sa.Column("metadata", sa.JSON),
        # sa.Column("start_date", sa.Text),
        # sa.Column("end_date", sa.Text),
        # sa.Column(
        #     "dataset_id",
        #     sa.Integer,
        #     sa.ForeignKey("dataset.dataset_id"),
        #     nullable=False,
        # ),
        # sa.Column(
        #     "raw_url",
        #     sa.Text,
        #     sa.Computed(
        #         "json_extract(metadata,'$.url[0]')",
        #         persisted=False,
        #     ),
        # ),
        # sa.Column(
        #     "raw_url_delim_pos",
        #     sa.Integer,
        #     sa.Computed("instr(raw_url, '|')", persisted=False),
        # ),
        # sa.Column(
        #     "url",
        #     sa.Text,
        #     sa.Computed(
        #         "substr(raw_url, 1, raw_url_delim_pos-1)",
        #         persisted=True,
        #     ),
        # ),
        sa.Column(
            "last_updated",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
        ),
    ],
)


def map_table(name: str, table_type: Type[Table]) -> None:
    table = sa.Table(name, Mapper.metadata, *TABLES[name])
    Mapper.map_imperatively(table_type, table)


map_table("version", Version)
map_table("param", Param)
map_table("file", File)


class SelectContext:
    """
    Interface to simplify `sqlalchemy.select` usage with custom
    `Database` objects.

    The query must start with a `select` method to register an initial
    statement in the context. Any new `select` will erase previous statements.
    After that, any regular sqlalchemy method can be used to further refine the
    statement, using `ctx.<sqlalchemy-method>(...)`.
    Operations can also be chained, the same way as in sqlalchemy.

    Tables are copied as context attributes to enable shorter syntax.

    Example:
        ```python
        from esgpull.db.core import Database, SelectContext
        from esgpull.utils import naturalsize

        db = Database(...)


        with db.select(db.Version.version) as stmt:
            print("version: ", stmt.scalar)

        with db.select(stmt.File.file_id, stmt.File.size) as stmt:
            stmt.where(db.File.file_id >= 1)
            for id, size in stmt.result:
                print(f"id: {id}, size: {naturalsize(size)})")

        with db.select(db.Param) as stmt:
            for param in stmt.where(db.Param.name.like("%ess")).scalars:
                print(param)

        # version:  3.10
        # id: 1, size: 1.9 GiB
        # id: 2, size: 2.2 GiB
        # id: 3, size: 2.2 GiB
        # Param(id=1, name='access', value='Globus', last_updated=None)
        # Param(id=2, name='access', value='GridFTP', last_updated=None)
        # Param(id=3, name='access', value='HTTPServer', last_updated=None)
        # Param(id=4, name='access', value='LAS', last_updated=None)
        # Param(id=5, name='access', value='OPENDAP', last_updated=None)
        ```
    """

    def __init__(self, session: Session, *tables: Type[Table]) -> None:
        self.session = session
        self.stmt: SelectStmt = sa.select(*tables)

        # for name, table in db.tables.items():
        #     setattr(self, name, table)

    def __getattr__(self, attr) -> Callable[..., SelectContext]:
        """
        Enables chaining operations on `self.stmt`.

        Methods of `CompoundSelect` that are called on `self.stmt` will be
        called inside `stmt_setter`, such that the stmt is registered in the
        current context.
        which exists simply to allow `()` after
        python's `__getattr__` which only
        stmt_setter(...) is returned used to catch `*args/**kwargs`

        Looks complex but isn't really...
        """
        assert self.stmt is not None

        def stmt_setter(*a, **kw):
            self.stmt = getattr(self.stmt, attr)(*a, **kw)
            return self

        return stmt_setter

    def execute(self) -> Result:
        """
        Execute and returns the context's statement.
        """
        assert self.stmt is not None
        return self.session.execute(self.stmt)

    @property
    def results(self) -> list[Row]:
        """
        Returns statement's result as a list of sqlalchemy rows.
        """
        return self.execute().all()

    @property
    def scalars(self) -> list[Any]:
        """
        Returns statement's result as a list of sqlalchemy scalars.
        """
        return self.execute().scalars().all()

    @property
    def scalar(self) -> Any:
        """
        Returns statement's result as a single sqlalchemy scalar.
        """
        result = self.scalars
        assert len(result) == 1
        return result[0]


class Database:
    """
    Main class to interact with esgpull's sqlite db.
    """

    def __init__(self, path: str | Path, verbosity: int = 0) -> None:
        self.path = str(path)
        self.verbosity = verbosity
        self.setup_path()
        self.apply_verbosity()
        self.engine: Engine = sa.create_engine(self.path, future=True)
        session_cls = sa.orm.sessionmaker(bind=self.engine, future=True)
        self.session: Session = session_cls()

        self.Version = Version
        self.File = File
        self.Param = Param
        self.update()

    def apply_verbosity(self) -> None:
        logging.basicConfig()
        engine = logging.getLogger("sqlalchemy.engine")
        if self.verbosity == 1:
            engine_lvl = logging.INFO
        elif self.verbosity == 2:
            engine_lvl = logging.DEBUG
        else:
            engine_lvl = logging.NOTSET
        engine.setLevel(engine_lvl)

    def setup_path(self) -> None:
        prefix = "sqlite:///"
        if not self.path:
            self.path = prefix[:-1]  # memory path is `sqlite://`
        elif not self.path.startswith(prefix):
            # assert os.path.exists(self.path)
            self.path = prefix + self.path
        # else:
        #     assert os.path.exists(self.path.removeprefix(prefix))

    def update(self) -> None:
        # TODO: check remaining options in alembic.ini (keep?)
        pkg_path = Path(esgpull.__file__).parent.parent
        migrations_path = str(pkg_path / "migrations")
        pkg_version = esgpull.__version__
        with self.engine.begin() as conn:
            opts = {"version_table": "version"}
            ctx = MigrationContext.configure(conn, opts=opts)
            self.version = ctx.get_current_revision()
        config = alembic.config.Config()
        config.set_main_option("sqlalchemy.url", self.path)
        config.set_main_option("script_location", migrations_path)
        if self.version != pkg_version:
            alembic.command.upgrade(config, pkg_version)
            self.version = pkg_version
        # from alembic.script import ScriptDirectory
        # from alembic.runtime.environment import EnvironmentContext
        # script = ScriptDirectory.from_config(alembic_config)
        # with EnvironmentContext(
        #     alembic_config,
        #     script,
        #     fn=my_function,
        #     destination_rev=pkg_version,
        # ):
        #     script.run_env()

    @contextmanager
    def select(self, *selectable):
        try:
            yield SelectContext(self.session, *selectable)
        finally:
            ...

    def add(self, *items: Table) -> None:
        for item in items:
            self.session.add(item)
        self.session.commit()

    def delete(self, *items: Table) -> None:
        for item in items:
            self.session.delete(item)
        self.session.commit()
        for item in items:
            sa.orm.session.make_transient(item)

    def get_files_with_status(self, status: Status) -> list[File]:
        with self.select(File) as sel:
            return sel.where(File.status == status).scalars

    def has(self, /, file: File = None, filepath: Path = None) -> bool:
        if file is not None:
            table = File
            condition = File.file_id == file.file_id
        elif filepath is not None:
            table = File
            condition = File.filename == filepath.name
        else:
            raise ValueError
        with self.select(table) as sel:
            matching = sel.where(condition).scalars
        return any(matching)

    def search(self, query: Query) -> list[File]:
        clauses = []
        for q in query.flatten():
            query_clauses = []
            for facet in q:
                # values are in a list, to keep support for CMIP5
                # search by first value only is supported for now
                facet_clause = sa.func.json_extract(
                    File.metadata, f"$.{facet.name}[0]"
                ).in_(facet.values)
                query_clauses.append(facet_clause)
            clauses.append(reduce(sa.and_, query_clauses))
        if clauses:
            with self.select(File) as sel:
                return sel.where(reduce(sa.or_, clauses)).scalars
        else:
            return []


__all__ = ["Database"]
