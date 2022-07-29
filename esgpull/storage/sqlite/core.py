from __future__ import annotations
from typing import Any, Callable, Type

# import os
import logging
from pathlib import Path
from contextlib import contextmanager
from dataclasses import dataclass

import sqlalchemy as sa
import sqlalchemy.orm
import alembic.config
import alembic.command
from alembic.migration import MigrationContext

import esgpull
from esgpull.storage.sqlite.types import (
    Engine,
    Session,
    Row,
    Result,
    SelectStmt,
)
from esgpull.storage.sqlite.tables import (
    Status,
    Table,
    Version,
    File,
    Param,
)


class SelectContext:
    """
    Interface to simplify `sqlalchemy.select` usage with custom
    `SqliteStorage` objects.

    The query must start with a `select` method to register an initial
    statement in the context. Any new `select` will erase previous statements.
    After that, any regular sqlalchemy method can be used to further refine the
    statement, using `ctx.<sqlalchemy-method>(...)`.
    Operations can also be chained, the same way as in sqlalchemy.

    Tables are copied as context attributes to enable shorter syntax.

    Example:
        ```python
        from esgpull.storage.sqlite import SqliteStorage, SelectContext
        from esgpull.utils import naturalsize

        storage = SqliteStorage(...)


        with storage.select(storage.Version.version) as stmt:
            print("version: ", stmt.scalar)

        with storage.select(stmt.File.file_id, stmt.File.size) as stmt:
            stmt.where(storage.File.file_id >= 1)
            for id, size in stmt.result:
                print(f"id: {id}, size: {naturalsize(size)})")

        with storage.select(storage.Param) as stmt:
            for param in stmt.where(storage.Param.name.like("%ess")).scalars:
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

        # for name, table in storage.tables.items():
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


@dataclass
class SqliteStorage:
    """
    Main class to interact with esgpull's sqlite storage.
    """

    path: str = "sqlite:////home/srodriguez/ipsl/data/synda/db/sdt_new.db"
    verbosity: int = 0

    def __post_init__(self) -> None:
        self.setup_verbosity()
        self.setup_path()
        self.engine: Engine = sa.create_engine(self.path, future=True)
        session_cls = sa.orm.sessionmaker(bind=self.engine, future=True)
        self.session: Session = session_cls()

        self.Version = Version
        self.File = File
        self.Param = Param
        self.tables: dict[str, Type[Table]] = {
            "Version": self.Version,
            "File": self.File,
            "Param": self.Param,
        }
        self.update_db()

    def setup_verbosity(self) -> None:
        logging.basicConfig()
        engine = logging.getLogger("sqlalchemy.engine")
        engine_lvl = logging.NOTSET
        match self.verbosity:
            case 0:
                ...
            case 1:
                engine_lvl = logging.INFO
            case 2:
                engine_lvl = logging.DEBUG
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

    def update_db(self) -> None:
        pkg_version = esgpull.__version__
        with self.engine.begin() as conn:
            opts = {"version_table": "version"}
            ctx = MigrationContext.configure(conn, opts=opts)
            revision = ctx.get_current_revision()
        config_path = Path(esgpull.__file__).parent.parent / "alembic.ini"
        config = alembic.config.Config(str(config_path))
        if revision != pkg_version:
            alembic.command.upgrade(config, pkg_version)

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

    def install(self, files: list[File]) -> None:
        to_install = []
        for file in files:
            with self.select(File) as sel:
                sel.where(File.file_id == file.file_id)
                if len(sel.scalars) == 0:
                    file.status = Status.waiting
                    to_install.append(file)
        self.session.add_all(to_install)
        self.session.commit()
        print(f"Installed {len(to_install)} new files.")

    def get_files_with_status(self, status: Status) -> list[File]:
        with self.select(File) as sel:
            return sel.where(File.status == status).scalars
