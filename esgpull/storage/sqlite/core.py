from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field

import sqlalchemy as sa
import sqlalchemy.orm

import esgpull
from esgpull.utils import Semver
from esgpull.storage.sqlite.types import Registry, Engine, Session
from esgpull.storage.sqlite.tables import (
    Version,
    File,
    Dataset,
    Param,
    Table,
)


# TODO: finish this implementation with `rebuild_from_filesystem`
@dataclass
class SqliteStorage:
    """
    Main class to interact with esgpull's sqlite storage.
    """

    path: str = "sqlite:////home/srodriguez/ipsl/data/synda/db/sdt_copy.db"
    mapper: Registry = field(default_factory=sa.orm.registry)
    verbosity: int = 0
    semver: Semver = field(default=Semver(3))  # Placeholder

    def __post_init__(self) -> None:
        self.setup_verbosity()
        self.setup_path()
        self.engine: Engine = sa.create_engine(self.path, future=True)
        session_cls = sa.orm.sessionmaker(bind=self.engine, future=True)
        self.session: Session = session_cls()
        self.Version: Table = Version.map(self.mapper, self.semver)

        # inspector = sa.inspect(self.engine)
        # if not inspector.has_table("version"):
        #     self.semver: Semver = Semver(3, 35)  # Placeholder
        #     if False:
        #         self.semver: Semver = esgpull.semver
        #     self.Version.create_table(self.session)
        #     current = self.Version(version=str(self.semver))
        #     print(current)
        #     # self.Version.insert(current, self.session)
        # else:
        #     self.semver: Semver = self.Version.get_version(self.session)
        # self.check_semver_against_module()

        self.File: Table = File.map(self.mapper, self.semver)
        self.Dataset: Table = Dataset.map(self.mapper, self.semver)
        self.Param: Table = Param.map(self.mapper, self.semver)

        self.tables: list[Table] = [
            self.Version,
            self.File,
            self.Dataset,
            self.Param,
        ]
        self.create_missing_tables()

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
        else:
            assert os.path.exists(self.path.removeprefix(prefix))

    def check_semver_against_module(self) -> None:
        if False:
            if esgpull.__semver__ != self.semver:
                raise Exception(
                    "Module is out of sync with database schema, migrate?"
                )

    def create_missing_tables(self) -> None:
        inspector = sa.inspect(self.engine)
        for table in self.tables:
            if not inspector.has_table(table.__table__.name):
                table.create_table(self.session)
