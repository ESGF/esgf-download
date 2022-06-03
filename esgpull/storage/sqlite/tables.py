from __future__ import annotations
from typing import Optional
from dataclasses import dataclass, field

import sqlalchemy as sa
from datetime import datetime

from esgpull.storage.sqlite.types import (
    Table,
    Session,
    Columns,
)
from esgpull.utils import Semver


@dataclass
class Version(Table):
    version: str
    # id: Optional[int] = field(init=False, default=None)
    # major: int = None
    # minor: int = None
    # patch: int = None

    @staticmethod
    def get_columns(version: Semver) -> Columns:
        columns: Columns = None
        match version:
            # case Semver(4):
            #     return [
            #         sa.Column("id", sa.Integer, primary_key=True),
            #         sa.Column("major", sa.Integer),
            #         sa.Column("minor", sa.Integer),
            #         sa.Column("patch", sa.Integer),
            #     ]
            case Semver(3 | 4 as major):
                major
                columns = [sa.Column("version", sa.Text, primary_key=True)]
        return columns

    @classmethod
    def get_version(cls, session: Session) -> Semver:
        version = session.scalar(sa.select(cls.version))
        if version is None:
            raise ValueError("No version found.")
        return Semver(version)


@dataclass
class File(Table):
    file_id: int = field(init=False)
    url: str
    file_functional_id: str
    filename: str
    local_path: str
    data_node: str
    checksum: str
    checksum_type: str
    duration: int
    size: int
    rate: int
    start_date: str
    end_date: str
    crea_date: str
    status: int
    error_msg: str
    sdget_status: str
    sdget_error_msg: str
    priority: int
    tracking_id: str
    model: str
    project: str
    variable: str
    last_access_date: str
    dataset_id: int
    insertion_group_id: int
    timestamp: str

    @staticmethod
    def get_columns(version: Semver) -> Columns:
        columns: Columns = None
        match version:
            case Semver(3 | 4):
                columns = [
                    sa.Column("file_id", sa.Integer, primary_key=True),
                    sa.Column("url", sa.Text),
                    sa.Column("file_functional_id", sa.Text),
                    sa.Column("filename", sa.Text),
                    sa.Column("local_path", sa.Text),
                    sa.Column("data_node", sa.Text),
                    sa.Column("checksum", sa.Text),
                    sa.Column("checksum_type", sa.Text),
                    sa.Column("duration", sa.Integer),
                    sa.Column("size", sa.Integer),
                    sa.Column("rate", sa.Integer),
                    sa.Column("start_date", sa.Text),
                    sa.Column("end_date", sa.Text),
                    sa.Column("crea_date", sa.Text),
                    sa.Column("status", sa.Text),
                    sa.Column("error_msg", sa.Text),
                    sa.Column("sdget_status", sa.Text),
                    sa.Column("sdget_error_msg", sa.Text),
                    sa.Column("priority", sa.Integer),
                    sa.Column("tracking_id", sa.Text),
                    sa.Column("model", sa.Text),
                    sa.Column("project", sa.Text),
                    sa.Column("variable", sa.Text),
                    sa.Column("last_access_date", sa.Text),
                    sa.Column("dataset_id", sa.Integer),
                    sa.Column("insertion_group_id", sa.Integer),
                    sa.Column("timestamp", sa.Text),
                ]
        return columns


@dataclass
class Dataset(Table):
    dataset_id: int = field(init=False)
    dataset_functional_id: str
    status: str
    crea_date: str
    path: str
    path_without_version: str
    version: str
    local_path: str
    last_mod_date: str
    latest: int
    latest_date: str
    last_done_transfer_date: str
    model: str
    project: str
    template: str
    timestamp: str

    @staticmethod
    def get_columns(version: Semver) -> Columns:
        columns: Columns = None
        match version:
            case Semver(3 | 4):
                columns = [
                    sa.Column("dataset_id", sa.Integer, primary_key=True),
                    sa.Column("dataset_functional_id", sa.Text),
                    sa.Column("status", sa.Text),
                    sa.Column("crea_date", sa.Text),
                    sa.Column("path", sa.Text),
                    sa.Column("path_without_version", sa.Text),
                    sa.Column("version", sa.Text),
                    sa.Column("local_path", sa.Text),
                    sa.Column("last_mod_date", sa.Text),
                    sa.Column("latest", sa.Integer),
                    sa.Column("latest_date", sa.Text),
                    sa.Column("last_done_transfer_date", sa.Text),
                    sa.Column("model", sa.Text),
                    sa.Column("project", sa.Text),
                    sa.Column("template", sa.Text),
                    sa.Column("timestamp", sa.Text),
                ]
        return columns


@dataclass
class Param(Table):
    id: Optional[int] = field(init=False, default=None)
    name: str
    value: str
    last_updated: Optional[datetime] = field(init=False, default=None)

    @staticmethod
    def get_columns(version: Semver) -> Columns:
        columns: Columns = None
        match version:
            case Semver(4):
                columns = [
                    sa.Column("id", sa.Integer, primary_key=True),
                    sa.Column("name", sa.String(50), nullable=False),
                    sa.Column("value", sa.String(255), nullable=False),
                    sa.Column(
                        "last_updated",
                        sa.DateTime(timezone=True),
                        server_default=sa.func.now(),
                        onupdate=sa.func.now(),
                    ),
                ]
            case Semver(3):
                columns = [
                    sa.Column("id", sa.Integer, primary_key=True),
                    sa.Column("name", sa.Text),
                    sa.Column("value", sa.Text),
                ]
        return columns


# # TODO: these as unit tests
# if __name__ == "__main__":

#     class BadTable(Table):
#         """
#         Intentionally missing `get_columns` implementation.
#         """

#     try:
#         BadTable.map(sa.orm.registry(), Semver(3))
#         assert False
#     except TypeError:
#         assert True
