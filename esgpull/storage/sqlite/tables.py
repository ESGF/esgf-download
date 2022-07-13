from __future__ import annotations
from typing import Optional, Type, cast
from dataclasses import dataclass, field

import sqlalchemy as sa
from datetime import datetime

from esgpull.storage.sqlite.types import (
    Table,
    Session,
    Columns,
    Registry,
)
from esgpull.utils import Semver


@dataclass
class Version(Table):
    version: str
    # id: Optional[int] = field(init=False, default=None)
    # major: int = None
    # minor: int = None
    # patch: int = None

    @classmethod
    def map(cls, mapper: Registry, version: Semver) -> Type[Version]:
        return cast(Type[Version], super().map(mapper, version))

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
            case Semver(3):
                columns = [sa.Column("version", sa.Text, primary_key=True)]
            case Semver(4):
                columns = [
                    sa.Column("version", sa.String(20), primary_key=True)
                ]
        return columns

    @classmethod
    def get_version(cls, session: Session) -> Semver:
        version = session.scalar(sa.select(cls.version))
        if version is None:
            raise ValueError("No version found.")
        return Semver(version)


@dataclass
class File(Table):
    id: int = field(init=False)
    file_id: str = None
    dataset_id: str = None
    url: str = None
    file_functional_id: str = None
    version: str = None
    filename: str = None
    local_path: str = None
    data_node: str = None
    checksum: str = None
    checksum_type: str = None
    duration: int = None
    size: int = None
    rate: int = None
    start_date: str = None
    end_date: str = None
    crea_date: str = None
    status: int = None
    error_msg: str = None
    sdget_status: str = None
    sdget_error_msg: str = None
    priority: int = None
    tracking_id: str = None
    model: str = None
    project: str = None
    variable: str = None
    last_access_date: str = None
    dataset_id: int = None
    insertion_group_id: int = None
    timestamp: str = None
    metadata: dict = field(repr=False, default_factory=dict)

    def __post_init__(self) -> None:
        if self.metadata is not None:
            self.parse_metadata()

    def parse_metadata(self) -> None:
        metadata = {}
        for k, v in self.metadata.items():
            if isinstance(v, list) and len(v) == 1:
                metadata[k] = v[0]
            else:
                metadata[k] = v
        if self.file_id is None:
            fid = metadata["instance_id"]
            if not fid.endswith(".nc"):
                # filter out `.nc0`, `.nc1`, etc.
                fid = fid.rsplit(".", 1)[0] + ".nc"
            self.file_id = fid
        if self.dataset_id is None:
            self.dataset_id = self.file_id.removesuffix("." + self.filename)
        if self.url is None:
            self.url = metadata["url"][0].split("|")[0]
        if self.version is None:
            # grab version from instance_id, as files always give `version=1`
            self.version = self.dataset_id.split(".")[-1]
        if self.filename is None:
            self.filename = metadata["title"]
        if self.local_path is None:
            dir_fmt_template = metadata["directory_format_template_"]
            # format: "%(a)/%(b)/%(c)/..."
            dir_fmt_template = dir_fmt_template.removeprefix("%(root)s/")
            dir_fmt_template = dir_fmt_template.replace("%(", "{")
            dir_fmt_template = dir_fmt_template.replace(")s", "}")
            metadata.pop("version", None)
            self.local_path = dir_fmt_template.format(
                version=self.version, **metadata
            )
        if self.data_node is None:
            self.data_node = metadata["data_node"]
        if self.checksum is None:
            self.checksum = metadata["checksum"]
        if self.checksum_type is None:
            self.checksum_type = metadata["checksum_type"]

    # def fill_dataset_id(self, storage: "Storage") -> None:
    #     with storage.select(storage.Dataset.dataset_id) as stmt:
    #         dataset_fid_expr = storage.Dataset.dataset_functional_id
    #         self.dataset_id = stmt.where(
    #             dataset_fid_expr == self.dataset_functional_id
    #         ).scalar

    # @property
    # def dataset_functional_id(self) -> str:
    #     return self.file_functional_id.removesuffix("." + self.filename)

    @staticmethod
    def get_columns(version: Semver) -> Columns:
        columns: Columns = None
        match version:
            case Semver(3):
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
            case Semver(4):
                columns = [
                    sa.Column("id", sa.Integer, primary_key=True),
                    sa.Column("file_id", sa.Text),
                    sa.Column("dataset_id", sa.Text),
                    sa.Column("url", sa.Text),
                    sa.Column("version", sa.String(8)),
                    sa.Column("filename", sa.String(255)),
                    sa.Column("local_path", sa.String(255)),
                    sa.Column("data_node", sa.String(40)),
                    sa.Column("checksum", sa.String(64)),
                    sa.Column("checksum_type", sa.String(16)),
                    # sa.Column("start_date", sa.Text),
                    # sa.Column("end_date", sa.Text),
                    # sa.Column(
                    #     "dataset_id",
                    #     sa.Integer,
                    #     sa.ForeignKey("dataset.dataset_id"),
                    #     nullable=False,
                    # ),
                    sa.Column("metadata", sa.JSON),
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
                ]
        return columns


@dataclass
class Dataset(Table):
    dataset_id: int = field(init=False)
    dataset_functional_id: str = None
    status: str = None
    crea_date: str = None
    path: str = None
    path_without_version: str = None
    version: str = None
    local_path: str = None
    last_mod_date: str = None
    latest: int = None
    latest_date: str = None
    last_done_transfer_date: str = None
    model: str = None
    project: str = None
    template: str = None
    timestamp: str = None
    metadata: dict = None

    def __post_init__(self) -> None:
        if self.metadata is not None:
            self.parse_metadata()

    def parse_metadata(self) -> None:
        metadata = {}
        for k, v in self.metadata.items():
            if isinstance(v, list) and len(v) == 1:
                metadata[k] = v[0]
            else:
                metadata[k] = v
        if self.dataset_functional_id is None:
            self.dataset_functional_id = metadata["instance_id"]
        if self.version is None:
            self.version = metadata["version"]
            if not self.version.startswith("v"):
                self.version = "v" + self.version

    @staticmethod
    def get_columns(version: Semver) -> Columns:
        columns: Columns = None
        match version:
            case Semver(3):
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
            case Semver(4):
                columns = [
                    sa.Column("dataset_id", sa.Integer, primary_key=True),
                    sa.Column(
                        "dataset_functional_id", sa.String(255), unique=True
                    ),
                    sa.Column("version", sa.String(8)),
                    sa.Column("metadata", sa.JSON),
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
            case Semver(3):
                columns = [
                    sa.Column("id", sa.Integer, primary_key=True),
                    sa.Column("name", sa.Text),
                    sa.Column("value", sa.Text),
                ]
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
                    sa.UniqueConstraint("name", "value"),
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
