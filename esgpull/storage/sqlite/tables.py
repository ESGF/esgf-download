from __future__ import annotations
from typing import Type
from enum import Enum, auto
from dataclasses import dataclass, field

import sqlalchemy as sa
import sqlalchemy.orm
from datetime import datetime

from esgpull.storage.sqlite.types import Table


@dataclass
class Version(Table):
    version_num: str

    __columns__ = [sa.Column("version_num", sa.String(32), primary_key=True)]


# @dataclass
# class ModuleVersion(Table):
#     version_num: str

#     __columns__ = [sa.Column("version_num", sa.String(32), primary_key=True)]


class Status(Enum):
    deleted = auto()
    done = auto()
    error = auto()
    new = auto()
    paused = auto()
    running = auto()
    waiting = auto()

    def __repr__(self) -> str:
        # return self.__class__.__name__ + "." + self.name
        return self.name


@dataclass
class File(Table):
    id: int = field(init=False)
    file_id: str
    dataset_id: str
    url: str
    version: str
    filename: str
    local_path: str
    data_node: str
    checksum: str
    checksum_type: str
    size: int
    status: Status = Status.new
    metadata: dict = field(repr=False, default_factory=dict)

    # duration: int
    # rate: int
    # start_date: str
    # end_date: str
    # crea_date: str
    # status: int
    # error_msg: str
    # sdget_status: str
    # sdget_error_msg: str
    # priority: int
    # tracking_id: str
    # model: str
    # project: str
    # variable: str
    # last_access_date: str
    # dataset_id: int
    # insertion_group_id: int
    # timestamp: str

    __columns__ = [
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
    ]

    @staticmethod
    def get_local_path(metadata: dict, version: str) -> str:
        template = metadata["directory_format_template_"]
        # format: "%(a)/%(b)/%(c)/..."
        template = template.removeprefix("%(root)s/")
        template = template.replace("%(", "{")
        template = template.replace(")s", "}")
        metadata.pop("version", None)
        if "rcm_name" in metadata:  # cordex special case
            metadata["rcm_model"] = (
                metadata["institute"] + "-" + metadata["rcm_name"]
            )
        return template.format(version=version, **metadata)

    @classmethod
    def from_metadata(cls, raw_metadata: dict) -> "File":
        metadata = {}
        for k, v in raw_metadata.items():
            if isinstance(v, list) and len(v) == 1:
                metadata[k] = v[0]
            else:
                metadata[k] = v
        file_id = metadata["instance_id"]
        if not file_id.endswith(".nc"):
            # filter out `.nc0`, `.nc1`, etc.
            file_id = file_id.rsplit(".", 1)[0] + ".nc"
        url = metadata["url"][0].split("|")[0]
        filename = metadata["title"]
        dataset_id = file_id.removesuffix("." + filename)
        # grab version from instance_id, as files always give `version=1`
        version = dataset_id.split(".")[-1]
        local_path = cls.get_local_path(metadata, version)
        data_node = metadata["data_node"]
        checksum = metadata["checksum"]
        checksum_type = metadata["checksum_type"]
        size = metadata["size"]
        return cls(
            file_id=file_id,
            url=url,
            filename=filename,
            dataset_id=dataset_id,
            version=version,
            local_path=local_path,
            data_node=data_node,
            checksum=checksum,
            checksum_type=checksum_type,
            size=size,
            metadata=raw_metadata,
        )


@dataclass
class Param(Table):
    id: int = field(init=False)
    name: str
    value: str
    last_updated: datetime = field(init=False)

    __columns__ = [
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


Mapper = sa.orm.registry()


def map_table(name: str, table_type: Type[Table]) -> None:
    columns = table_type.__columns__
    table = sa.Table(name, Mapper.metadata, *columns)
    Mapper.map_imperatively(table_type, table)


map_table("version", Version)
map_table("param", Param)
map_table("file", File)

__all__ = ["Mapper", "Version", "Param", "File"]
