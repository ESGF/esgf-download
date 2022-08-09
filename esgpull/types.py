from typing import TypeAlias

from enum import Enum, auto
from datetime import datetime
from dataclasses import dataclass, field


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
class Version:
    version_num: str


@dataclass
class Param:
    id: int = field(init=False)
    name: str
    value: str
    last_updated: datetime = field(init=False)


@dataclass
class File:
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


Table: TypeAlias = Version | Param | File

__all__ = ["Status", "Version", "Param", "File", "Table"]
