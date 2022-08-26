from __future__ import annotations
from typing import TypeAlias, TypeGuard, Any, Optional

from enum import Enum, auto
from datetime import datetime
from dataclasses import dataclass, field

FacetValues: TypeAlias = str | set[str] | list[str] | tuple[str]
FacetDict: TypeAlias = dict[str, FacetValues]
NestedFacetDict: TypeAlias = dict[str, FacetValues | list[FacetDict]]


def is_facet_values(values: Any) -> TypeGuard[FacetValues]:
    if isinstance(values, str):
        return True
    elif isinstance(values, (list, tuple, set)) and all(
        isinstance(x, str) for x in values
    ):
        return True
    else:
        return False


def is_facet_dict(d: dict[str, Any]) -> TypeGuard[FacetDict]:
    return all(is_facet_values(x) for x in d.values())


def split_nested_facet_dict(
    d: dict[str, Any]
) -> tuple[FacetDict, list[FacetDict]]:
    simple = dict(d)
    requests = simple.pop("requests", [])
    if not is_facet_dict(simple) or not isinstance(requests, list):
        raise TypeError
    if any(not is_facet_dict(x) for x in requests):
        raise TypeError
    return simple, requests


def is_nested_facet_dict(d: dict[str, Any]) -> TypeGuard[NestedFacetDict]:
    try:
        simple, requests = split_nested_facet_dict(d)
        return True
    except TypeError:
        return False


class FileStatus(Enum):
    deleted = auto()
    done = auto()
    error = auto()
    new = auto()
    paused = auto()
    running = auto()
    waiting = auto()

    def __repr__(self) -> str:
        return self.__class__.__name__ + "." + self.name
        # return self.name


@dataclass
class Version:
    version_num: str


@dataclass
class Param:
    id: int = field(init=False, repr=False)
    name: str
    value: str
    last_updated: Optional[datetime] = field(init=False, repr=False)


@dataclass
class File:
    id: int = field(init=False, repr=False)
    file_id: str
    dataset_id: str
    # `master_id` is used to find duplicate files from multiple versions
    master_id: str
    url: str
    version: str
    filename: str
    local_path: str
    data_node: str
    checksum: str
    checksum_type: str
    size: int
    status: FileStatus = FileStatus.new
    metadata: dict = field(repr=False, default_factory=dict, compare=False)
    last_updated: Optional[datetime] = field(init=False, repr=False)

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
    def from_dict(cls, raw_metadata: dict) -> "File":
        metadata = {}
        for k, v in raw_metadata.items():
            if isinstance(v, list) and len(v) == 1:
                metadata[k] = v[0]
            else:
                metadata[k] = v
        file_id = metadata["instance_id"]
        if not file_id.endswith(".nc"):
            # extension is forced to `.nc` (some were .nc[0|1|...])
            file_id = file_id.rsplit(".", 1)[0] + ".nc"
        url = metadata["url"][0].split("|")[0]
        filename = metadata["title"]
        dataset_id = file_id.removesuffix("." + filename)
        master_id = metadata["master_id"].rsplit(".", 1)[0]
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
            master_id=master_id,
            version=version,
            local_path=local_path,
            data_node=data_node,
            checksum=checksum,
            checksum_type=checksum_type,
            size=size,
            metadata=raw_metadata,
        )

    def clone(self) -> File:
        return File(
            file_id=self.file_id,
            dataset_id=self.dataset_id,
            master_id=self.master_id,
            url=self.url,
            version=self.version,
            filename=self.filename,
            local_path=self.local_path,
            data_node=self.data_node,
            checksum=self.checksum,
            checksum_type=self.checksum_type,
            size=self.size,
        )


Table: TypeAlias = Version | Param | File

__all__ = ["FileStatus", "Version", "Param", "File", "Table"]
