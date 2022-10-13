from __future__ import annotations
from typing import TypeAlias, TypeGuard, Any

from enum import Enum, unique
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


def find_str(container: list | str) -> str:
    if isinstance(container, list):
        return find_str(container[0])
    elif isinstance(container, str):
        return container
    else:
        raise ValueError(container)


def find_int(container: list | int) -> int:
    if isinstance(container, list):
        return find_int(container[0])
    elif isinstance(container, int):
        return container
    else:
        raise ValueError(container)


@unique
class FileStatus(str, Enum):
    new = "new"
    queued = "queued"
    starting = "starting"
    started = "started"
    pausing = "pausing"
    paused = "paused"
    error = "error"
    cancelled = "cancelled"
    done = "done"


@dataclass(repr=False)
class Version:
    version_num: str


@dataclass(repr=False)
class Param:
    id: int = field(init=False, repr=False)
    name: str
    value: str
    last_updated: datetime | None = field(init=False, repr=False)


@dataclass(repr=False)
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
    last_updated: datetime | None = field(init=False, repr=False)

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
    # last_access_date: str

    @staticmethod
    def get_local_path(metadata: dict, version: str) -> str:
        # template = metadata["directory_format_template_"]
        template = find_str(metadata["directory_format_template_"])
        # format: "%(a)/%(b)/%(c)/..."
        template = template.removeprefix("%(root)s/")
        template = template.replace("%(", "{")
        template = template.replace(")s", "}")
        metadata.pop("version", None)
        if "rcm_name" in metadata:  # cordex special case
            institute = find_str(metadata["institute"])
            rcm_name = find_str(metadata["rcm_name"])
            rcm_model = institute + "-" + rcm_name
            metadata["rcm_model"] = rcm_model
        return template.format(version=version, **metadata)

    @classmethod
    def from_dict(cls, metadata: dict) -> "File":
        # def from_dict(cls, raw_metadata: dict) -> "File":
        # metadata = {}
        # for k, v in raw_metadata.items():
        #     if isinstance(v, list) and len(v) == 1:
        #         metadata[k] = v[0]
        #     else:
        #         metadata[k] = v
        # dataset_id = metadata[0]["dataset_id"].partition("|")[0]
        # filename = metadata[0]["title"]
        # url = metadata["url"][0].partition("|")[0]

        dataset_id = find_str(metadata["dataset_id"]).partition("|")[0]
        filename = find_str(metadata["title"])
        url = find_str(metadata["url"]).partition("|")[0]
        data_node = find_str(metadata["data_node"])
        checksum = find_str(metadata["checksum"])
        checksum_type = find_str(metadata["checksum_type"])
        size = find_int(metadata["size"])

        file_id = ".".join([dataset_id, filename])
        dataset_master = dataset_id.rsplit(".", 1)[0]  # remove version
        master_id = ".".join([dataset_master, filename])
        version = dataset_id.rsplit(".", 1)[1]
        local_path = cls.get_local_path(metadata, version)

        # if not file_id.endswith(".nc"):
        #     extension is forced to `.nc` (some were .nc[0|1|...])
        #     file_id = file_id.rsplit(".", 1)[0] + ".nc"
        # master_id = ".".join([dataset_id, filename])
        # master_id = metadata["master_id"].rsplit(".", 1)[0]
        # grab version from instance_id, as files always give `version=1`

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
            metadata=metadata,
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


@unique
class DownloadMethod(str, Enum):
    Download = "Download"
    ChunkedDownload = "ChunkedDownload"
    MultiSourceChunkedDownload = "MultiSourceChunkedDownload"


__all__ = ["FileStatus", "Version", "Param", "File", "Table"]
