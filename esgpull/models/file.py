from __future__ import annotations

from enum import Enum

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from esgpull.models.base import Base
from esgpull.utils import find_int, find_str


class FileStatus(Enum):
    New = "new"
    Queued = "queued"
    Starting = "starting"
    Started = "started"
    Pausing = "pausing"
    Paused = "paused"
    Error = "error"
    Cancelled = "cancelled"
    Done = "done"


def get_local_path(source: dict, version: str) -> str:
    flat_raw = {}
    for k, v in source.items():
        if isinstance(v, list) and len(v) == 1:
            flat_raw[k] = v[0]
        else:
            flat_raw[k] = v
    template = find_str(flat_raw["directory_format_template_"])
    # format: "%(a)/%(b)/%(c)/..."
    template = template.removeprefix("%(root)s/")
    template = template.replace("%(", "{")
    template = template.replace(")s", "}")
    flat_raw.pop("version", None)
    if "rcm_name" in flat_raw:  # cordex special case
        institute = flat_raw["institute"]
        rcm_name = flat_raw["rcm_name"]
        rcm_model = institute + "-" + rcm_name
        flat_raw["rcm_model"] = rcm_model
    return template.format(version=version, **flat_raw)


class File(Base):
    __tablename__ = "file"

    file_id: Mapped[str] = mapped_column(sa.String(255), unique=True)
    dataset_id: Mapped[str] = mapped_column(sa.String(255))
    master_id: Mapped[str] = mapped_column(sa.String(255))
    url: Mapped[str] = mapped_column(sa.String(255))
    version: Mapped[str] = mapped_column(sa.String(16))
    filename: Mapped[str] = mapped_column(sa.String(255))
    local_path: Mapped[str] = mapped_column(sa.String(255))
    data_node: Mapped[str] = mapped_column(sa.String(40))
    checksum: Mapped[str] = mapped_column(sa.String(64), unique=True)
    checksum_type: Mapped[str] = mapped_column(sa.String(16))
    size: Mapped[int]
    status: Mapped[FileStatus] = mapped_column(
        sa.Enum(FileStatus), default=FileStatus.New
    )

    def _as_bytes(self) -> bytes:
        self_tuple = (self.file_id, self.checksum)
        return str(self_tuple).encode()

    @classmethod
    def from_dict(cls, raw: dict) -> File:
        raise NotImplementedError

    @classmethod
    def serialize(cls, source: dict) -> File:
        dataset_id = find_str(source["dataset_id"]).partition("|")[0]
        filename = find_str(source["title"])
        url = find_str(source["url"]).partition("|")[0]
        data_node = find_str(source["data_node"])
        checksum = find_str(source["checksum"])
        checksum_type = find_str(source["checksum_type"])
        size = find_int(source["size"])
        file_id = ".".join([dataset_id, filename])
        dataset_master = dataset_id.rsplit(".", 1)[0]  # remove version
        master_id = ".".join([dataset_master, filename])
        version = dataset_id.rsplit(".", 1)[1]
        local_path = get_local_path(source, version)

        result = cls(
            file_id=file_id,
            dataset_id=dataset_id,
            master_id=master_id,
            url=url,
            version=version,
            filename=filename,
            local_path=local_path,
            data_node=data_node,
            checksum=checksum,
            checksum_type=checksum_type,
            size=size,
        )
        result.compute_sha()
        return result


# FileRequires = [
# File facets
#     "instance_id",
#     "dataset_id",
#     "title",
#     "url",
#     "data_node",
#     "checksum",
#     "checksum_type",
#     "size",
#     "directory_format_template_",
#     "version",
#     "institute",
#     "rcm_name",
# directory_format_template_ facets
#     "activity",
#     "activity_drs",
#     "activity_id",
#     "atmos_grid_resolution",
#     "bias_adjustment",
#     "cmor_table",
#     "data_structure",
#     "data_type",
#     "date",
#     "domain",
#     "driving_model",
#     "ensemble",
#     "ensemble_member",
#     "experiment",
#     "experiment_id",
#     "frequency",
#     "grid_label",
#     "grid_resolution",
#     "institute",
#     "institution_id",
#     "member_id",
#     "mip_era",
#     "model",
#     "model_version",
#     "ocean_grid_resolution",
#     "product",
#     "project",
#     "rcm_model",
#     "rcm_version",
#     "realization",
#     "realm",
#     "regridding",
#     "root",
#     "run_category",
#     "source",
#     "source_data_id",
#     "source_id",
#     "source_type",
#     "table_id",
#     "target_mip",
#     "time_frequency",
#     "var",
#     "variable",
#     "variable_id",
#     "version",
#     "work_package",
# ]
