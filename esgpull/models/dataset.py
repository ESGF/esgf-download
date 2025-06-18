from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, relationship

from esgpull.models.base import BaseNoSHA
from esgpull.models.utils import find_int, find_str

if TYPE_CHECKING:
    from esgpull.models.query import File


@dataclass
class DatasetRecord:
    dataset_id: str
    master_id: str
    version: str
    data_node: str
    size: int
    number_of_files: int

    @classmethod
    def serialize(cls, source: dict) -> DatasetRecord:
        dataset_id = find_str(source["instance_id"]).partition("|")[0]
        master_id, version = dataset_id.rsplit(".", 1)
        data_node = find_str(source["data_node"])
        size = find_int(source["size"])
        number_of_files = find_int(source["number_of_files"])
        return cls(
            dataset_id=dataset_id,
            master_id=master_id,
            version=version,
            data_node=data_node,
            size=size,
            number_of_files=number_of_files,
        )


class Dataset(BaseNoSHA):
    __tablename__ = "dataset"

    dataset_id: Mapped[str] = mapped_column(sa.String(255), primary_key=True)
    total_files: Mapped[int] = mapped_column(sa.Integer)
    created_at: Mapped[datetime] = mapped_column(
        server_default=sa.func.now(),
        default_factory=lambda: datetime.now(timezone.utc),
        init=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        server_default=sa.func.now(),
        default_factory=lambda: datetime.now(timezone.utc),
        init=False,
    )
    files: Mapped[list[File]] = relationship(
        back_populates="dataset",
        foreign_keys="[File.dataset_id]",
        primaryjoin="Dataset.dataset_id==File.dataset_id",
        default_factory=list,
        init=False,
        repr=False,
    )

    def asdict(self) -> Mapping[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "total_files": self.total_files,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    def __hash__(self) -> int:
        return hash(self.dataset_id)
