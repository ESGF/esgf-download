from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column, object_session, relationship

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
    )

    @property
    def completed_files(self) -> int:
        """Count of files with status='done' for this dataset."""
        from esgpull.models.file import FileStatus
        from esgpull.models.query import File

        session = object_session(self)
        if session is None:
            return sum(1 for f in self.files if f.status == FileStatus.Done)
        else:
            stmt = (
                sa.select(sa.func.count(File.sha))
                .where(File.dataset_id == self.dataset_id)
                .where(File.status == FileStatus.Done)
            )
            return session.scalar(stmt) or 0

    @property
    def is_valid(self) -> bool:
        """
        Check if the dataset is valid.

        An invalid dataset is most certainly a result of migrations, and needs to be updated/repaired.
        """
        return self.total_files > 0

    @property
    def is_complete(self) -> bool:
        """Check if all files for this dataset are downloaded."""
        if not self.is_valid:
            raise ValueError("An invalid dataset has undefined completion.")
        return self.completed_files == self.total_files

    @property
    def completion_percentage(self) -> float:
        """Calculate the completion percentage."""
        if self.is_complete:
            return 100.0
        elif self.total_files == 0:
            return 0.0
        return (self.completed_files / self.total_files) * 100

    def asdict(self) -> Mapping[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "total_files": self.total_files,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "completed_files": self.completed_files,
            "is_complete": self.is_complete,
            "completion_percentage": self.completion_percentage,
        }

