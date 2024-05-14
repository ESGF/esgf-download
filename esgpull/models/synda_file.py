from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
)

from esgpull.models.file import FileStatus
from esgpull.models.query import File

SyndaStatusMap = {
    "running": FileStatus.Started,
    "waiting": FileStatus.Queued,
}


class SyndaBase(MappedAsDataclass, DeclarativeBase):
    pass


class SyndaFile(SyndaBase):
    __tablename__ = "file"

    url: Mapped[str]
    file_functional_id: Mapped[str]
    filename: Mapped[str]
    local_path: Mapped[str]
    data_node: Mapped[str]
    checksum: Mapped[str]
    checksum_type: Mapped[str]
    duration: Mapped[int]
    size: Mapped[int]
    rate: Mapped[int]
    start_date: Mapped[str]
    end_date: Mapped[str]
    crea_date: Mapped[str]
    status: Mapped[str]
    error_msg: Mapped[str]
    sdget_status: Mapped[str]
    sdget_error_msg: Mapped[str]
    priority: Mapped[int]
    tracking_id: Mapped[str]
    model: Mapped[str]
    project: Mapped[str]
    variable: Mapped[str]
    last_access_date: Mapped[str]
    dataset_id: Mapped[int]
    insertion_group_id: Mapped[int]
    timestamp: Mapped[str]
    file_id: Mapped[int] = mapped_column(init=False, primary_key=True)

    def get_status(self) -> FileStatus:
        s = self.status.lower()
        result: FileStatus
        if FileStatus.contains(s):
            result = FileStatus(s)
        elif s in SyndaStatusMap:
            result = SyndaStatusMap[s]
        else:
            raise ValueError(s)
        return result

    def to_file(self) -> File:
        file_id = self.file_functional_id
        dataset_id = file_id.removesuffix(self.filename).strip(".")
        dataset_master, version = dataset_id.rsplit(".", 1)
        master_id = ".".join([dataset_master, self.filename])
        url = self.url.replace("http://", "https://")
        local_path = self.local_path.removesuffix(self.filename).strip("/")
        result = File(
            file_id=file_id,
            dataset_id=dataset_id,
            master_id=master_id,
            url=url,
            version=version,
            filename=self.filename,
            local_path=local_path,
            data_node=self.data_node,
            checksum=self.checksum,
            checksum_type=self.checksum_type.upper(),
            size=self.size,
            status=self.get_status(),
        )
        result.compute_sha()
        return result
