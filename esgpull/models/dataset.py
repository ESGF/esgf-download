from __future__ import annotations

from dataclasses import asdict, dataclass

from esgpull.models.utils import find_int, find_str


@dataclass
class Dataset:
    dataset_id: str
    master_id: str
    version: str
    data_node: str
    size: int
    number_of_files: int

    @classmethod
    def serialize(cls, source: dict) -> Dataset:
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

    def asdict(self) -> dict:
        return asdict(self)
