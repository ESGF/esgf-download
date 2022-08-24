import asyncio

from esgpull.types import File
from esgpull.fs import Filesystem


def test_fs(tmp_path):
    root = tmp_path / "esgpull"
    fs = Filesystem(root)
    assert str(fs.data) == str(root / "data")
    assert str(fs.db) == str(root / "db")
    file = File(
        file_id="file",
        dataset_id="dataset",
        url="file",
        version="v0",
        filename="file.nc",
        local_path=str(fs.data),
        data_node="data_node",
        checksum="0",
        checksum_type="0",
        size=0,
    )
    assert fs.path_of(file) == fs.data / "file.nc"
    asyncio.run(fs.write(file, b""))
    assert list(fs.glob_netcdf()) == [fs.path_of(file)]
