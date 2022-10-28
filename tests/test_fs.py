import asyncio

import pytest

from esgpull.config import Paths
from esgpull.db.models import File
from esgpull.fs import Filesystem


@pytest.fixture
def fs(tmp_path):
    paths = Paths(root=tmp_path)
    return Filesystem.from_paths(paths)


@pytest.fixture
def file(fs):
    f = File(
        file_id="file",
        dataset_id="dataset",
        master_id="master",
        url="file",
        version="v0",
        filename="file.nc",
        local_path=str(fs.data),
        data_node="data_node",
        checksum="0",
        checksum_type="0",
        size=0,
    )
    f.id = 1
    return f


@pytest.fixture
def file_object(fs, file):
    return fs.open(file_object)


def test_fs(tmp_path, fs):
    assert str(fs.data) == str(tmp_path / "data")
    assert str(fs.db) == str(tmp_path / "db")
    assert fs.data.is_dir()
    assert fs.data.is_dir()


def test_file_paths(fs, file):
    file.id = 1234
    assert fs.path_of(file) == fs.data / "file.nc"
    assert fs.tmp_path_of(file) == fs.tmp / "1234.part"


async def writer_steps(fs, file):
    async with fs.open(file) as f:
        await f.write(b"")


def test_fs_writer(fs, file):
    asyncio.run(writer_steps(fs, file))
    assert list(fs.glob_netcdf()) == [fs.path_of(file)]
