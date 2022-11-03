import asyncio

import pytest

from esgpull.config import Config
from esgpull.fs import Filesystem


@pytest.fixture
def fs(root):
    config = Config()
    return Filesystem.from_config(config)


@pytest.fixture
def file_object(fs, file):
    return fs.open(file_object)


def test_fs(root, fs):
    assert str(fs.auth) == str(root / "auth")
    assert str(fs.data) == str(root / "data")
    assert str(fs.db) == str(root / "db")
    assert str(fs.log) == str(root / "log")
    assert str(fs.tmp) == str(root / "tmp")
    assert fs.auth.is_dir()
    assert fs.data.is_dir()
    assert fs.db.is_dir()
    assert fs.log.is_dir()
    assert fs.tmp.is_dir()


def test_file_paths(fs, file):
    file.id = 1234
    assert fs.path_of(file) == fs.data / "project/folder/file.nc"
    assert fs.tmp_path_of(file) == fs.tmp / "1234.part"


async def writer_steps(fs, file):
    async with fs.open(file) as f:
        await f.write(b"")


def test_fs_writer(fs, file):
    asyncio.run(writer_steps(fs, file))
    for path in fs.glob_netcdf():
        assert str(path) == "project/folder/file.nc"
