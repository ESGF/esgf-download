import asyncio
from pathlib import Path

import pytest

from esgpull.config import Config
from esgpull.fs import FileCheck, Filesystem
from esgpull.models import File


@pytest.fixture
def fs_no_install():
    config = Config()
    return Filesystem.from_config(config, install=False)


@pytest.fixture
def fs() -> Filesystem:
    config = Config()
    return Filesystem.from_config(config, install=True)


def test_install(root: Path, fs: Filesystem):
    assert str(fs.paths.auth) == str(root / "auth")
    assert str(fs.paths.data) == str(root / "data")
    assert str(fs.paths.db) == str(root / "db")
    assert str(fs.paths.log) == str(root / "log")
    assert str(fs.paths.tmp) == str(root / "tmp")
    assert fs.paths.auth.is_dir()
    assert fs.paths.data.is_dir()
    assert fs.paths.db.is_dir()
    assert fs.paths.log.is_dir()
    assert fs.paths.tmp.is_dir()


def test_no_install(root: Path, fs_no_install: Filesystem):
    assert str(fs_no_install.paths.auth) == str(root / "auth")
    assert str(fs_no_install.paths.data) == str(root / "data")
    assert str(fs_no_install.paths.db) == str(root / "db")
    assert str(fs_no_install.paths.log) == str(root / "log")
    assert str(fs_no_install.paths.tmp) == str(root / "tmp")
    assert not fs_no_install.paths.auth.is_dir()
    assert not fs_no_install.paths.data.is_dir()
    assert not fs_no_install.paths.db.is_dir()
    assert not fs_no_install.paths.log.is_dir()
    assert not fs_no_install.paths.tmp.is_dir()


def test_file_paths(fs: Filesystem, file: File):
    file.sha = "1234"
    path = fs[file]
    assert path.drs == fs.paths.data / "project/folder/file.nc"
    assert path.tmp == fs.paths.tmp / "1234.part"


async def write_steps(fs: Filesystem, file: File):
    async with fs.open(file) as f:
        await f.write(b"")


def test_write(fs: Filesystem, file: File):
    asyncio.run(write_steps(fs, file))
    for path in fs.glob_netcdf():
        assert str(path) == "project/folder/file.nc"


@pytest.mark.parametrize(
    "expected_check,content,kind,size,checksum",
    [
        pytest.param(
            FileCheck.Missing,
            None,
            None,
            None,
            None,
            id="file does not exist anywhere",
        ),
        pytest.param(
            FileCheck.Part,
            "cont",
            "tmp",
            7,
            None,
            id="size not matching content (tmp)",
        ),
        pytest.param(
            FileCheck.BadSize,
            "content",
            "done",
            0,
            None,
            id="size not matching content (done)",
        ),
        pytest.param(
            FileCheck.BadSize,
            "content",
            "drs",
            0,
            None,
            id="size not matching content (drs)",
        ),
        pytest.param(
            FileCheck.BadChecksum,
            "content",
            "tmp",
            7,
            "",
            marks=pytest.mark.xfail(raises=ValueError, strict=True),
            id="checksum not matching content (tmp)",
        ),
        pytest.param(
            FileCheck.BadChecksum,
            "content",
            "done",
            7,
            "0",
            id="checksum not matching content (done)",
        ),
        pytest.param(
            FileCheck.BadChecksum,
            "content",
            "drs",
            7,
            "0",
            id="checksum not matching content (drs)",
        ),
        pytest.param(
            FileCheck.Done,
            "content",
            "tmp",
            7,
            "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73",
            marks=pytest.mark.xfail(raises=ValueError, strict=True),
            id="size and checksum match content (tmp)",
        ),
        pytest.param(
            FileCheck.Done,
            "content",
            "done",
            7,
            "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73",
            id="size and checksum match content (done)",
        ),
        pytest.param(
            FileCheck.Ok,
            "content",
            "drs",
            7,
            "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73",
            id="size and checksum match content (drs)",
        ),
    ],
)
def test_check(
    root: Path,
    fs: Filesystem,
    file: File,
    expected_check: FileCheck,
    content: str | None,
    kind: str,
    size: int,
    checksum: str,
):
    if content is not None:
        file.size = size
        file.checksum = checksum
        file.checksum_type = "SHA256"
        if kind == "tmp":
            path = fs[file].tmp
        elif kind == "done":
            path = fs[file].done
        elif kind == "drs":
            path = fs[file].drs
            path.parent.mkdir(parents=True)
        else:
            path = root / kind
        with path.open("wb") as f:
            f.write(str(content).encode())
    check = fs.check(file)
    assert check == expected_check
