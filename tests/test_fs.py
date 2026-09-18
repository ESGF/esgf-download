import asyncio
from contextlib import nullcontext as does_not_raise
from pathlib import Path

import pytest

from esgpull.config import Config
from esgpull.exceptions import UnknownMultihashError
from esgpull.fs import Digest, FileCheck, Filesystem
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
    assert str(fs.paths.data) == str(root / "data")
    assert str(fs.paths.db) == str(root / "db")
    assert str(fs.paths.log) == str(root / "log")
    assert str(fs.paths.tmp) == str(root / "tmp")
    assert fs.paths.data.is_dir()
    assert fs.paths.db.is_dir()
    assert fs.paths.log.is_dir()
    assert fs.paths.tmp.is_dir()


def test_no_install(root: Path, fs_no_install: Filesystem):
    assert str(fs_no_install.paths.data) == str(root / "data")
    assert str(fs_no_install.paths.db) == str(root / "db")
    assert str(fs_no_install.paths.log) == str(root / "log")
    assert str(fs_no_install.paths.tmp) == str(root / "tmp")
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


@pytest.mark.parametrize(
    ("checksum", "exc"),
    [
        pytest.param(
            "1220ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73",
            does_not_raise(),
            id="sha256",
        ),
        pytest.param(
            "1114040f06fd774092478d450774f5ba30c5da78acc8",
            does_not_raise(),
            id="sha1",
        ),
        pytest.param(
            "1340b2d1d285b5199c85f988d03649c37e44fd3dde01e5d69c50fef90651962f48110e9340b60d49a479c4c0b53f5f07d690686dd87d2481937a512e8b85ee7c617f",
            does_not_raise(),
            id="sha512",
        ),
        pytest.param(
            "14400e16bc8f42243e3cd44391411ea2e756646f5683bfb4875c82e7ad6cc9d4230ba4f74b7207d424ce2a63cf43e31b03fd3819b16b0ad4e88c73e1f47438d32049",
            does_not_raise(),
            id="sha3-512",
        ),
        pytest.param(
            "162073a38b9e525c9c2ae262feeaa3c2947ab19bce3a173f075c75341e5e7fa080b6",
            does_not_raise(),
            id="sha3-256",
        ),
        pytest.param(
            "402073a38b9e525c9c2ae262feeaa3c2947ab19bce3a173f075c75341e5e7fa080b6",
            pytest.raises(UnknownMultihashError),
            id="Wrong algo code",
        ),
        pytest.param(
            "test",
            pytest.raises(ValueError),
            id="Not hexadecimal checksum",
        ),
    ],
)
def test_digest_multihash(
    file: File,
    tmp_path: Path,
    checksum: str,
    exc,
):
    content = "content"
    file.checksum_type = "MULTIHASH"
    file.checksum = checksum
    path = tmp_path / "file"
    with path.open("wb") as f:
        f.write(str(content).encode())
    with exc:
        digest = Digest.from_path(file, path)
        assert digest.hexdigest() == checksum
