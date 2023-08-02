from __future__ import annotations

import hashlib
from collections.abc import Iterator
from dataclasses import InitVar, dataclass, field
from enum import Enum, auto
from pathlib import Path
from shutil import copyfile

import aiofiles
from aiofiles.threadpool.binary import AsyncBufferedIOBase

from esgpull.config import Config
from esgpull.models import File
from esgpull.result import Err, Ok, Result
from esgpull.tui import logger


class FileCheck(Enum):
    Missing = auto()  # not in any known paths
    Part = auto()  # {file.sha}.part exists
    BadSize = auto()  # {file.sha}.done exists AND has wrong size
    BadChecksum = auto()  # {file.sha}.done exists AND has wrong size
    Done = auto()  # {file.sha}.done exists AND is ready to be moved
    Ok = auto()  # file is in drs with everything ok

    def as_err(self, file: File) -> Exception:
        err_cls = type(str(self), (Exception,), {})
        return err_cls(file)


@dataclass
class Digest:
    file: InitVar[File]
    alg: hashlib._Hash = field(init=False)

    def __post_init__(self, file: File) -> None:
        match file.checksum_type:
            case "SHA256":
                self.alg = hashlib.sha256()
            case _:
                raise NotImplementedError

    @classmethod
    def from_path(cls, file: File, path: Path) -> Digest:
        block_size = path.stat().st_blksize
        digest = cls(file)
        with path.open("rb") as f:
            while True:
                block = f.read(block_size)
                if block == b"":
                    break
                else:
                    digest.update(block)
        return digest

    def update(self, chunk: bytes) -> None:
        self.alg.update(chunk)

    def hexdigest(self) -> str:
        return self.alg.hexdigest()


@dataclass
class Filesystem:
    auth: Path
    data: Path
    db: Path
    log: Path
    tmp: Path
    disable_checksum: bool = False
    install: InitVar[bool] = True

    @staticmethod
    def from_config(config: Config, install: bool = False) -> Filesystem:
        return Filesystem(
            auth=config.paths.auth,
            data=config.paths.data,
            db=config.paths.db,
            log=config.paths.log,
            tmp=config.paths.tmp,
            disable_checksum=config.download.disable_checksum,
            install=install,
        )

    def __post_init__(self, install: bool = True) -> None:
        if install:
            self.auth.mkdir(parents=True, exist_ok=True)
            self.data.mkdir(parents=True, exist_ok=True)
            self.db.mkdir(parents=True, exist_ok=True)
            self.log.mkdir(parents=True, exist_ok=True)
            self.tmp.mkdir(parents=True, exist_ok=True)

    def __getitem__(self, file: File) -> FilePath:
        if not isinstance(file, File):
            raise TypeError(file)
        return FilePath(
            drs=self.data / file.local_path / file.filename,
            tmp=self.tmp / f"{file.sha}.part",
        )

    def glob_netcdf(self) -> Iterator[Path]:
        for path in self.data.glob("**/*.nc"):
            yield path.relative_to(self.data)

    def open(self, file: File) -> FileObject:
        return FileObject(self[file])

    def isempty(self, path: Path) -> bool:
        if next(path.iterdir(), None) is None:
            return True
        else:
            return False

    def iter_empty_parents(self, path: Path) -> Iterator[Path]:
        sample: Path | None
        for _ in range(10):  # abitrary 10 to avoid infinite loop
            sample = next(path.glob("**/*.nc"), None)
            if sample is None and self.isempty(path):
                yield path
                path = path.parent
            else:
                return

    def move_to_drs(self, file: File) -> None:
        path = self[file]
        path.drs.parent.mkdir(parents=True, exist_ok=True)
        try:
            path.done.rename(path.drs)
        except OSError as err:
            logger.error(err)
            copyfile(path.done, path.drs)
            msg = """
File rename error, shutil.copyfile was used instead.
For large files, download times might be impacted.
To address this issue, you may consider setting your `tmp` directory to the same filesystem as your `data` directory:

$ esgpull config path.tmp <some/path/on/data/filesystem>
            """.strip()
            logger.error(msg)

    def delete(self, *files: File) -> None:
        for file in files:
            path = self[file].drs
            if not path.is_file():
                continue
            path.unlink()
            logger.info(f"Deleted file {path}")
            for subpath in self.iter_empty_parents(path.parent):
                subpath.rmdir()
                logger.info(f"Deleted empty folder {subpath}")

    def compute_checksum(
        self,
        file: File,
        path: Path,
        disable_checksum: bool | None = None,
    ) -> str:
        if disable_checksum is None:
            disable_checksum = self.disable_checksum
        if disable_checksum:
            return file.checksum
        else:
            return Digest.from_path(file, path).hexdigest()

    def check_impl(
        self,
        file: File,
        path: Path,
        digest: Digest | None = None,
    ) -> FileCheck:
        if path.stat().st_size != file.size:
            return FileCheck.BadSize
        if digest is None:
            checksum = self.compute_checksum(file, path)
        else:
            checksum = digest.hexdigest()
        if checksum == file.checksum:
            return FileCheck.Ok
        else:
            return FileCheck.BadChecksum

    def check(
        self,
        file: File,
        digest: Digest | None = None,
    ) -> FileCheck:
        path = self[file]
        if path.drs.is_file():
            return self.check_impl(file, path.drs, digest)
        elif path.done.is_file():
            match self.check_impl(file, path.done, digest):
                case FileCheck.Ok:
                    return FileCheck.Done
                case check:
                    return check
        elif path.tmp.is_file():
            match self.check_impl(file, path.tmp, digest):
                case FileCheck.BadSize:
                    return FileCheck.Part
                case _:
                    raise ValueError()
        else:
            return FileCheck.Missing

    def finalize(
        self,
        file: File,
        digest: Digest | None = None,
    ) -> Result[FileCheck]:
        match self.check(file, digest=digest):
            case FileCheck.Ok:
                return Ok(FileCheck.Ok)
            case FileCheck.Done:
                self.move_to_drs(file)
                return Ok(FileCheck.Ok)
            case check:
                return Err(check, check.as_err(file))


@dataclass
class FilePath:
    drs: Path
    tmp: Path

    @property
    def done(self) -> Path:
        return self.tmp.with_suffix(".done")

    def __str__(self) -> str:
        return str(self.drs)


@dataclass
class FileObject:
    path: FilePath
    buffer: AsyncBufferedIOBase = field(init=False)

    async def __aenter__(self) -> FileObject:
        self.buffer = await aiofiles.open(self.path.tmp, "wb")
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback) -> None:
        if not self.buffer.closed:
            await self.buffer.close()

    async def write(self, chunk: bytes) -> None:
        await self.buffer.write(chunk)

    async def to_done(self) -> None:
        if not self.buffer.closed:
            await self.buffer.close()
        self.path.tmp.rename(self.path.done)
