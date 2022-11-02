from __future__ import annotations

from pathlib import Path
from typing import Iterator

import aiofiles
from aiofiles.threadpool.binary import AsyncBufferedIOBase
from attrs import define, field

from esgpull.config import Config, Paths
from esgpull.db.models import File


@define
class Filesystem:
    root: Path
    auth: Path
    data: Path
    db: Path
    log: Path
    tmp: Path

    @staticmethod
    def from_config(config: Config) -> Filesystem:
        return Filesystem.from_paths(config.paths)

    @staticmethod
    def from_paths(paths: Paths) -> Filesystem:
        return Filesystem(
            root=paths.root,
            auth=paths.auth,
            data=paths.data,
            db=paths.db,
            log=paths.log,
            tmp=paths.tmp,
        )

    def __attrs_post_init__(self) -> None:
        self.auth.mkdir(exist_ok=True)
        self.data.mkdir(exist_ok=True)
        self.db.mkdir(exist_ok=True)
        self.log.mkdir(exist_ok=True)
        self.tmp.mkdir(exist_ok=True)

    def path_of(self, file: File) -> Path:
        return self.data / file.local_path / file.filename

    def tmp_path_of(self, file: File) -> Path:
        return self.tmp / f"{file.id}.part"

    def glob_netcdf(self) -> Iterator[Path]:
        for path in self.data.glob("**/*.nc"):
            yield path

    def open(self, file: File) -> FileObject:
        return FileObject(
            file,
            self.tmp_path_of(file),
            self.path_of(file),
        )

    def isempty(self, path: Path) -> bool:
        if next(path.iterdir(), None) is None:
            return True
        else:
            return False

    def iter_empty_parents(self, path: Path) -> Iterator[Path]:
        sample: Path | None
        for _ in range(6):  # abitrary 6 to avoid infinite loop
            if not path.exists():
                path = path.parent
                continue
            sample = next(path.glob("**/*.nc"), None)
            if sample is None and self.isempty(path):
                yield path
                path = path.parent
            else:
                return

    def delete(self, *files: File) -> None:
        for file in files:
            path = self.path_of(file)
            path.unlink(missing_ok=True)
            for subpath in self.iter_empty_parents(path.parent):
                subpath.rmdir()


@define
class FileObject:
    file: File
    tmp_path: Path
    final_path: Path
    buffer: AsyncBufferedIOBase = field(init=False)

    async def write(self, chunk: bytes) -> None:
        if self.buffer is not None:
            await self.buffer.write(chunk)
        else:
            raise ValueError("write to closed file")

    async def __aenter__(self) -> AsyncBufferedIOBase:
        self.buffer = await aiofiles.open(self.tmp_path, "wb")
        return self.buffer

    async def __aexit__(self, exc_type, exc_value, exc_traceback) -> None:
        if not self.buffer.closed:
            await self.buffer.close()
        self.final_path.parent.mkdir(parents=True, exist_ok=True)
        self.tmp_path.rename(self.final_path)
