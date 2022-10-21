from __future__ import annotations

from pathlib import Path
from typing import Iterator

import aiofiles
from aiofiles.threadpool.binary import AsyncBufferedIOBase
from attrs import define, field

from esgpull.db.models import File
from esgpull.settings import Paths


@define
class Filesystem:
    paths: Paths

    def __attrs_post_init__(self) -> None:
        self.paths.root.mkdir(exist_ok=True)
        self.paths.auth.mkdir(exist_ok=True)
        self.paths.data.mkdir(exist_ok=True)
        self.paths.db.mkdir(exist_ok=True)
        self.paths.settings.mkdir(exist_ok=True)
        self.paths.tmp.mkdir(exist_ok=True)

    def path_of(self, file: File) -> Path:
        return self.paths.data / file.local_path / file.filename

    def tmp_path_of(self, file: File) -> Path:
        return self.paths.tmp / f"{file.id}.{file.filename}"

    def glob_netcdf(self) -> Iterator[Path]:
        for path in self.paths.data.glob("**/*.nc"):
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


__all__ = ["Filesystem", "FileObject"]
