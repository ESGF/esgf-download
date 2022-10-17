from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Awaitable, Callable, Iterator

import aiofiles

from esgpull.constants import ENV_VARNAME
from esgpull.exceptions import NoRootError
from esgpull.types import File


@dataclass(init=False)
class Filesystem:
    root: Path

    def __init__(self, path: str | Path | None = None) -> None:
        env_home = os.environ.get(ENV_VARNAME)
        if path is not None:
            self.root = Path(path)
        elif env_home is not None:
            self.root = Path(env_home)
        else:
            raise NoRootError
        self.root.mkdir(exist_ok=True)
        self.auth.mkdir(exist_ok=True)
        self.data.mkdir(exist_ok=True)
        self.db.mkdir(exist_ok=True)
        self.settings.mkdir(exist_ok=True)
        self.tmp.mkdir(exist_ok=True)

    @property
    def auth(self) -> Path:
        return self.root / "auth"

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def db(self) -> Path:
        return self.root / "db"

    @property
    def settings(self) -> Path:
        return self.root / "settings"

    @property
    def tmp(self) -> Path:
        return self.root / ".tmp"

    def path_of(self, file: File) -> Path:
        return self.data / file.local_path / file.filename

    def tmp_path_of(self, file: File) -> Path:
        return self.tmp / f"{file.id}.{file.filename}"

    def glob_netcdf(self) -> Iterator[Path]:
        for path in self.data.glob("**/*.nc"):
            yield path

    def make_writer(self, file: File) -> Writer:
        tmp_path = self.tmp_path_of(file)
        final_path = self.path_of(file)
        return Writer(file, tmp_path, final_path)

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


class Writer:
    def __init__(self, file: File, tmp_path: Path, final_path: Path) -> None:
        self.file = File
        self.tmp_path = tmp_path
        self.final_path = final_path
        self.can_write = False

    @asynccontextmanager
    async def open(self) -> AsyncIterator[Callable[[bytes], Awaitable[None]]]:
        tmp = await aiofiles.open(self.tmp_path, "wb")

        async def write(chunk: bytes) -> None:
            await tmp.write(chunk)

        try:
            yield write
        finally:
            await tmp.close()
            self.final_path.parent.mkdir(parents=True, exist_ok=True)
            self.tmp_path.rename(self.final_path)


__all__ = ["Filesystem", "Writer"]
