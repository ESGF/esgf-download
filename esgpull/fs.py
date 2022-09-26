import os
from typing import Iterator, Optional

from pathlib import Path
from dataclasses import dataclass

import aiofiles

from esgpull.types import File
from esgpull.exceptions import NoRootError


@dataclass(init=False)
class Filesystem:
    root: Path

    def __init__(self, path: Optional[str | Path] = None) -> None:
        env_home = os.environ.get("ESGPULL_HOME")
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

    def path_of(self, file: File) -> Path:
        return self.data / file.local_path / file.filename

    def glob_netcdf(self) -> Iterator[Path]:
        for path in self.data.glob("**/*.nc"):
            yield path

    async def write(self, file: File, data: bytes) -> None:
        path = self.path_of(file)
        path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)

    def isempty(self, path: Path) -> bool:
        if next(path.iterdir(), None) is None:
            return True
        else:
            return False

    def iter_empty_parents(self, path: Path) -> Iterator[Path]:
        sample: Optional[Path]
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


__all__ = ["Filesystem"]
