import os
from typing import Iterable, Optional

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
        if not self.root.is_dir():
            self.root.mkdir()
            self.data.mkdir()
            self.db.mkdir()
            self.auth.mkdir()

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def db(self) -> Path:
        return self.root / "db"

    @property
    def auth(self) -> Path:
        return self.root / "auth"

    def path_of(self, file: File) -> Path:
        return self.data / file.local_path / file.filename

    def glob_netcdf(self) -> Iterable[Path]:
        for path in self.data.glob("**/*.nc"):
            yield path

    async def write(self, file: File, data: bytes) -> None:
        path = self.path_of(file)
        path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)


__all__ = ["Filesystem"]
