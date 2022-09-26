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

    def glob_netcdf(self) -> Iterable[Path]:
        for path in self.data.glob("**/*.nc"):
            yield path

    async def write(self, file: File, data: bytes) -> None:
        path = self.path_of(file)
        path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)

    def delete(self, *files: File) -> None:
        for file in files:
            path = self.path_of(file)
            path.unlink(missing_ok=True)
            if path.parent.is_dir():
                path.parent.rmdir()


__all__ = ["Filesystem"]
