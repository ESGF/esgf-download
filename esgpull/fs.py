from typing import Iterable

from pathlib import Path
from dataclasses import dataclass

import aiofiles

from esgpull.types import File


@dataclass
class Filesystem:
    root: Path

    def __post_init__(self):
        self.root = Path(self.root)

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def db(self) -> Path:
        return self.root / "db"

    def path_of(self, file: File) -> Path:
        return self.data / file.local_path / file.filename

    def glob_netcdf(self) -> Iterable[Path]:
        for path in self.data.glob("**/*.nc"):
            yield path

    async def write_file(self, file: File, data: bytes) -> None:
        path = self.path_of(file)
        path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(path, "wb") as f:
            await f.write(data)


__all__ = ["Filesystem"]
