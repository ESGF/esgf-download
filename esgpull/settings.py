# from __future__ import annotations
import os
from enum import Enum, unique
from pathlib import Path
from typing import Any

import rich
import tomlkit
import typedload
from attrs import Factory, define, field

from esgpull.constants import ENV_VARNAME, SETTINGS_FILENAME


@define
class Paths:
    root: Path = field()
    auth: Path = field()
    data: Path = field()
    db: Path = field()
    settings: Path = field()
    # settings_filename: str = "settings.toml"
    tmp: Path = field()

    @root.default
    def _root_factory(self) -> Path:
        root_env = os.environ.get(ENV_VARNAME)
        if root_env is None:
            root = Path.home() / ".esgpull"
            rich.print(f":warning-emoji: Using default root directory: {root}")
            # raise NoRootError
        else:
            root = Path(root_env)
        return root

    @auth.default
    def _auth_factory(self) -> Path:
        return self.root / "auth"

    @data.default
    def _data_factory(self) -> Path:
        return self.root / "data"

    @db.default
    def _db_factory(self) -> Path:
        return self.root / "db"

    @settings.default
    def _settings_factory(self) -> Path:
        return self.root / "settings"

    @tmp.default
    def _tmp_factory(self) -> Path:
        return self.root / "tmp"


@define
class Core:
    paths: Paths = Paths()
    # credentials_filename: str = "credentials.toml"
    db_filename: str = "esgpull.db"
    # settings_filename: str = "settings.toml"


@define
class Search:
    index_node: str = "esgf-node.ipsl.upmc.fr"
    http_timeout: int = 20


@define
class Db:
    verbosity: int = 0


@unique
class DownloadKind(str, Enum):
    Simple = "Simple"
    Distributed = "Distributed"


@define
class Download:
    chunk_size: int = 1 << 26
    kind: DownloadKind = DownloadKind.Simple
    http_timeout: int = 20
    max_concurrent: int = 5


@define
class Settings:
    core: Core = Factory(Core)
    search: Search = Factory(Search)
    db: Db = Factory(Db)
    download: Download = Factory(Download)

    @classmethod
    def from_file(cls, root: Path | str | None = None) -> "Settings":
        if root is not None:
            paths = Paths(root=Path(root))
        else:
            paths = Paths()

        # path = paths.settings / paths.settings_filename
        path = paths.settings / SETTINGS_FILENAME
        if not path.exists():
            # path.touch()  # [?]TODO: maybe do not do this implicitly
            settings = cls()
        else:
            with path.open() as fh:
                settings = typedload.load(tomlkit.load(fh), cls)
        return settings

    def dump(self) -> dict[str, Any]:
        return typedload.dump(self)

    def dict(self) -> dict[str, Any]:
        return typedload.dump(self, hidedefault=False)

    # def save(self) -> None:
    #     path = self.core.paths.settings / SETTINGS_FILENAME
    #     with path.open("w") as f:
    #         tomlkit.dump(self.dump(), f)


__all__ = ["Settings"]
