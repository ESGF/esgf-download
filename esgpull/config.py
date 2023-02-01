from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Any

import platformdirs
import tomlkit
from attrs import Factory, define, field
from cattrs import Converter
from cattrs.gen import make_dict_unstructure_fn, override

from esgpull.constants import CONFIG_FILENAME, ROOT_ENV


class RootSource(Enum):
    Set = auto()
    UserConfig = auto()
    Env = auto()
    Default = auto()


@dataclass
class _RootSolver:
    root: Path
    default: Path
    from_set: Path | None
    from_env: Path | None
    from_user_config: Path | None
    user_config_path: Path

    def __init__(self, root: Path | str | None = None) -> None:
        self.default = Path.home() / ".esgpull"
        self.from_set = None
        self.reload()

    def reload(self) -> None:
        self._load_from_env()
        self._load_from_user_config()
        self._resolve()

    def set(self, root: Path | str | None = None) -> None:
        if root is not None:
            self.from_set = Path(root).expanduser().resolve()
        else:
            self.from_set = None
        self.reload()

    @property
    def source(self) -> RootSource:
        if self.from_set is not None:
            return RootSource.Set
        elif self.from_user_config is not None:
            return RootSource.UserConfig
        elif self.from_env is not None:
            return RootSource.Env
        else:
            return RootSource.Default

    @property
    def installed(self) -> bool:
        return self.root.is_dir()

    @property
    def user_installed(self) -> bool:
        if self.from_user_config is None:
            return False
        else:
            return self.from_user_config.is_dir()

    def mkdir(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        config_file = self.root / CONFIG_FILENAME
        if not config_file.is_file():
            config_file.touch()

    def create_user_config(self, override: bool = False) -> None:
        if self.user_config_path.is_file() and not override:
            raise ValueError(f"{self.user_config_path} already exists")
        elif self.root is None:
            raise ValueError("Nothing to do")
        user_config_dir = self.user_config_path.parent
        if not user_config_dir.is_dir():
            user_config_dir.mkdir(parents=True)
        with self.user_config_path.open("w") as f:
            f.write(str(self.root))

    def reset_user_config(self) -> None:
        user_config_dir = self.user_config_path.parent
        if self.user_config_path.is_file():
            self.user_config_path.unlink()
        if user_config_dir.is_dir():
            user_config_dir.rmdir()

    def _different_env(self) -> None:
        raise ValueError(
            f"{ROOT_ENV}={self.from_env} is different from installation "
            f"directory at {self.from_user_config}"
        )

    def _load_from_env(self) -> None:
        from_env = os.getenv(ROOT_ENV)
        if from_env is not None:
            self.from_env = Path(from_env).expanduser().resolve()
        else:
            self.from_env = None

    def _load_from_user_config(self) -> None:
        user_config_dir = platformdirs.user_config_path("esgpull")
        self.user_config_path = user_config_dir / "root.path"
        if self.user_config_path.is_file():
            content = self.user_config_path.read_text()
            self.from_user_config = Path(content).expanduser().resolve()
        else:
            self.from_user_config = None

    def _resolve(self):
        if self.from_set is not None:
            self.root = self.from_set
        elif self.from_env is None and self.from_user_config is not None:
            self.root = self.from_user_config
        elif self.from_env is not None and self.from_user_config is None:
            self.root = self.from_env
        elif self.from_env is not None and self.from_user_config is not None:
            if self.from_env != self.from_user_config:
                self.root = None
                self._different_env()
            else:
                self.root = self.from_user_config
        else:
            self.root = self.default


RootSolver = _RootSolver()


@define
class Paths:
    auth: Path = field(converter=Path)
    data: Path = field(converter=Path)
    db: Path = field(converter=Path)
    log: Path = field(converter=Path)
    tmp: Path = field(converter=Path)

    @auth.default
    def _auth_factory(self) -> Path:
        return RootSolver.root / "auth"

    @data.default
    def _data_factory(self) -> Path:
        return RootSolver.root / "data"

    @db.default
    def _db_factory(self) -> Path:
        return RootSolver.root / "db"

    @log.default
    def _log_factory(self) -> Path:
        return RootSolver.root / "log"

    @tmp.default
    def _tmp_factory(self) -> Path:
        return RootSolver.root / "tmp"


@define
class Search:
    index_node: str = "esgf-node.ipsl.upmc.fr"
    http_timeout: int = 20
    max_concurrent: int = 5
    page_limit: int = 50


@define
class CLI:
    page_size: int = 20


@define
class Db:
    filename: str = "esgpull.db"


@define
class Download:
    chunk_size: int = 1 << 26  # 64 MiB
    http_timeout: int = 20
    max_concurrent: int = 5


@define
class Config:
    paths: Paths = Factory(Paths)
    cli: CLI = Factory(CLI)
    db: Db = Factory(Db)
    search: Search = Factory(Search)
    download: Download = Factory(Download)
    _raw: str | None = field(init=False, default=None)

    @classmethod
    def load(cls, root: Path) -> Config:
        config_file = root / CONFIG_FILENAME
        if config_file.is_file():
            with config_file.open() as fh:
                raw = fh.read()
            doc = tomlkit.loads(raw)
        else:
            raw = None
            doc = tomlkit.TOMLDocument()
        # doc.add(tomlkit.key(["paths", "root"]), tomlkit.string(str(root)))
        config = _converter_defaults.structure(doc, cls)
        config._raw = raw
        return config

    @classmethod
    def default(cls) -> Config:
        return cls.load(RootSolver.root)

    def dumps(self, defaults: bool = True, comments: bool = False) -> str:
        return self.dump(defaults, comments).as_string()

    def dump(
        self, defaults: bool = True, comments: bool = False
    ) -> tomlkit.TOMLDocument:
        if defaults:
            converter = _converter_defaults
        else:
            converter = _converter_no_defaults
        dump = converter.unstructure(self)
        if not defaults:
            pop_empty(dump)
        doc = tomlkit.TOMLDocument()
        doc.update(dump)
        # if comments and self._raw is not None:
        #     original = tomlkit.loads(self._raw)
        return doc


def _make_converter(omit_default: bool) -> Converter:
    conv = Converter(omit_if_default=omit_default, forbid_extra_keys=True)
    conv.register_unstructure_hook(Path, str)
    conv.register_unstructure_hook(
        Config,
        make_dict_unstructure_fn(Config, conv, _raw=override(omit=True)),
    )
    return conv


_converter_defaults = _make_converter(omit_default=False)
_converter_no_defaults = _make_converter(omit_default=True)


def pop_empty(d: dict[str, Any]) -> None:
    keys = list(d.keys())
    for key in keys:
        value = d[key]
        if isinstance(value, dict):
            pop_empty(value)
            if not value:
                d.pop(key)
