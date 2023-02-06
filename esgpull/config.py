from __future__ import annotations

import json
import os
from collections.abc import Container
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import platformdirs
import tomlkit
from attrs import Factory, define, field
from cattrs import Converter
from cattrs.gen import make_dict_unstructure_fn, override
from typing_extensions import NotRequired, TypedDict

from esgpull.constants import CONFIG_FILENAME, ROOT_ENV
from esgpull.exceptions import (
    NameAlreadyInstalled,
    PathAlreadyInstalled,
    VirtualConfigError,
)


@dataclass(init=False)
class Install:
    path: Path
    name: str | None = None

    def __init__(self, path: Path | str, name: str | None = None) -> None:
        self.path = Path(path)
        self.name = name

    def asdict(self) -> InstallDict:
        result: InstallDict = {"path": str(self.path)}
        if self.name is not None:
            result["name"] = self.name
        return result


class InstallDict(TypedDict):
    path: str
    name: NotRequired[str]


class InstallConfigDict(TypedDict):
    current: NotRequired[int]
    installs: list[InstallDict]


@dataclass(init=False)
class _InstallConfig:
    path: Path
    current_idx: int | None
    installs: list[Install]

    def __init__(self) -> None:
        user_config_dir = platformdirs.user_config_path("esgpull")
        self.path = user_config_dir / "installs.json"
        if self.path.is_file():
            with self.path.open() as f:
                content = json.load(f)
            self.current_idx = content.get("current")
            installs = content.get("installs", [])
            self.installs = [Install(**inst) for inst in installs]
        else:
            self.current_idx = None
            self.installs = []

    def fullpath(self, path: Path) -> Path:
        return path.expanduser().resolve()

    @property
    def current(self) -> Install | None:
        env = os.getenv(ROOT_ENV)
        if env is not None:
            return self.installs[int(env)]
        elif self.current_idx is not None:
            return self.installs[self.current_idx]
        else:
            return None

    @property
    def default(self) -> Path:
        return Path.home() / ".esgpull"

    def activate_msg(self, idx: int, commented: bool = False) -> str:
        install = self.installs[idx]
        name = install.name or install.path
        msg = f"""
To choose {install.path} as the current install location:

$ esgpull self choose {name}


To enable {install.path} for the current shell:

$ eval $(esgpull self activate {name})
""".strip()
        if commented:
            lines = msg.splitlines()
            return "\n".join("# " + line for line in lines)
        else:
            return msg

    def activate_needs_eval(self, idx: int) -> str:
        export = f"export {ROOT_ENV}={idx}"
        comment = self.activate_msg(idx, commented=True)
        return "\n".join([export, comment])

    def asdict(self) -> InstallConfigDict:
        result: InstallConfigDict = {
            "installs": [inst.asdict() for inst in self.installs]
        }
        if self.current_idx is not None:
            result["current"] = self.current_idx
        return result

    def add(self, path: Path, name: str | None = None) -> int:
        install = Install(self.fullpath(path), name)
        idx_path = self.index(path=install.path)
        if idx_path > -1:
            raise PathAlreadyInstalled(
                path=install.path,
                msg=self.activate_msg(idx_path),
            )
        if name is not None:
            idx_name = self.index(name=name)
            if idx_name > -1:
                raise NameAlreadyInstalled(
                    name=name,
                    msg=self.activate_msg(idx_name),
                )
        self.installs.append(install)
        return len(self.installs) - 1

    def choose(
        self,
        *,
        idx: int | None = None,
        name: str | None = None,
        path: Path | None = None,
    ) -> None:
        if idx is not None:
            self.current_idx = idx
        elif name is not None:
            self.current_idx = self.index(name=name)
        elif path is not None:
            self.current_idx = self.index(path=path)
        else:
            self.current_idx = None
        if self.current_idx == -1:
            self.current_idx = None

    def write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w") as f:
            json.dump(self.asdict(), f)

    def index(
        self,
        *,
        name: str | None = None,
        path: Path | None = None,
    ) -> int:
        if name is not None:
            return self._index_name(name)
        elif path is not None:
            return self._index_path(path)
        else:
            raise ValueError("nothing provided")

    def _index_name(self, name: str) -> int:
        for i, inst in enumerate(self.installs):
            if inst.name is not None and name == inst.name:
                return i
        return -1

    def _index_path(self, path: Path) -> int:
        path = self.fullpath(path)
        for i, inst in enumerate(self.installs):
            if path == inst.path:
                return i
        return -1


InstallConfig = _InstallConfig()


@define
class Paths:
    auth: Path = field(converter=Path)
    data: Path = field(converter=Path)
    db: Path = field(converter=Path)
    log: Path = field(converter=Path)
    tmp: Path = field(converter=Path)

    @auth.default
    def _auth_factory(self) -> Path:
        if InstallConfig.current is not None:
            root = InstallConfig.current.path
        else:
            root = InstallConfig.default
        return root / "auth"

    @data.default
    def _data_factory(self) -> Path:
        if InstallConfig.current is not None:
            root = InstallConfig.current.path
        else:
            root = InstallConfig.default
        return root / "data"

    @db.default
    def _db_factory(self) -> Path:
        if InstallConfig.current is not None:
            root = InstallConfig.current.path
        else:
            root = InstallConfig.default
        return root / "db"

    @log.default
    def _log_factory(self) -> Path:
        if InstallConfig.current is not None:
            root = InstallConfig.current.path
        else:
            root = InstallConfig.default
        return root / "log"

    @tmp.default
    def _tmp_factory(self) -> Path:
        if InstallConfig.current is not None:
            root = InstallConfig.current.path
        else:
            root = InstallConfig.default
        return root / "tmp"


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
    _raw: tomlkit.TOMLDocument | None = field(init=False, default=None)
    _config_file: Path | None = field(init=False, default=None)

    @classmethod
    def load(cls, path: Path) -> Config:
        config_file = path / CONFIG_FILENAME
        if config_file.is_file():
            with config_file.open() as fh:
                doc = tomlkit.load(fh)
                raw = doc
        else:
            doc = tomlkit.TOMLDocument()
            raw = None
        config = _converter_defaults.structure(doc, cls)
        config._raw = raw
        config._config_file = config_file
        return config

    @classmethod
    def default(cls) -> Config:
        if InstallConfig.current is not None:
            root = InstallConfig.current.path
        else:
            root = InstallConfig.default
        return cls.load(root)

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

    def update_item(self, key: str, value: int | str) -> int | str | None:
        if self._config_file is None or self._raw is None:
            raise VirtualConfigError
        doc: dict = self._raw
        obj = self
        *parts, last = key.split(".")
        for part in parts:
            doc.setdefault(part, {})
            doc = doc[part]
            obj = getattr(self, part)
        old_value = doc.get(last)
        if isinstance(doc[last], str):
            ...
        elif isinstance(doc[last], Container):
            raise KeyError(key)
        try:
            value = int(value)
        except ValueError:
            ...
        setattr(obj, last, value)
        doc[last] = value
        return old_value

    def write(self) -> None:
        if self._raw is None or self._config_file is None:
            raise VirtualConfigError
        with self._config_file.open("w") as f:
            tomlkit.dump(self._raw, f)


def _make_converter(omit_default: bool) -> Converter:
    conv = Converter(omit_if_default=omit_default, forbid_extra_keys=True)
    conv.register_unstructure_hook(Path, str)
    conv.register_unstructure_hook(
        Config,
        make_dict_unstructure_fn(
            Config,
            conv,
            _raw=override(omit=True),
            _config_file=override(omit=True),
        ),
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
