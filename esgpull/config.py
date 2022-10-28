from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import rich
import tomlkit
from attrs import Factory, define, field
from cattrs import Converter
from cattrs.gen import make_dict_unstructure_fn, override

from esgpull.constants import CONFIG_FILENAME, ENV_VARNAME


@define
class Paths:
    root: Path = field(converter=Path)
    auth: Path = field(converter=Path)
    data: Path = field(converter=Path)
    db: Path = field(converter=Path)
    log: Path = field(converter=Path)
    tmp: Path = field(converter=Path)

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

    @log.default
    def _log_factory(self) -> Path:
        return self.root / "log"

    @tmp.default
    def _tmp_factory(self) -> Path:
        return self.root / "tmp"


@define
class Search:
    index_node: str = "esgf-node.ipsl.upmc.fr"
    http_timeout: int = 20


@define
class Db:
    filename: str = "esgpull.db"
    verbosity: int = 0


@define
class Download:
    chunk_size: int = 1 << 26  # 64 MiB
    http_timeout: int = 20
    max_concurrent: int = 5


@define
class Config:
    paths: Paths = Factory(Paths)
    db: Db = Factory(Db)
    search: Search = Factory(Search)
    download: Download = Factory(Download)
    _raw: str | None = field(init=False, default=None)

    @staticmethod
    def load(root: Path) -> Config:
        config_file = root / CONFIG_FILENAME
        if not config_file.is_file():
            config_file.touch()
        with config_file.open() as fh:
            raw = fh.read()
        doc = tomlkit.loads(raw)
        doc.add(tomlkit.key(["paths", "root"]), tomlkit.string(str(root)))
        config = _converter_defaults.structure(doc, Config)
        config._raw = raw
        return config

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


_converter_defaults = Converter(omit_if_default=False, forbid_extra_keys=True)
_converter_defaults.register_unstructure_hook(Path, str)
_converter_defaults.register_unstructure_hook(
    Config,
    make_dict_unstructure_fn(
        Config, _converter_defaults, _raw=override(omit=True)
    ),
)
_converter_no_defaults = Converter(
    omit_if_default=True, forbid_extra_keys=True
)
_converter_no_defaults.register_unstructure_hook(Path, str)
_converter_no_defaults.register_unstructure_hook(
    Config,
    make_dict_unstructure_fn(
        Config, _converter_no_defaults, _raw=override(omit=True)
    ),
)


def pop_empty(d: dict[str, Any]) -> None:
    keys = list(d.keys())
    for key in keys:
        value = d[key]
        if isinstance(value, dict):
            pop_empty(value)
            if not value:
                d.pop(key)
