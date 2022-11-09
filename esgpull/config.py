from __future__ import annotations

from pathlib import Path
from typing import Any

import tomlkit
from attrs import Factory, define, field
from cattrs import Converter
from cattrs.gen import make_dict_unstructure_fn, override

from esgpull.constants import CONFIG_FILENAME
from esgpull.utils import Root


@define
class Paths:
    root: Path = field(converter=Path, factory=Root.get)
    auth: Path = field(converter=Path)
    data: Path = field(converter=Path)
    db: Path = field(converter=Path)
    log: Path = field(converter=Path)
    tmp: Path = field(converter=Path)

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
    max_concurrent: int = 5


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
    db: Db = Factory(Db)
    search: Search = Factory(Search)
    download: Download = Factory(Download)
    _raw: str | None = field(init=False, default=None)

    @staticmethod
    def load(root: Path) -> Config:
        config_file = root / CONFIG_FILENAME
        if config_file.is_file():
            with config_file.open() as fh:
                raw = fh.read()
            doc = tomlkit.loads(raw)
        else:
            raw = None
            doc = tomlkit.TOMLDocument()
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
