from __future__ import annotations

from collections.abc import Container
from pathlib import Path
from typing import Any, Iterator

import tomlkit
from attrs import Factory, define, field
from cattrs import Converter
from cattrs.gen import make_dict_unstructure_fn, override

from esgpull.constants import CONFIG_FILENAME
from esgpull.exceptions import VirtualConfigError
from esgpull.install_config import InstallConfig


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

    def __iter__(self) -> Iterator[Path]:
        yield self.auth
        yield self.data
        yield self.db
        yield self.log
        yield self.tmp


@define
class Credentials:
    filename: str = "credentials.toml"


@define
class Cli:
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
class Search:
    index_node: str = "esgf-node.ipsl.upmc.fr"
    http_timeout: int = 20
    max_concurrent: int = 5
    page_limit: int = 50


@define
class Config:
    paths: Paths = Factory(Paths)
    credentials: Credentials = Factory(Credentials)
    cli: Cli = Factory(Cli)
    db: Db = Factory(Db)
    download: Download = Factory(Download)
    search: Search = Factory(Search)
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

    def update_item(
        self,
        key: str,
        value: int | str,
        empty_ok: bool = False,
    ) -> int | str | None:
        if self._config_file is not None and self._raw is None and empty_ok:
            self.generate(key)
        if self._raw is None:
            raise VirtualConfigError
        else:
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

    def generate(self, key: str | None = None) -> None:
        if self._config_file is None:
            raise VirtualConfigError
        elif self._config_file.is_file():
            raise FileExistsError(self._config_file)
        with self._config_file.open("w") as f:
            if key is None:
                self._raw = self.dump()
            else:
                self._raw = tomlkit.TOMLDocument()
                *parts, last = key.split(".")
                doc: dict = {last: "NOT_SET"}
                for part in parts[::-1]:
                    doc = {part: doc}
                self._raw.update(doc)
            tomlkit.dump(self._raw, f)

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
