from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from enum import Enum, auto
from pathlib import Path
from typing import Any, cast

import tomlkit
from attrs import Factory, define, field, fields
from attrs import has as attrs_has
from cattrs import Converter
from cattrs.gen import make_dict_unstructure_fn, override
from tomlkit import TOMLDocument

from esgpull.constants import CONFIG_FILENAME
from esgpull.exceptions import BadConfigError, VirtualConfigError
from esgpull.install_config import InstallConfig
from esgpull.models.options import Options

logger = logging.getLogger("esgpull")


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
    disable_ssl: bool = False
    disable_checksum: bool = False
    show_filename: bool = False


@define
class DefaultOptions:
    distrib: str = Options._distrib_.name
    latest: str = Options._latest_.name
    replica: str = Options._replica_.name
    retracted: str = Options._retracted_.name

    def asdict(self) -> dict[str, str]:
        return dict(
            distrib=self.distrib,
            latest=self.latest,
            replica=self.replica,
            retracted=self.retracted,
        )


@define
class API:
    index_node: str = "esgf-node.ipsl.upmc.fr"
    http_timeout: int = 20
    max_concurrent: int = 5
    page_limit: int = 50
    default_options: DefaultOptions = Factory(DefaultOptions)
    default_query_id: str = ""


def fix_rename_search_api(doc: TOMLDocument) -> TOMLDocument:
    if "api" in doc and "search" in doc:
        raise KeyError(
            "Both 'api' and 'search' (deprecated) are used in your "
            "config, please use 'api' only."
        )
    elif "search" in doc:
        logger.warn(
            "Deprecated key 'search' is used in your config, "
            "please use 'api' instead."
        )
        doc["api"] = doc.pop("search")
    return doc


config_fixers = [fix_rename_search_api]


class ConfigKind(Enum):
    Virtual = auto()
    NoFile = auto()
    Partial = auto()
    Complete = auto()


class ConfigKey:
    path: tuple[str, ...]

    def __init__(self, first: str | tuple[str, ...], *rest: str) -> None:
        if isinstance(first, tuple):
            self.path = first + rest
        elif "." in first:
            self.path = tuple(first.split(".")) + rest
        else:
            self.path = (first,) + rest

    def __iter__(self) -> Iterator[str]:
        yield from self.path

    def __hash__(self) -> int:
        return hash(self.path)

    def __repr__(self) -> str:
        return ".".join(self)

    def __add__(self, path: str) -> ConfigKey:
        return ConfigKey(self.path, path)

    def __len__(self) -> int:
        return len(self.path)

    def exists_in(self, source: Mapping | None) -> bool:
        if source is None:
            return False
        doc = source
        for key in self:
            if key in doc:
                doc = doc[key]
            else:
                return False
        return True

    def value_of(self, source: Mapping) -> Any:
        doc = source
        for key in self:
            doc = doc[key]
        return doc


def iter_keys(
    source: Mapping,
    path: ConfigKey | None = None,
) -> Iterator[ConfigKey]:
    for key in source.keys():
        if path is None:
            local_path = ConfigKey(key)
        else:
            local_path = path + key
        if isinstance(source[key], Mapping):
            yield from iter_keys(source[key], local_path)
        else:
            yield local_path


@define
class Config:
    paths: Paths = Factory(Paths)
    credentials: Credentials = Factory(Credentials)
    cli: Cli = Factory(Cli)
    db: Db = Factory(Db)
    download: Download = Factory(Download)
    api: API = Factory(API)
    _raw: TOMLDocument | None = field(init=False, default=None)
    _config_file: Path | None = field(init=False, default=None)

    @classmethod
    def load(cls, path: Path) -> Config:
        config_file = path / CONFIG_FILENAME
        if config_file.is_file():
            with config_file.open() as fh:
                doc = tomlkit.load(fh)
                for fixer in config_fixers:
                    try:
                        doc = fixer(doc)
                    except Exception:
                        raise BadConfigError(config_file)
                raw = doc
        else:
            doc = TOMLDocument()
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

    @property
    def kind(self) -> ConfigKind:
        if self._config_file is None:
            return ConfigKind.Virtual
        elif not self._config_file.is_file():
            return ConfigKind.NoFile
        elif self.unset_options():
            return ConfigKind.Partial
        else:
            return ConfigKind.Complete

    def dumps(self, defaults: bool = True, comments: bool = False) -> str:
        return self.dump(defaults, comments).as_string()

    def dump(
        self,
        defaults: bool = True,
        comments: bool = False,
    ) -> TOMLDocument:
        if defaults:
            converter = _converter_defaults
        else:
            converter = _converter_no_defaults
        dump = converter.unstructure(self)
        if not defaults:
            pop_empty(dump)
        doc = TOMLDocument()
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
        if self._raw is None and empty_ok:
            self._raw = TOMLDocument()
        if self._raw is None:
            raise VirtualConfigError
        else:
            doc: dict = self._raw
        obj = self
        *parts, last = ConfigKey(key)
        for part in parts:
            doc.setdefault(part, {})
            doc = doc[part]
            obj = getattr(obj, part)
        value_type = getattr(fields(type(obj)), last).type
        old_value = getattr(obj, last)
        if attrs_has(value_type):
            raise KeyError(key)
        elif value_type is str:
            ...
        elif value_type is int:
            try:
                value = value_type(value)
            except Exception:
                ...
        elif value_type is bool:
            if isinstance(value, bool):
                ...
            elif isinstance(value, str):
                if value.lower() in ["on", "true"]:
                    value = True
                elif value.lower() in ["off", "false"]:
                    value = False
                else:
                    raise ValueError(value)
            else:
                raise TypeError(value)
        setattr(obj, last, value)
        doc[last] = value
        return old_value

    def set_default(self, key: str) -> int | str | None:
        ckey = ConfigKey(key)
        if self._raw is None:
            raise VirtualConfigError()
        elif not ckey.exists_in(self._raw):
            return None
        default_config = self.__class__()
        default_value = ckey.value_of(default_config.dump())
        old_value: Any = ckey.value_of(self.dump())
        first_pass = True
        obj = self
        for idx in range(len(ckey), 0, -1):
            *parts, last = ckey.path[:idx]
            doc: tomlkit.container.Container = self._raw
            for part in parts:
                if first_pass:
                    obj = getattr(obj, part)
                doc = cast(tomlkit.container.Container, doc[part])
            if first_pass:
                doc.remove(last)
                setattr(obj, last, default_value)
                first_pass = False
            elif (
                (value := doc[last])
                and isinstance(value, tomlkit.container.Container)
                and len(value) == 0
            ):
                doc.remove(last)
        return old_value

    def unset_options(self) -> list[ConfigKey]:
        result: list[ConfigKey] = []
        raw: dict
        dump = self.dump()
        if self._raw is None:
            raw = {}
        else:
            raw = self._raw
        for ckey in iter_keys(dump):
            if not ckey.exists_in(raw):
                result.append(ckey)
        return result

    def generate(
        self,
        overwrite: bool = False,
    ) -> None:
        match (self.kind, overwrite):
            case (ConfigKind.Virtual, _):
                raise VirtualConfigError
            case (ConfigKind.Partial, overwrite):
                defaults = self.dump()
                for ckey in self.unset_options():
                    self.update_item(str(ckey), ckey.value_of(defaults))
            case (ConfigKind.Partial | ConfigKind.Complete, _):
                raise FileExistsError(self._config_file)
            case (ConfigKind.NoFile, _):
                self._raw = self.dump()
            case _:
                raise ValueError(self.kind)
        self.write()

    def write(self) -> None:
        if self.kind == ConfigKind.Virtual or self._raw is None:
            raise VirtualConfigError
        config_file = cast(Path, self._config_file)
        with config_file.open("w") as f:
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
