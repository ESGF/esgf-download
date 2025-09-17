from __future__ import annotations

import logging
from enum import Enum, auto
from pathlib import Path
from typing import Any, Iterator, Mapping

import tomlkit
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from esgpull.constants import CONFIG_FILENAME
from esgpull.exceptions import BadConfigError, VirtualConfigError
from esgpull.install_config import InstallConfig
from esgpull.models.options import Option, Options

logger = logging.getLogger("esgpull")


def _get_root() -> Path:
    if InstallConfig.current is not None:
        return InstallConfig.current.path
    else:
        return InstallConfig.default


class Paths(BaseModel, validate_assignment=True, validate_default=True):
    data: Path = Path("data")
    db: Path = Path("db")
    log: Path = Path("log")
    tmp: Path = Path("tmp")
    plugins: Path = Path("plugins")

    @field_validator(
        "data",
        "db",
        "log",
        "tmp",
        "plugins",
        mode="after",
    )
    @classmethod
    def _set_path_from_root(cls, value: Path) -> Path:
        root = _get_root()
        if not value.is_absolute():
            value = root / value
        return value

    def values(self) -> Iterator[Path]:
        yield self.data
        yield self.db
        yield self.log
        yield self.tmp
        yield self.plugins


class Credentials(BaseModel, validate_assignment=True):
    filename: str = "credentials.toml"


class Cli(BaseModel, validate_assignment=True):
    page_size: int = 20


class Db(BaseModel, validate_assignment=True):
    filename: str = "esgpull.db"


class Download(BaseModel, validate_assignment=True):
    chunk_size: int = 1 << 26
    http_timeout: int = 20
    max_concurrent: int = 5
    disable_ssl: bool = False
    disable_checksum: bool = False
    show_filename: bool = False


class DefaultOptions(BaseModel, validate_assignment=True):
    distrib: str = Options._distrib_.name
    latest: str = Options._latest_.name
    replica: str = Options._replica_.name
    retracted: str = Options._retracted_.name

    @field_validator(
        "distrib", "latest", "replica", "retracted", mode="before"
    )
    @classmethod
    def _is_valid_option(cls, value: str | Option) -> str:
        if isinstance(value, str):
            return Option(value.lower()).name
        else:
            return value.name

    def asdict(self) -> dict[str, str]:
        return dict(
            distrib=self.distrib,
            latest=self.latest,
            replica=self.replica,
            retracted=self.retracted,
        )


class API(BaseModel, validate_assignment=True):
    index_node: str = "esgf-node.ipsl.upmc.fr"
    http_timeout: int = 20
    max_concurrent: int = 5
    page_limit: int = 50
    default_options: DefaultOptions = Field(default_factory=DefaultOptions)
    default_query_id: str = ""
    use_custom_distribution_algorithm: bool = False


class Plugins(BaseModel, validate_assignment=True):
    enabled: bool = False


class ConfigKind(Enum):
    Virtual = auto()
    NoFile = auto()
    Partial = auto()
    Complete = auto()


class ConfigKey:
    path: tuple[str, ...]

    def __init__(
        self,
        first: str | tuple[str, ...] | list[str],
        *rest: str,
    ) -> None:
        if isinstance(first, (tuple, list)):
            self.path = tuple(first) + rest
        elif "." in first:
            self.path = tuple(first.split(".")) + rest
        else:
            self.path = (first,) + rest

    def __iter__(self) -> Iterator[str]:
        yield from self.path

    def __hash__(self) -> int:
        return hash(str(self.path))

    def __eq__(self, other: object) -> bool:
        match other:
            case ConfigKey():
                return self.path == other.path
            case _:
                raise TypeError(type(other))

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

    def value_of(self, source: Any) -> Any:
        doc = source
        for key in self:
            try:
                doc = doc[key]
            except TypeError:
                doc = getattr(doc, key)
        return doc


def fix_rename_search_api(doc: dict) -> dict:
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


def fix_remove_auth(doc: dict) -> dict:
    if "paths" in doc and "auth" in doc["paths"]:
        logger.warn(
            "Deprecated 'paths.auth' is present in your config, "
            "you can remove it safely."
        )
        doc["paths"].pop("auth")
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


def pop_and_clear_empty_parents(source: Mapping, ckey: ConfigKey):
    *parent_path, last_key = ckey.path
    parent_ckey = ConfigKey(parent_path)
    parent_ckey.value_of(source).pop(last_key)

    for i in range(len(parent_path), 0, -1):
        parent_ckey = ConfigKey(parent_path[: i - 1])
        container_ckey = ConfigKey(parent_path[:i])
        parent = parent_ckey.value_of(source)
        container = container_ckey.value_of(source)
        if isinstance(container, dict) and len(container) == 0:
            parent.pop(container_ckey.path[-1])
        else:
            break  # Stop if we hit a non-empty container


config_fixers = [fix_rename_search_api, fix_remove_auth]


class TomlKitConfigSettingsSource(TomlConfigSettingsSource):
    def _read_file(self, file_path: Path) -> dict[str, Any]:
        with open(file_path, mode="rb") as toml_file:
            doc = tomlkit.load(toml_file)
            for fixer in config_fixers:
                try:
                    doc = fixer(doc)
                except Exception:
                    raise BadConfigError(file_path)
            doc = dict(doc)
            doc["raw"] = doc
            return doc


class Config(BaseSettings):
    # TODO: set in a load method instead
    # model_config = SettingsConfigDict(toml_file=_get_root() / "config.toml")
    model_config = SettingsConfigDict(toml_file=None)

    paths: Paths = Field(default_factory=Paths)
    credentials: Credentials = Field(default_factory=Credentials)
    cli: Cli = Field(default_factory=Cli)
    db: Db = Field(default_factory=Db)
    download: Download = Field(default_factory=Download)
    api: API = Field(default_factory=API)
    plugins: Plugins = Field(default_factory=Plugins)
    raw: dict[str, Any] | None = Field(
        default=None,
        repr=False,
        exclude=True,
    )
    config_file: Path | None = Field(
        default=None,
        repr=False,
        exclude=True,
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            TomlKitConfigSettingsSource(settings_cls),
        )

    @classmethod
    def load(cls, path: Path) -> Config:
        try:
            file_path = path / CONFIG_FILENAME
            cls.model_config["toml_file"] = file_path
            return cls(config_file=file_path)
        finally:
            cls.model_config["toml_file"] = None

    @classmethod
    def default(cls) -> Config:
        ## TODO: rename+deprecate
        ## very bad name since this is loading from the **default** config
        ## file path, as set in InstallConfig
        return cls.load(path=_get_root())

    @property
    def kind(self) -> ConfigKind:
        if self.config_file is None:
            return ConfigKind.Virtual
        elif not self.config_file.is_file():
            return ConfigKind.NoFile
        elif self.unset_options():
            return ConfigKind.Partial
        else:
            return ConfigKind.Complete

    def dump(self, with_defaults: bool = True) -> dict:
        result = self.model_dump(mode="json")
        if not with_defaults:
            unset = set(self.unset_options())
            for ckey in iter_keys(self.model_dump()):
                if ckey in unset:
                    pop_and_clear_empty_parents(result, ckey)
        return result

    def unset_options(self) -> list[ConfigKey]:
        result: list[ConfigKey] = []
        raw: dict
        dump = self.model_dump()
        if self.raw is None:
            raw = {}
        else:
            raw = self.raw
        for ckey in iter_keys(dump):
            if not ckey.exists_in(raw):
                result.append(ckey)
        return result

    def update_item(
        self,
        key: str,
        value: Any,
        empty_ok: bool = False,
    ) -> Any:
        if self.raw is None and empty_ok:
            self.raw = {}
        elif self.raw is None:
            raise VirtualConfigError
        doc = self.raw
        obj = self
        ckey = ConfigKey(key)
        *parts, last = ckey
        for part in parts:
            doc.setdefault(part, {})
            doc = doc[part]
            obj = getattr(obj, part)
        old_value = getattr(obj, last)
        setattr(obj, last, value)
        doc[last] = value
        return old_value

    def set_default(self, key: str) -> Any:
        ckey = ConfigKey(key)
        if self.raw is None:
            raise VirtualConfigError()
        elif not ckey.exists_in(self.raw):
            return None
        default_config = Config()
        default_value: Any = ckey.value_of(default_config)
        old_value: Any = ckey.value_of(self)

        *parent_path, last_key = ckey.path
        parent_ckey = ConfigKey(parent_path)
        obj = parent_ckey.value_of(self)
        setattr(obj, last_key, default_value)
        pop_and_clear_empty_parents(self.raw, ckey)
        return old_value

    def generate(self, overwrite: bool = False) -> None:
        match (self.kind, overwrite):
            case (ConfigKind.Virtual, _):
                raise VirtualConfigError
            case (ConfigKind.Partial, overwrite):
                defaults = self.model_dump()
                for ckey in self.unset_options():
                    self.update_item(str(ckey), ckey.value_of(defaults))
            case (ConfigKind.Partial | ConfigKind.Complete, _):
                raise FileExistsError(self.config_file)
            case (ConfigKind.NoFile, _):
                self.raw = self.model_dump(mode="json")
            case _:
                raise ValueError(self.kind)
        self.write()

    def write(self) -> None:
        if self.config_file is None or self.raw is None:
            raise VirtualConfigError
        with self.config_file.open("w") as f:
            tomlkit.dump(self.raw, f)
