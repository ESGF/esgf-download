from pathlib import Path
from typing import Iterator

from pydantic import BaseModel, Field, field_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

from esgpull.install_config import InstallConfig
from esgpull.models.options import Option, Options


def _get_root() -> Path:
    if InstallConfig.current is not None:
        return InstallConfig.current.path
    else:
        return InstallConfig.default


class Paths(BaseModel, validate_assignment=True, validate_default=True):
    auth: Path = Path("auth")
    data: Path = Path("data")
    db: Path = Path("db")
    log: Path = Path("log")
    tmp: Path = Path("tmp")
    plugins: Path = Path("plugins")

    @field_validator(
        "auth",
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
        yield self.auth
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


class Config(BaseSettings):
    model_config = SettingsConfigDict(toml_file=_get_root() / "config.toml")

    paths: Paths = Field(default_factory=Paths)
    credentials: Credentials = Field(default_factory=Credentials)
    cli: Cli = Field(default_factory=Cli)
    db: Db = Field(default_factory=Db)
    download: Download = Field(default_factory=Download)
    api: API = Field(default_factory=API)
    plugins: Plugins = Field(default_factory=Plugins)
    # _raw: TOMLDocument | None = Field(init=False, default=None)
    # _config_file: Path | None = Field(init=False, default=None)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (TomlConfigSettingsSource(settings_cls),)
