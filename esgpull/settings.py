from __future__ import annotations
from typing import Any

import tomlkit
from pathlib import Path
from pydantic import BaseModel, BaseSettings, NoneStr, SecretStr

from esgpull.types import DownloadKind


class BasePath:
    path: str | Path | None = None

    @classmethod
    def toml_config_source(cls, config: BaseSettings) -> dict[str, Any]:
        result: dict[str, Any]
        if cls.path is None:
            result = {}
        else:
            if isinstance(cls.path, (str,)):
                cls.path = Path(cls.path)
            if not cls.path.exists():
                cls.path.touch()  # [?]TODO: maybe do not do this implicitly
                result = {}
            else:
                with cls.path.open() as fh:
                    result = tomlkit.load(fh)
        return result


class SettingsPath(BasePath):
    ...


class CredentialsPath(BasePath):
    ...


class Context(BaseModel):
    index_node: str = "esgf-node.ipsl.upmc.fr"
    http_timeout: int = 20


class Download(BaseModel):
    chunk_size: int = 1 << 26
    kind: DownloadKind = DownloadKind.Simple
    http_timeout: int = 20
    max_concurrent: int = 5


class Settings(BaseSettings):
    context: Context = Context()
    download: Download = Download()

    class Config:
        @classmethod
        def customise_sources(
            cls, init_settings, env_settings, file_secret_settings
        ):
            return (
                init_settings,
                SettingsPath.toml_config_source,
            )


class Credentials(BaseSettings):
    provider: NoneStr = None
    user: NoneStr = None
    password: SecretStr | None = None

    class Config:
        env_file_encoding = "utf-8"

        @classmethod
        def customise_sources(
            cls, init_settings, env_settings, file_secret_settings
        ):
            return (
                init_settings,
                CredentialsPath.toml_config_source,
            )


__all__ = ["Settings"]
