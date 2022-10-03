from __future__ import annotations
from typing import Any

import yaml
from pathlib import Path
from pydantic import BaseModel, BaseSettings, NoneStr, SecretStr


# TODO: find a better way to dynamically set path through __init__
class BasePath:
    path: str | Path | None = None

    @classmethod
    def yaml_config_source(cls, config: BaseSettings) -> dict[str, Any]:
        result: dict[str, Any]
        encoding = config.__config__.env_file_encoding
        if cls.path is None:
            result = {}
        else:
            if isinstance(cls.path, str):
                cls.path = Path(cls.path)
            if not cls.path.exists():
                cls.path.touch()
                result = {}
            else:
                result = yaml.safe_load(cls.path.read_text(encoding))
                if result is None:
                    result = {}
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
    method: str = "Download"


class Settings(BaseSettings):
    context: Context = Context()
    download: Download = Download()

    class Config:
        env_file_encoding = "utf-8"

        @classmethod
        def customise_sources(
            cls, init_settings, env_settings, file_secret_settings
        ):
            return (
                init_settings,
                SettingsPath.yaml_config_source,
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
                CredentialsPath.yaml_config_source,
            )


__all__ = ["Settings"]
