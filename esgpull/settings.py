from __future__ import annotations
from typing import Any, Optional

import yaml
from pathlib import Path
from pydantic import BaseModel, BaseSettings


# TODO: find a better way to dynamically set path through __init__
class SettingsPath:
    path: Optional[Path] = None


def yaml_config_source(config: BaseSettings) -> dict[str, Any]:
    result: dict[str, Any]
    encoding = config.__config__.env_file_encoding
    path = SettingsPath.path
    if path is None:
        result = {}
    elif not path.exists():
        path.touch()
        result = {}
    else:
        result = yaml.safe_load(path.read_text(encoding))
        if result is None:
            result = {}
    return result


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
                yaml_config_source,
            )


__all__ = ["Settings"]
