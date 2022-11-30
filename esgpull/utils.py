import asyncio
import datetime
import logging
import os
from pathlib import Path
from typing import Callable, Coroutine, TypeVar
from urllib.parse import urlparse

from rich.filesize import _to_str

from esgpull.constants import CONFIG_FILENAME, ENV_VARNAME

T = TypeVar("T")


def sync(
    self,
    coro: Coroutine[None, None, T],
    prerun_cb: Callable | None = None,
    postrun_cb: Callable | None = None,
) -> T:
    if prerun_cb is not None:
        prerun_cb()
    result: T = asyncio.run(coro)
    if postrun_cb is not None:
        postrun_cb()
    return result


def format_size(size: int) -> str:
    return _to_str(
        size,
        ("kiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"),
        1024,
        precision=1,
        separator=" ",
    )


def format_date(date: str | datetime.datetime, fmt: str = "%Y-%m-%d") -> str:
    match date:
        case datetime.datetime():
            ...
        case str():
            date = datetime.datetime.strptime(date, fmt)
        case _:
            raise ValueError(date)
    return date.replace(microsecond=0).isoformat() + "Z"


def url2index(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc == "":
        return parsed.path
    else:
        return parsed.netloc


def index2url(index: str) -> str:
    return "https://" + url2index(index) + "/esg-search/search"


def find_str(container: list | str) -> str:
    if isinstance(container, list):
        return find_str(container[0])
    elif isinstance(container, str):
        return container
    else:
        raise ValueError(container)


def find_int(container: list | int) -> int:
    if isinstance(container, list):
        return find_int(container[0])
    elif isinstance(container, int):
        return container
    else:
        raise ValueError(container)


class Root:
    root: Path | None = None

    @classmethod
    def get(cls, mkdir: bool = False, noraise: bool = False) -> Path:
        if cls.root is None:
            root_env = os.environ.get(ENV_VARNAME)
            if root_env is None:
                cls.root = Path.home() / ".esgpull"
                msg = f"Using root directory: {cls.root}\n"
                msg += f"Set {ENV_VARNAME} to the desired root directory to disable this warning."
                logger = logging.getLogger("esgpull")
                logger.warning(msg)
            else:
                cls.root = Path(root_env)
        if mkdir:
            cls.root.mkdir(parents=True, exist_ok=True)
            config_file = cls.root / CONFIG_FILENAME
            if not config_file.is_file():
                config_file.touch()
        elif not cls.root.is_dir() and not noraise:
            raise NotADirectoryError(cls.root)
        return cls.root
