import datetime
import os
from pathlib import Path
from urllib.parse import urlparse

import rich
from rich.filesize import _to_str

from esgpull.constants import ENV_VARNAME


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
    def get(cls, mkdir=False) -> Path:
        if cls.root is None:
            root_env = os.environ.get(ENV_VARNAME)
            if root_env is None:
                cls.root = Path.home() / ".esgpull"
                rich.print(
                    f":warning-emoji: Using default root directory: {cls.root}"
                )
                rich.print(
                    f"Set [yellow]{ENV_VARNAME}[/] to the desired root directory to disable this warning."
                )
            else:
                cls.root = Path(root_env)
        if mkdir:
            cls.root.mkdir(parents=True, exist_ok=True)
        elif not cls.root.is_dir():
            raise NotADirectoryError(cls.root)
        return cls.root
