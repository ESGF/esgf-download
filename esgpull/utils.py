import asyncio
import datetime
from typing import Callable, Coroutine, TypeVar
from urllib.parse import urlparse

from rich.filesize import _to_str

T = TypeVar("T")


def sync(
    coro: Coroutine[None, None, T],
    before_cb: Callable | None = None,
    after_cb: Callable | None = None,
) -> T:
    if before_cb is not None:
        before_cb()
    result = asyncio.run(coro)
    if after_cb is not None:
        after_cb()
    return result


def format_size(size: int) -> str:
    return _to_str(
        size,
        ("kiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"),
        1024,
        precision=1,
        separator=" ",
    )


def parse_date(
    date: str | datetime.datetime, fmt: str = "%Y-%m-%d"
) -> datetime.datetime:
    match date:
        case datetime.datetime():
            ...
        case str():
            date = datetime.datetime.strptime(date, fmt)
        case _:
            raise ValueError(date)
    return date


def format_date(date: str | datetime.datetime, fmt: str = "%Y-%m-%d") -> str:
    return parse_date(date, fmt).strftime(fmt)


def format_date_iso(
    date: str | datetime.datetime, fmt: str = "%Y-%m-%d"
) -> str:
    return parse_date(date, fmt).replace(microsecond=0).isoformat() + "Z"


def url2index(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc == "":
        return parsed.path
    else:
        return parsed.netloc


def index2url(index: str) -> str:
    return "https://" + url2index(index) + "/esg-search/search"
