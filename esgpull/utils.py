import datetime
from urllib.parse import urlparse


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


__all__ = ["format_date", "url2index", "index2url", "find_str", "find_int"]
