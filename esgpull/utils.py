import humanize
import datetime
from urllib.parse import urlparse


def naturalsize(value: int | float) -> str:
    """Get size in KiB / MiB / GiB / etc."""
    return humanize.naturalsize(value, "unix")


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


__all__ = ["naturalsize", "format_date", "url2index", "index2url"]
