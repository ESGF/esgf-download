from dataclasses import dataclass
from typing import TypeAlias
from urllib.parse import urlparse

from esgpull.models import ApiBackend

HintsDict: TypeAlias = dict[str, dict[str, int]]


@dataclass
class IndexNode:
    value: str
    backend: ApiBackend

    def is_bridge(self) -> bool:
        match self.backend:
            case ApiBackend.solr:
                return "esgf-1-5-bridge" in self.value
            case ApiBackend.stac:
                return False

    @property
    def url(self) -> str:
        parsed = urlparse(self.value)
        result: str
        match (
            self.backend,
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            self.is_bridge(),
        ):
            case (ApiBackend.solr, "", "", path, False):
                result = f"https://{path}/esg-search/search"
            case (_, "", "", path, _):
                result = f"https://{path}"
            case _:
                result = self.value
        if "." not in result:
            raise ValueError(self.value)
        return result
