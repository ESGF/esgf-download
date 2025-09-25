from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class IndexNode:
    value: str

    def is_bridge(self) -> bool:
        return "esgf-1-5-bridge" in self.value

    @property
    def url(self) -> str:
        parsed = urlparse(self.value)
        result: str
        match (parsed.scheme, parsed.netloc, parsed.path, self.is_bridge()):
            case ("", "", path, True):
                result = "https://" + parsed.path
            case ("", "", path, False):
                result = "https://" + parsed.path + "/esg-search/search"
            case _:
                result = self.value
        if "." not in result:
            raise ValueError(self.value)
        return result
