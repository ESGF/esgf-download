from typing import NewType

import aiohttp

# import asyncio
from pathlib import Path

# from urllib.parse import quote
from dataclasses import dataclass, field


UrlParams = NewType("UrlParams", dict)


@dataclass
class Url:
    server: Path
    route: Path = field(default=Path(""))

    def __post_init__(self):
        if isinstance(self.server, str):
            self.server = Path(self.server)
        if isinstance(self.route, str):
            self.route = Path(self.route)

    def __str__(self):
        return self.server / self.route


# TODO: re-implement (async) pyesgf ? or pull request to its repo ?
@dataclass
class EsgfClient:
    url: Url
    params: UrlParams

    @property
    async def session(self):
        return await aiohttp.ClientSession()

    async def fetch(self):
        async with self.session.get(self.url, params=self.params) as response:
            return await response.content
