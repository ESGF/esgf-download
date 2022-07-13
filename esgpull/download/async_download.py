# from typing import TypeAlias

import asyncio
import httpx

from esgpull.context import Context


# UrlParams = NewType("UrlParams", dict)


# @dataclass
# class Url:
#     server: Path
#     route: Path = field(default=Path(""))

#     def __post_init__(self):
#         if isinstance(self.server, str):
#             self.server = Path(self.server)
#         if isinstance(self.route, str):
#             self.route = Path(self.route)

#     def __str__(self):
#         return self.server / self.route


# TODO: re-implement (async) pyesgf ? or pull request to its repo ?
class AsyncDownloader:
    def __init__(self, ctx: Context):
        self.ctx = ctx

    async def _download(
        self, client: httpx.AsyncClient, sem: asyncio.Semaphore
    ):
        ...

    async def _fetch(self):
        async with self.session.get(self.url, params=self.params) as response:
            return await response.content
