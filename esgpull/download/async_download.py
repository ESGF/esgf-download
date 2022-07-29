import asyncio
import httpx

from esgpull.context import Context


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
