from __future__ import annotations
from typing import TypeAlias
from collections.abc import AsyncIterator, AsyncGenerator

from math import ceil
from pathlib import Path
from tqdm.auto import tqdm
from contextlib import asynccontextmanager

import asyncio
from urllib.parse import urlsplit
from httpx import AsyncClient, HTTPError

from esgpull.auth import Auth
from esgpull.types import File, DownloadMethod
from esgpull.context import Context
from esgpull.settings import Settings
from esgpull.result import Result, Ok, Err


class Download:
    """
    Simple async download class.
    """

    def __init__(
        self,
        auth: Auth,
        *,
        file: File | None = None,
        url: str | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.auth = auth
        if settings is None:
            self.settings = Settings()
        else:
            self.settings = settings
        if file is not None:
            self.file = file
        elif url is not None:
            ctx = Context()
            # TODO: define map data_node->index_node to find url-file
            # ctx.query.index_node = ...
            ctx.query.title = Path(url).name
            results = ctx.search(file=True)
            found_file = False
            for res in results:
                file = File.from_dict(res)
                if file.version in url:
                    self.file = file
                    found_file = True
                    break
            if not found_file:
                raise ValueError(f"{url} is not valid")
        else:
            raise ValueError("no arguments")

    @property
    def url(self) -> str:
        return self.file.url

    @asynccontextmanager
    async def make_client(self) -> AsyncGenerator:
        try:
            client = AsyncClient(
                follow_redirects=True,
                cert=self.auth.cert,
                timeout=self.settings.download.http_timeout,
            )
            yield client
        finally:
            await client.aclose()

    async def aget(self) -> bytes:
        async with self.make_client() as client:
            resp = await client.get(self.url)
            resp.raise_for_status()
        return resp.content


class ChunkedDownload(Download):
    """
    Sequential async chunked download class.

    Can be extended to download chunks concurrently (Semaphore for caution).
    """

    def __init__(
        self,
        auth: Auth,
        *,
        file: File | None = None,
        url: str | None = None,
        settings: Settings | None = None,
    ) -> None:
        super().__init__(auth, file=file, url=url, settings=settings)

    # TODO: decide whether:
    # 1. await-get directly as classmethod (e.g. `file`)
    # 2. async-with -> aget (use `contextlib.asynccontextmanager`)
    # 3. custom-init -> aget (e.g. `from_url`)

    # @classmethod
    # async def from_url(cls, url: str, size: int = None) -> "ChunkedDownload":
    #     if size is None:
    #         resp = await client.head(url)
    #         size = int(resp.headers["Content-Length"])
    #     download = cls(url, size, client)
    #     return download

    async def aget_chunk(self, chunk_idx: int, client: AsyncClient) -> bytes:
        start = chunk_idx * self.settings.download.chunk_size
        end = min(
            self.file.size,
            (chunk_idx + 1) * self.settings.download.chunk_size - 1,
        )
        headers = {"Range": f"bytes={start}-{end}"}
        resp = await client.get(self.url, headers=headers)
        resp.raise_for_status()
        return resp.content

    async def aget(self) -> bytes:
        nb_chunks = ceil(self.file.size / self.settings.download.chunk_size)
        chunks: list[bytes] = []
        async with self.make_client() as client:
            for i in range(nb_chunks):
                chunk = await self.aget_chunk(i, client)
                chunks.append(chunk)
        return b"".join(chunks)


class MultiSourceChunkedDownload(Download):
    """
    Concurrent chunked downloader that resolves chunks from multiple URLs
    pointing to the same file. These URLs are fetched during `__init__`.
    """

    def __init__(
        self,
        auth: Auth,
        *,
        file: File | None = None,
        url: str | None = None,
        settings: Settings | None = None,
        max_ping: float = 5.0,
    ) -> None:
        super().__init__(auth, file=file, url=url, settings=settings)
        self.max_ping = max_ping

    async def try_url(self, url: str, client: AsyncClient) -> str | None:
        result = None
        node = urlsplit(url).netloc
        print(f"trying url on '{node}'")
        try:
            resp = await client.head(url)
            print(f"got response on '{node}'")
            resp.raise_for_status()
            accept_ranges = resp.headers.get("Accept-Ranges")
            content_length = resp.headers.get("Content-Length")
            if (
                accept_ranges == "bytes"
                and int(content_length) == self.file.size
            ):
                result = str(resp.url)
            else:
                print(dict(resp.headers))
        except HTTPError as err:
            print(type(err))
            print(err.request.headers)
        return result

    async def process_queue(
        self, url: str, queue: asyncio.Queue
    ) -> tuple[list[tuple[int, bytes]], str]:
        node = urlsplit(url).netloc
        print(f"starting process on '{node}'")
        chunks: list[tuple[int, bytes]] = []
        async with self.make_client() as client:
            final_url = await self.try_url(url, client)
            if final_url is None:
                print(f"no url found for '{node}'")
                return chunks, url
            else:
                url = final_url
            while not queue.empty():
                chunk_idx = await queue.get()
                print(f"processing chunk {chunk_idx} on '{node}'")
                start = chunk_idx * self.settings.download.chunk_size
                end = min(
                    self.file.size,
                    (chunk_idx + 1) * self.settings.download.chunk_size - 1,
                )
                headers = {"Range": f"bytes={start}-{end}"}
                resp = await client.get(url, headers=headers)
                queue.task_done()
                if resp.status_code == 206:
                    chunks.append((chunk_idx, resp.content))
                else:
                    await queue.put(chunk_idx)
                    print(f"error status {resp.status_code} on '{node}'")
                    break
        return chunks, url

    async def fetch_urls(self) -> list[str]:
        ctx = Context(distrib=True)
        ctx.query.instance_id = self.file.file_id
        results = await ctx._search(file=True)
        files = [File.from_dict(item) for item in results]
        return [file.url for file in files]

    async def aget(self) -> bytes:
        nb_chunks = ceil(self.file.size / self.settings.download.chunk_size)
        queue: asyncio.Queue[int] = asyncio.Queue(nb_chunks)
        for chunk_idx in range(nb_chunks):
            queue.put_nowait(chunk_idx)
        completed: list[bool] = [False for _ in range(nb_chunks)]
        chunks: list[bytes] = [bytes() for _ in range(nb_chunks)]
        urls = await self.fetch_urls()
        workers = [self.process_queue(url, queue) for url in urls]
        for future in asyncio.as_completed(workers):
            some_chunks, url = await future
            print(f"got {len(some_chunks)} chunks from {url}")
            for chunk_idx, chunk in some_chunks:
                completed[chunk_idx] = True
                chunks[chunk_idx] = chunk
        if not all(completed):
            raise ValueError("TODO: progressive write (with .part file)")
        return b"".join(chunks)


Process: TypeAlias = Download | ChunkedDownload | MultiSourceChunkedDownload


class Processor:
    METHODS = {
        DownloadMethod.Download: Download,
        DownloadMethod.ChunkedDownload: ChunkedDownload,
        DownloadMethod.MultiSourceChunkedDownload: MultiSourceChunkedDownload,
    }

    def __init__(
        self,
        auth: Auth,
        files: list[File],
        max_concurrent: int = 5,
        settings: Settings | None = None,
    ) -> None:
        self.auth = auth
        self.files = files
        self.max_concurrent = max_concurrent
        if settings is None:
            self.settings = Settings()
        else:
            self.settings = settings
        self.method = self.METHODS[self.settings.download.method]

    async def process_one(
        self, process: Process, semaphore: asyncio.Semaphore
    ) -> Result:
        result: Result
        async with semaphore:
            try:
                result = Ok(process.file)
                result.data = await process.aget()
            except HTTPError as err:
                result = Err(process.file)
                result.err = err
        return result

    async def process(self, use_bar=False) -> AsyncIterator[Result]:
        if use_bar:
            total_size = sum(file.size for file in self.files)
            bar = tqdm(
                total=total_size, unit="iB", unit_scale=True, unit_divisor=1024
            )
        semaphore = asyncio.Semaphore(self.max_concurrent)
        processes = [
            self.method(self.auth, file=file, settings=self.settings)
            for file in self.files
        ]
        tasks = [self.process_one(process, semaphore) for process in processes]
        for future in asyncio.as_completed(tasks):
            result = await future
            yield result
            if use_bar:
                bar.update(result.file.size)
        if use_bar:
            bar.close()


__all__ = ["Download", "Processor"]
