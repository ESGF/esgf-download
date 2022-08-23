from __future__ import annotations
from typing import Optional, TypeAlias
from collections.abc import AsyncIterator

import math
from pathlib import Path
from tqdm.auto import tqdm
from dataclasses import dataclass

import asyncio
from urllib.parse import urlsplit
from httpx import AsyncClient, RequestError

from esgpull.types import File
from esgpull.context import Context
from esgpull.constants import DOWNLOAD_CHUNK_SIZE


class Download:
    """
    Simple async download class.
    """

    def __init__(
        self,
        file: File = None,
        url: str = None,
    ) -> None:
        if file is not None:
            self.file = file
        elif url is not None:
            ctx = Context()
            # TODO: define map data_node->index_node to find url-file
            # ctx.query.index_node = ...
            ctx.query.title = Path(url).name
            results = ctx.search(file=True)
            found_file = False
            if len(results) == 1:
                found_file = True
                self.file = File.from_dict(results[0])
            elif len(results) > 1:
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

    @property
    def size(self) -> int:
        return self.file.size

    async def aget(self) -> bytes:
        async with AsyncClient(follow_redirects=True) as client:
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
        file: File = None,
        url: str = None,
        chunk_size: int = DOWNLOAD_CHUNK_SIZE,
    ) -> None:
        super().__init__(file, url)
        self.chunk_size = chunk_size

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
        start = chunk_idx * self.chunk_size
        end = min(self.size, (chunk_idx + 1) * self.chunk_size - 1)
        headers = {"Range": f"bytes={start}-{end}"}
        resp = await client.get(self.url, headers=headers)
        resp.raise_for_status()
        return resp.content

    async def aget(self) -> bytes:
        client = AsyncClient(follow_redirects=True, timeout=5.0)
        nb_chunks = math.ceil(self.size / self.chunk_size)
        chunks: list[bytes] = []
        for i in range(nb_chunks):
            chunk = await self.aget_chunk(i, client)
            chunks.append(chunk)
        await client.aclose()
        return b"".join(chunks)


@dataclass(init=False)
class MultiSourceChunkedDownload(Download):
    """
    Concurrent chunked downloader that resolves chunks from multiple URLs
    pointing to the same file. These URLs are fetched during `__init__`.
    """

    def __init__(
        self,
        file: File,
        max_ping=5.0,
        chunk_size: int = DOWNLOAD_CHUNK_SIZE,
    ) -> None:
        self.file = file
        self.max_ping = max_ping
        self.chunk_size = chunk_size
        c = Context(distrib=True, fields="id,size,url")
        c.query.instance_id = file.file_id
        results = c.search(file=True)
        self.urls = [r["url"][0].split("|")[0] for r in results]

    async def try_url(self, url: str, client: AsyncClient) -> Optional[str]:
        result = None
        node = urlsplit(url).netloc
        print(f"trying url on '{node}'")
        try:
            resp = await client.head(url)
            print(f"got response on '{node}'")
            resp.raise_for_status()
            accept_ranges = resp.headers.get("Accept-Ranges")
            content_length = resp.headers.get("Content-Length")
            if accept_ranges == "bytes" and int(content_length) == self.size:
                result = str(resp.url)
            else:
                print(dict(resp.headers))
        except RequestError as e:
            print(type(e))
            print(e.request.headers)
        return result

    async def process_queue(
        self, url: str, queue: asyncio.Queue
    ) -> tuple[list[tuple[int, bytes]], str]:
        node = urlsplit(url).netloc
        print(f"starting process on '{node}'")
        client = AsyncClient(follow_redirects=True, timeout=self.max_ping)
        chunks: list[tuple[int, bytes]] = []
        final_url = await self.try_url(url, client)
        if final_url is None:
            print(f"no url found for '{node}'")
            await client.aclose()
            return chunks, url
        else:
            url = final_url
        while not queue.empty():
            chunk_idx = await queue.get()
            print(f"processing chunk {chunk_idx} on '{node}'")
            start = chunk_idx * self.chunk_size
            end = min(self.size, (chunk_idx + 1) * self.chunk_size - 1)
            headers = {"Range": f"bytes={start}-{end}"}
            resp = await client.get(url, headers=headers)
            queue.task_done()
            if resp.status_code == 206:
                chunks.append((chunk_idx, resp.content))
            else:
                await queue.put(chunk_idx)
                print(f"error status {resp.status_code} on '{node}'")
                break
        await client.aclose()
        return chunks, url

    async def aget(self) -> bytes:
        nb_chunks = math.ceil(self.size / self.chunk_size)
        queue: asyncio.Queue[int] = asyncio.Queue(nb_chunks)
        for chunk_idx in range(nb_chunks):
            queue.put_nowait(chunk_idx)
        completed: list[bool] = [False for _ in range(nb_chunks)]
        chunks: list[bytes] = [bytes() for _ in range(nb_chunks)]
        workers = [self.process_queue(url, queue) for url in self.urls]
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
ResultData: TypeAlias = tuple[File, bytes]


@dataclass
class Processor:
    files: list[File]
    max_concurrent: int = 5

    async def process_one(
        self, process: Process, semaphore: asyncio.Semaphore
    ) -> ResultData:
        async with semaphore:
            data = await process.aget()
        return process.file, data

    async def process(self, use_bar=False) -> AsyncIterator[ResultData]:
        if use_bar:
            total_size = sum(file.size for file in self.files)
            bar = tqdm(
                total=total_size, unit="iB", unit_scale=True, unit_divisor=1024
            )
        semaphore = asyncio.Semaphore(self.max_concurrent)
        processes = [Download(file) for file in self.files]
        tasks = [self.process_one(process, semaphore) for process in processes]
        for future in asyncio.as_completed(tasks):
            file, data = await future
            yield file, data
            if use_bar:
                bar.update(file.size)
        if use_bar:
            bar.close()


__all__ = ["Download", "Processor"]
