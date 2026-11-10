import asyncio
import ssl
from collections.abc import AsyncIterator
from functools import partial
from typing import TypeAlias

from aiostream.stream import merge
from httpx import AsyncClient, HTTPError

from esgpull.config import Config
from esgpull.download import DownloadCtx, Simple
from esgpull.exceptions import (
    DownloadSizeError,
    RangeFallback,
    RangeNoProgressError,
)
from esgpull.fs import Digest, Filesystem
from esgpull.models import File
from esgpull.result import Err, Ok, Result
from esgpull.tui import logger

# Callback: TypeAlias = Callable[[], None] | partial[None]
Callback: TypeAlias = partial[None]

default_ssl_context: ssl.SSLContext | bool = False
default_ssl_context_loaded = False


def load_default_ssl_context() -> str:
    global default_ssl_context
    global default_ssl_context_loaded
    if ssl.OPENSSL_VERSION_INFO[0] >= 3:
        default_ssl_context = ssl.create_default_context()
        default_ssl_context.options |= 0x4
        msg = "Using openssl 3 or higher"
    else:
        default_ssl_context = True
        msg = "Using openssl 1"
    default_ssl_context_loaded = True
    return msg


class Task:
    def __init__(
        self,
        config: Config,
        fs: Filesystem,
        # *,
        # url: str | None = None,
        file: File,
        start_callbacks: list[Callback] | None = None,
    ) -> None:
        self.config = config
        self.fs = fs
        self.ctx = DownloadCtx(file)
        self.ctx.digest = self._make_digest()
        # if file is None and url is not None:
        #     self.file = self.fetch_file(url)
        # elif file is not None:
        #     self.file = file
        # else:
        #     raise ValueError("no arguments")
        self.downloader = Simple()
        if start_callbacks is None:
            self.start_callbacks = []
        else:
            self.start_callbacks = start_callbacks

    @property
    def file(self) -> File:
        return self.ctx.file

    def _make_digest(self) -> Digest | None:
        if self.config.download.disable_checksum:
            return None
        return Digest(self.file)

    def _resume_offset(self) -> int:
        """
        Bytes already on disk in the `.part` file, safe to resume from.

        Re-stat'ed at attempt time (never trusted from memory) so lost
        buffered writes can never be double-counted. A `.part` that is
        empty or >= the expected file size is treated as unusable.
        """
        tmp = self.fs[self.file].tmp
        if not tmp.is_file():
            return 0
        size = tmp.stat().st_size
        if 0 < size < self.file.size:
            return size
        return 0

    # def fetch_file(self, url: str) -> File:
    #     ctx = Context()
    #     # [?]TODO: define map data_node->index_node to find url-file
    #     # ctx.query.index_node = ...
    #     ctx.query.title = Path(url).name
    #     results = ctx.search(file=True)
    #     for res in results:
    #         file = File.from_dict(res)
    #         if file.version in url:
    #             return file
    #     raise ValueError(f"{url} is not valid")

    async def stream(
        self,
        semaphore: asyncio.Semaphore,
        client: AsyncClient,
    ) -> AsyncIterator[Result[DownloadCtx]]:
        ctx = self.ctx
        try:
            async with semaphore:
                allow_range = True
                callbacks_done = False
                # At most two attempts: the fallback pass sends no
                # Range header, so `RangeFallback` cannot re-raise.
                while True:
                    # Only resume from `.part` while ranges are allowed:
                    # on the fallback pass, stale bytes must be truncated
                    # by the fresh plain GET (opened in "wb" mode).
                    offset = self._resume_offset() if allow_range else 0
                    resume = offset > 0
                    ctx.completed = offset
                    ctx.chunk = None
                    ctx.digest = self._make_digest()
                    try:
                        async with self.fs.open(
                            ctx.file, resume=resume
                        ) as file_obj:
                            if resume and ctx.digest is not None:
                                await file_obj.seed_digest(
                                    ctx.digest,
                                    self.config.download.chunk_size,
                                )
                            if not callbacks_done:
                                for callback in self.start_callbacks:
                                    callback()
                                callbacks_done = True
                            stream = self.downloader.stream(
                                client,
                                ctx,
                                self.config.download.chunk_size,
                                range_size=(
                                    self.config.download.range_size
                                    if allow_range
                                    else 0
                                ),
                            )
                            async for ctx in stream:
                                if ctx.chunk is not None:
                                    await file_obj.write(ctx.chunk)
                                    ctx.chunk = None
                                if ctx.error:
                                    err = DownloadSizeError(
                                        ctx.completed, ctx.file.size
                                    )
                                    yield Err(ctx, err)
                                    await stream.aclose()
                                    break
                                elif ctx.finished:
                                    await file_obj.to_done()
                                yield Ok(ctx)
                            break  # stream finished cleanly
                    except RangeFallback:
                        allow_range = False
                        continue
        except (
            HTTPError,
            DownloadSizeError,
            RangeNoProgressError,
            GeneratorExit,
            ssl.SSLError,
            FileNotFoundError,
            # KeyboardInterrupt,
        ) as err:
            yield Err(ctx, err)


class Processor:
    def __init__(
        self,
        config: Config,
        fs: Filesystem,
        files: list[File],
        start_callbacks: dict[str, list[Callback]],
    ) -> None:
        self.config = config
        self.fs = fs
        self.files = list(filter(self.should_download, files))
        self.tasks: list[Task] = []
        msg: str | None = None
        if not default_ssl_context_loaded:
            msg = load_default_ssl_context()
        self.ssl_context: ssl.SSLContext | bool
        if self.config.download.disable_ssl:
            self.ssl_context = False
        else:
            if msg is not None:
                logger.info(msg)
            self.ssl_context = default_ssl_context
        for file in files:
            task = Task(
                config=config,
                fs=fs,
                file=file,
                start_callbacks=start_callbacks[file.sha],
            )
            self.tasks.append(task)

    def should_download(self, file: File) -> bool:
        if self.fs[file].drs.is_file():
            return False
        else:
            return True

    async def process(self) -> AsyncIterator[Result[DownloadCtx]]:
        semaphore = asyncio.Semaphore(self.config.download.max_concurrent)
        async with AsyncClient(
            follow_redirects=True,
            verify=self.ssl_context,
            timeout=self.config.download.http_timeout,
        ) as client:
            streams = [task.stream(semaphore, client) for task in self.tasks]
            async with merge(*streams).stream() as stream:
                async for result in stream:
                    yield result
