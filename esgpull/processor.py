import asyncio
import ssl
from collections.abc import AsyncIterator
from functools import partial
from typing import TypeAlias

from aiostream.stream import merge
from httpx import AsyncClient, HTTPError

from esgpull.auth import Auth
from esgpull.config import Config
from esgpull.download import DownloadCtx, Simple
from esgpull.exceptions import DownloadSizeError
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
        if not self.config.download.disable_checksum:
            self.ctx.digest = Digest(file)
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
    ) -> AsyncIterator[Result]:
        ctx = self.ctx
        try:
            async with semaphore, self.fs.open(ctx.file) as file_obj:
                for callback in self.start_callbacks:
                    callback()
                stream = self.downloader.stream(
                    client,
                    ctx,
                    self.config.download.chunk_size,
                )
                async for ctx in stream:
                    if ctx.chunk is not None:
                        await file_obj.write(ctx.chunk)
                        ctx.chunk = None
                    if ctx.error:
                        err = DownloadSizeError(ctx.completed, ctx.file.size)
                        yield Err(ctx, err)
                        await stream.aclose()
                        break
                    elif ctx.finished:
                        await file_obj.to_done()
                    yield Ok(ctx)
        except (
            HTTPError,
            DownloadSizeError,
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
        auth: Auth,
        fs: Filesystem,
        files: list[File],
        start_callbacks: dict[str, list[Callback]],
    ) -> None:
        self.config = config
        self.auth = auth
        self.fs = fs
        self.files = list(filter(self.should_download, files))
        self.tasks = []
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

    async def process(self) -> AsyncIterator[Result]:
        semaphore = asyncio.Semaphore(self.config.download.max_concurrent)
        async with AsyncClient(
            follow_redirects=True,
            cert=self.auth.cert,
            verify=self.ssl_context,
            timeout=self.config.download.http_timeout,
        ) as client:
            streams = [task.stream(semaphore, client) for task in self.tasks]
            async with merge(*streams).stream() as stream:
                async for result in stream:
                    yield result
