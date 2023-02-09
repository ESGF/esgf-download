import asyncio
import ssl
from functools import partial
from typing import AsyncIterator, TypeAlias

from aiostream.stream import merge
from httpx import AsyncClient, HTTPError

from esgpull.auth import Auth
from esgpull.config import Config
from esgpull.download import DownloadCtx, Simple
from esgpull.exceptions import DownloadSizeError
from esgpull.fs import Filesystem
from esgpull.models import File
from esgpull.result import Err, Ok, Result

# Callback: TypeAlias = Callable[[], None] | partial[None]
Callback: TypeAlias = partial[None]

ssl_context: ssl.SSLContext | bool
if ssl.OPENSSL_VERSION_INFO[0] >= 3:
    ssl_context = ssl.create_default_context()
    ssl_context.options |= 0x4
else:
    ssl_context = True


class Task:
    def __init__(
        self,
        config: Config,
        auth: Auth,
        fs: Filesystem,
        # *,
        # url: str | None = None,
        file: File,
        start_callbacks: list[Callback] | None = None,
    ) -> None:
        self.config = config
        self.auth = auth
        self.fs = fs
        self.ctx = DownloadCtx(file)
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
        self, semaphore: asyncio.Semaphore
    ) -> AsyncIterator[Result]:
        try:
            async with (
                semaphore,
                self.fs.open(self.ctx.file) as file_obj,
                AsyncClient(
                    follow_redirects=True,
                    cert=self.auth.cert,
                    verify=ssl_context,
                    timeout=self.config.download.http_timeout,
                ) as client,
            ):
                for callback in self.start_callbacks:
                    callback()
                async for ctx in self.downloader.stream(client, self.ctx):
                    if ctx.chunk is not None:
                        await file_obj.write(ctx.chunk)
                    if ctx.error:
                        raise DownloadSizeError(ctx.completed, ctx.file.size)
                    elif ctx.finished:
                        file_obj.finished = True
                    yield Ok(ctx)
        except (
            HTTPError,
            DownloadSizeError,
            GeneratorExit,
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
        self.files = files
        self.tasks = []
        for file in files:
            task = Task(
                config=config,
                auth=auth,
                fs=fs,
                file=file,
                start_callbacks=start_callbacks[file.sha],
            )
            self.tasks.append(task)

    async def process(self) -> AsyncIterator[Result]:
        semaphore = asyncio.Semaphore(self.config.download.max_concurrent)
        streams = [task.stream(semaphore) for task in self.tasks]
        async with merge(*streams).stream() as stream:
            async for result in stream:
                yield result
