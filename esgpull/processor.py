import asyncio
from functools import partial
from pathlib import Path
from typing import AsyncIterator, TypeAlias

from aiostream.stream import merge
from httpx import AsyncClient, HTTPError

from esgpull.auth import Auth
from esgpull.context import Context
from esgpull.db.models import File
from esgpull.download import Downloaders
from esgpull.exceptions import DownloadSizeError
from esgpull.fs import Filesystem
from esgpull.result import Err, Ok, Result
from esgpull.settings import Settings

# Callback: TypeAlias = Callable[[], None] | partial[None]
Callback: TypeAlias = partial[None]


class Task:
    def __init__(
        self,
        auth: Auth,
        fs: Filesystem,
        settings: Settings,
        *,
        url: str | None = None,
        file: File | None = None,
        start_callbacks: list[Callback] | None = None,
    ) -> None:
        self.auth = auth
        self.fs = fs
        self.settings = settings
        if file is None and url is not None:
            self.file = self.fetch_file(url)
        elif file is not None:
            self.file = file
        else:
            raise ValueError("no arguments")
        self.downloader = Downloaders[settings.download.kind]()
        if start_callbacks is None:
            self.start_callbacks = []
        else:
            self.start_callbacks = start_callbacks

    def fetch_file(self, url: str) -> File:
        ctx = Context()
        # [?]TODO: define map data_node->index_node to find url-file
        # ctx.query.index_node = ...
        ctx.query.title = Path(url).name
        results = ctx.search(file=True)
        for res in results:
            file = File.from_dict(res)
            if file.version in url:
                return file
        raise ValueError(f"{url} is not valid")

    async def stream(
        self, semaphore: asyncio.Semaphore
    ) -> AsyncIterator[Result]:
        completed = 0
        try:
            async with (
                semaphore,
                self.fs.open(self.file) as file_obj,
                AsyncClient(
                    follow_redirects=True,
                    cert=self.auth.cert,
                    timeout=self.settings.download.http_timeout,
                ) as client,
            ):
                for callback in self.start_callbacks:
                    callback()
                async for chunk in self.downloader.stream(client, self.file):
                    await file_obj.write(chunk)
                    completed += len(chunk)
                    if completed > self.file.size:
                        raise DownloadSizeError(completed, self.file.size)
                    yield Ok(self.file, completed)
        except (
            HTTPError,
            DownloadSizeError,
            GeneratorExit,
            # KeyboardInterrupt,
        ) as err:
            yield Err(self.file, completed, err)


class Processor:
    def __init__(
        self,
        auth: Auth,
        fs: Filesystem,
        files: list[File],
        settings: Settings,
        start_callbacks: dict[int, list[Callback]],
    ) -> None:
        self.files = files
        self.settings = settings
        self.remaining_ids = set([file.id for file in self.files])
        self.tasks = [
            Task(
                auth=auth,
                fs=fs,
                settings=settings,
                file=file,
                start_callbacks=start_callbacks[file.id],
            )
            for file in files
        ]

    async def process(self) -> AsyncIterator[Result]:
        semaphore = asyncio.Semaphore(self.settings.download.max_concurrent)
        streams = [task.stream(semaphore) for task in self.tasks]
        async with merge(*streams).stream() as stream:
            async for result in stream:
                yield result
                if result.completed == result.file.size:
                    self.remaining_ids.remove(result.file.id)


__all__ = ["Processor"]
