from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import AsyncIterator

from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from esgpull.auth import Auth, Credentials
from esgpull.config import Config, InstallConfig
from esgpull.context import Context
from esgpull.database import Database
from esgpull.exceptions import (
    DownloadCancelled,
    InvalidInstallPath,
    NoInstallPath,
)
from esgpull.fs import Filesystem
from esgpull.graph import Graph
from esgpull.models import Facet, File, FileStatus, Options, Query, sql
from esgpull.models.utils import short_sha
from esgpull.processor import Processor
from esgpull.result import Err, Ok, Result
from esgpull.tui import UI, Verbosity, logger
from esgpull.utils import format_size


@dataclass(repr=False)
class Esgpull:
    path: Path
    config: Config
    ui: UI
    auth: Auth
    db: Database
    context: Context
    fs: Filesystem
    graph: Graph

    def __init__(
        self,
        path: Path | str | None = None,
        verbosity: Verbosity = Verbosity.Detail,
        install: bool = False,
        record: bool = False,
        safe: bool = False,
    ) -> None:
        if path is not None:
            path = Path(path)
            InstallConfig.choose(path=path)
            default = path
            warning = f"Using unknown location: {path}\n"
        else:
            default = InstallConfig.default
            warning = f"Using default location: {default}\n"
        if InstallConfig.current is None:
            if safe:
                raise NoInstallPath
            idx = InstallConfig.add(default)
            InstallConfig.current_idx = idx
            self.path = InstallConfig.installs[idx].path
            warning += "To disable this warning, please run:\n"
            warning += f"$ esgpull self install {self.path}"
            logger.warning(warning)
        else:
            self.path = InstallConfig.current.path
        if not install and not self.path.is_dir():
            raise InvalidInstallPath(path=self.path)
        self.config = Config.load(path=self.path)
        self.fs = Filesystem.from_config(self.config, install=install)
        self.ui = UI.from_config(
            self.config,
            verbosity=verbosity,
            record=record,
        )
        credentials = Credentials()  # TODO: load file
        self.auth = Auth.from_config(self.config, credentials)
        self.db = Database.from_config(self.config)
        self.context = Context(self.config, noraise=True)
        self.graph = Graph(self.db)

    def fetch_index_nodes(self) -> list[str]:
        """
        Returns a list of ESGF index nodes.

        Fetch hints from ESGF search API with a distributed query.
        """

        default_index = self.config.search.index_node
        logger.info(f"Fetching index nodes from '{default_index}'")
        options = Options(distrib=True)
        query = Query(options=options)
        facets = ["index_node"]
        hints = self.context.hints(
            query,
            file=False,
            facets=facets,
            index_node=default_index,
        )
        return list(hints[0]["index_node"])

    def fetch_facets(self, update: bool = False) -> bool:
        """
        Fill db with all existing facets found in ESGF index nodes.

        1. Fetch index nodes from `Esgpull.fetch_index_nodes()`
        2. Fetch all facets (names + values) from all index nodes.

        Workaround method, since searching directly for all facets using
        `distrib=True` seems to crash the index node.
        """

        # those facets have (almost) unique values
        IGNORE_NAMES = [
            "version",
            # "cf_standard_name",
            # "variable_long_name",
            "creation_date",
            # "datetime_end",
        ]
        nb_facets = self.db.scalars(sql.count_table(Facet))[0]
        logger.info(f"Found {nb_facets} facets in database")
        if nb_facets and not update:
            return False
        index_nodes = self.fetch_index_nodes()
        options = Options(distrib=False)
        query = Query(options=options)
        hints_coros = []
        for index_node in index_nodes:
            hints_results = self.context.prepare_hints(
                query,
                file=False,
                facets=["*"],
                index_node=index_node,
            )
            hints_coros.append(self.context._hints(*hints_results))
        hints = self.context.sync_gather(*hints_coros)
        new_facets: set[Facet] = set()
        facets_db = self.db.scalars(sql.facet.all())
        for index_hints in hints:
            for name, values in index_hints[0].items():
                if name in IGNORE_NAMES:
                    continue
                for value in values.keys():
                    facet = Facet(name=name, value=value)
                    if facet not in facets_db:
                        facet.compute_sha()
                        new_facets.add(facet)
        self.db.add(*new_facets)
        return len(new_facets) > 0

    # def add(
    #     self,
    #     *queries: Query,
    #     with_file: bool = False,
    # ) -> tuple[list[Query], list[Query]]:
    #     """
    #     Add new queries to query/options/selection tables.
    #     Returns two lists: added and discarded queries
    #     """
    #     for query in
    #     self.graph.add()
    #     return [], []

    # def install(
    #     self,
    #     *files: File,
    #     status: FileStatus = FileStatus.Queued,
    # ) -> tuple[list[File], list[File]]:
    #     """
    #     Insert `files` with specified `status` into db if not already there.
    #     """
    #     file_ids = [f.file_id for f in files]
    #     with self.db.select(File.file_id) as stmt:
    #         stmt.where(File.file_id.in_(file_ids))
    #         existing_file_ids = set(stmt.scalars)
    #     to_install = [f for f in files if f.file_id not in existing_file_ids]
    #     to_download: list[File] = []
    #     already_on_disk: list[File] = []
    #     for file in to_install:
    #         if status == FileStatus.Done:
    #             # skip check on status=done
    #             file.status = status
    #             to_download.append(file)
    #             continue
    #         path = self.fs.path_of(file)
    #         if path.is_file():
    #             file.status = FileStatus.Done
    #             already_on_disk.append(file)
    #         else:
    #             file.status = status
    #             to_download.append(file)
    #     self.db.add(*to_install)
    #     return to_download, already_on_disk

    # def remove(self, *files: File) -> list[File]:
    #     """
    #     Remove `files` from db and delete from filesystem.
    #     """
    #     file_ids = [f.file_id for f in files]
    #     with self.db.select(File) as stmt:
    #         stmt.where(File.file_id.in_(file_ids))
    #         deleted = stmt.scalars
    #     for file in files:
    #         if file.status == FileStatus.Done:
    #             self.fs.delete(file)
    #     self.db.delete(*deleted)
    #     return deleted

    # def autoremove(self) -> list[File]:
    #     """
    #     Search duplicate files and keep latest version only.
    #     """
    #     deprecated = self.db.get_deprecated_files()
    #     return self.remove(*deprecated)

    async def iter_results(
        self,
        processor: Processor,
        progress: Progress,
        task_ids: dict[str, TaskID],
    ) -> AsyncIterator[Result]:
        async for result in processor.process():
            task_idx = progress.task_ids.index(task_ids[result.data.file.sha])
            task = progress.tasks[task_idx]
            progress.update(task.id, visible=True)
            match result:
                case Ok():
                    progress.update(task.id, completed=result.data.completed)
                    if task.finished:
                        # TODO: add checksum verif here
                        progress.stop_task(task.id)
                        progress.update(task.id, visible=False)
                        sha = short_sha(result.data.file.sha)
                        sha = f"file: [cyan]{sha}[/]"
                        size = f"[green]{format_size(int(task.completed))}[/]"
                        items = [sha, size]
                        if task.elapsed is not None:
                            final_speed = int(task.completed / task.elapsed)
                            speed = f"[red]{format_size(final_speed)}/s[/]"
                            items.append(speed)
                        logger.info("✓ " + " · ".join(items))
                        yield result
                case Err():
                    progress.remove_task(task.id)
                    yield result
                case _:
                    raise ValueError("Unexpected result")

    async def download(
        self,
        queue: list[File],
        show_progress: bool = True,
    ) -> tuple[list[File], list[Err]]:
        """
        Download all files from db for which status is `Queued`.
        """
        for file in queue:
            file.status = FileStatus.Starting
        self.db.add(*queue)
        main_progress = self.ui.make_progress(
            SpinnerColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(compact=True, elapsed_when_finished=True),
        )
        file_progress = self.ui.make_progress(
            TextColumn("[cyan][{task.id}]"),
            "[progress.percentage]{task.percentage:>3.0f}%",
            BarColumn(),
            "·",
            DownloadColumn(),
            "·",
            TransferSpeedColumn(),
            transient=True,
        )
        queue_size = len(queue)
        main_task_id = main_progress.add_task("", total=queue_size)
        file_task_shas = {}
        start_callbacks = {}
        for file in queue:
            task_id = file_progress.add_task(
                "", total=file.size, visible=False, start=False
            )
            callback = partial(file_progress.start_task, task_id)
            file_task_shas[file.sha] = task_id
            start_callbacks[file.sha] = [callback]
        processor = Processor(
            config=self.config,
            auth=self.auth,
            fs=self.fs,
            files=queue,
            start_callbacks=start_callbacks,
        )
        # TODO: rename ? installed/downloaded/completed/...
        files: list[File] = []
        errors: list[Err] = []
        remaining_dict = {file.sha: file for file in queue}
        try:
            with self.ui.live(
                file_progress,
                main_progress,
                disable=not show_progress,
            ):
                async for result in self.iter_results(
                    processor, file_progress, file_task_shas
                ):
                    match result:
                        case Ok():
                            main_progress.update(main_task_id, advance=1)
                            result.data.file.status = FileStatus.Done
                            files.append(result.data.file)
                        case Err():
                            queue_size -= 1
                            main_progress.update(
                                main_task_id, total=queue_size
                            )
                            result.data.file.status = FileStatus.Error
                            errors.append(result)
                    self.db.add(result.data.file)
                    remaining_dict.pop(result.data.file.sha)
        finally:
            if remaining_dict:
                logger.warning(f"Cancelling {len(remaining_dict)} downloads.")
                cancelled: list[File] = []
                for file in remaining_dict.values():
                    file.status = FileStatus.Cancelled
                    cancelled.append(file)
                    errors.append(Err(file, DownloadCancelled()))
                self.db.add(*cancelled)
        return files, errors
