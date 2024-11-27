from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from functools import cached_property, partial
from pathlib import Path
from warnings import warn

from rich.live import Live
from rich.progress import (
    BarColumn,
    DownloadColumn,
    MofNCompleteColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from esgpull.auth import Auth, Credentials
from esgpull.config import Config
from esgpull.context import Context
from esgpull.database import Database
from esgpull.exceptions import (
    DownloadCancelled,
    InvalidInstallPath,
    NoInstallPath,
    UnknownDefaultQueryID,
)
from esgpull.fs import Filesystem
from esgpull.graph import Graph
from esgpull.install_config import InstallConfig
from esgpull.models import (
    Facet,
    File,
    FileStatus,
    LegacyQuery,
    Options,
    Query,
    sql,
)
from esgpull.models.utils import short_sha
from esgpull.processor import Processor
from esgpull.result import Err, Ok, Result
from esgpull.tui import UI, DummyLive, Verbosity, logger
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
        load_db: bool = True,
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
            InstallConfig.choose(path=default)
            if InstallConfig.current_idx is None:
                idx = InstallConfig.add(default)
                InstallConfig.choose(idx=idx)
                needs_install = True
            else:
                idx = InstallConfig.current_idx
                needs_install = False
            self.path = InstallConfig.installs[idx].path
            warning += "To disable this warning, please run:\n"
            if needs_install:
                warning += f"$ esgpull self install {self.path}"
            else:
                warning += f"$ esgpull self choose {self.path}"
            if logger.level == logging.NOTSET:
                warn(warning)
            else:
                logger.warning(warning)
        else:
            self.path = InstallConfig.current.path
        if not install and not self.path.is_dir():
            raise InvalidInstallPath(path=self.path)
        self.config = Config.load(path=self.path)
        Options._set_defaults(**self.config.api.default_options.asdict())
        self.fs = Filesystem.from_config(self.config, install=install)
        self.ui = UI.from_config(
            self.config,
            verbosity=verbosity,
            record=record,
        )
        credentials = Credentials.from_config(self.config)
        self.auth = Auth.from_config(self.config, credentials)
        self.context = Context(self.config, noraise=True)
        if load_db:
            self.db = Database.from_config(self.config)
            self.graph = Graph(self.db)

    def fetch_index_nodes(self) -> list[str]:
        """
        Returns a list of ESGF index nodes.

        Fetch hints from ESGF search API with a distributed query.
        """

        default_index = self.config.api.index_node
        logger.info(f"Fetching index nodes from {default_index!r}")
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

    @cached_property
    def legacy_query(self) -> Query:
        legacy = LegacyQuery
        if (
            legacy_db := self.db.get(Query, "LEGACY")
        ) and legacy_db is not None:
            legacy = legacy_db
        # else:
        # self.db.add(legacy)
        # self.graph.add(legacy, clone=False)
        # self.graph.merge(commit=True)
        return legacy

    def import_synda(
        self,
        url: Path,
        track: bool = False,
        size: int = 5000,
        ask: bool = False,
    ) -> int:
        assert url.is_file()
        synda = Database(f"sqlite:///{url}", run_migrations=False)
        synda_ids = synda.scalars(sql.synda_file.ids())
        shas = set(self.db.scalars(sql.file.linked()))
        msg = f"Found {len(synda_ids)} files to import, proceed?"
        if ask and not self.ui.ask(msg):
            return 0
        synda_shas: set[str] = set()
        idx_range = range(0, len(synda_ids), size)
        if track:
            iter_idx_range = self.ui.track(idx_range)
        else:
            iter_idx_range = iter(idx_range)
        nb_imported = 0
        for start in iter_idx_range:
            stop = min(len(synda_ids), start + size)
            ids = synda_ids[start:stop]
            synda_files = synda.scalars(sql.synda_file.with_ids(*ids))
            files: list[File] = []
            for synda_file in synda_files:
                file = synda_file.to_file()
                if file.sha not in shas:
                    file.queries.append(self.legacy_query)
                    files.append(file)
                    synda_shas.add(file.sha)
            if files:
                nb_imported += len(files)
                self.db.add(*files)
        return nb_imported

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
        live: Live | DummyLive,
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
                        sha = f"[b blue]{task.fields['sha']}[/]"
                        file = result.data.file
                        digest = result.data.digest
                        match self.fs.finalize(file, digest=digest):
                            case Ok():
                                size = f"[green]{format_size(int(task.completed))}[/]"
                                if task.elapsed is not None:
                                    final_speed = int(
                                        task.completed / task.elapsed
                                    )
                                    speed = (
                                        f"[red]{format_size(final_speed)}/s[/]"
                                    )
                                else:
                                    speed = "[b red]?[/]"
                                data_node = (
                                    f"[blue]{task.fields['data_node']}[/]"
                                )
                                parts = [sha, size, speed, data_node]
                                if self.config.download.show_filename:
                                    parts.append(task.fields["filename"])
                                msg = " · ".join(parts)
                                logger.info(msg)
                                live.console.print(msg)
                                yield result
                            case Err(_, err):
                                progress.remove_task(task.id)
                                yield Err(result.data, err)
                case Err():
                    progress.remove_task(task.id)
                    yield result
                case _:
                    raise ValueError("Unexpected result")

    async def download(
        self,
        queue: list[File],
        use_db: bool = True,
        show_progress: bool = True,
    ) -> tuple[list[File], list[Err]]:
        """
        Download files provided in `queue`.
        """
        for file in queue:
            file.status = FileStatus.Starting
        main_progress = self.ui.make_progress(
            SpinnerColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(compact=True, elapsed_when_finished=True),
        )
        file_columns: list[str | ProgressColumn] = [
            TextColumn("[cyan][{task.id}] [b blue]{task.fields[sha]}"),
            "[progress.percentage]{task.percentage:>3.0f}%",
            BarColumn(),
            "·",
            DownloadColumn(binary_units=True),
            "·",
            TransferSpeedColumn(),
            "·",
            TextColumn("[blue]{task.fields[data_node]}"),
        ]
        if self.config.download.show_filename:
            file_columns.extend(
                [
                    "·",
                    TextColumn("{task.fields[filename]}"),
                ]
            )
        file_progress = self.ui.make_progress(
            *file_columns,
            transient=True,
        )
        file_task_shas = {}
        start_callbacks = {}
        for file in queue:
            task_id = file_progress.add_task(
                "",
                total=file.size,
                visible=False,
                start=False,
                sha=short_sha(file.sha),
                filename=file.filename,
                data_node=file.data_node,
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
        if use_db:
            self.db.add(*processor.files)
        queue_size = len(processor.tasks)
        main_task_id = main_progress.add_task("", total=queue_size)
        # TODO: rename ? installed/downloaded/completed/...
        files: list[File] = []
        errors: list[Err] = []
        remaining_dict = {file.sha: file for file in processor.files}
        try:
            with self.ui.live(
                file_progress,
                main_progress,
                disable=not show_progress,
            ) as live:
                async for result in self.iter_results(
                    processor,
                    file_progress,
                    file_task_shas,
                    live,
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
                    if use_db:
                        self.db.add(result.data.file)
                    remaining_dict.pop(result.data.file.sha, None)
        finally:
            if remaining_dict:
                logger.warning(f"Cancelling {len(remaining_dict)} downloads.")
                cancelled: list[File] = []
                for file in remaining_dict.values():
                    file.status = FileStatus.Cancelled
                    cancelled.append(file)
                    errors.append(Err(file, DownloadCancelled()))
                if use_db:
                    self.db.add(*cancelled)
        return files, errors

    def replace_queries(
        self,
        graph: Graph,
        mapping: tuple[str | None, str],
    ) -> None:
        to_replace = [
            q for q in graph.queries.values() if q.require == mapping[0]
        ]
        for query in to_replace:
            new_query = query.clone(compute_sha=False)
            new_query.require = mapping[1]
            new_query.compute_sha()
            graph.replace(query, new_query)
            self.replace_queries(graph, (query.sha, new_query.sha))

    def insert_default_query(self, *queries: Query) -> list[Query]:
        if self.config.api.default_query_id == "":
            return list(queries)
        default_query_id = self.config.api.default_query_id
        try:
            default_query = self.graph.get(default_query_id)
        except KeyError:
            raise UnknownDefaultQueryID(default_query_id)
        graph = Graph(None)
        graph.add(*queries)
        self.replace_queries(graph, (None, default_query.sha))
        return list(graph.queries.values())
