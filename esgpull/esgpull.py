from pathlib import Path
from typing import Optional, cast

from sqlalchemy.orm.attributes import InstrumentedAttribute

from esgpull.types import File, Param, FileStatus
from esgpull.context import Context
from esgpull.auth import Auth  # , Identity
from esgpull.db import Database
from esgpull.download import Processor
from esgpull.fs import Filesystem
from esgpull.settings import Settings, SettingsPath


class Esgpull:
    def __init__(self, path: Optional[str | Path] = None) -> None:
        self.fs = Filesystem(path)
        self.db = Database(self.fs.db / "esgpull.db")
        self.auth = Auth(self.fs.auth)
        SettingsPath.path = self.fs.settings / "settings.yaml"
        self.settings = Settings()

    def fetch_index_nodes(self) -> list[str]:
        """
        Returns a list of ESGF index nodes.

        Fetch facet_counts from ESGF search API, using `distrib=True`.
        """

        ctx = Context(distrib=True)
        ctx.query.facets = "index_node"
        return list(ctx.facet_counts[0]["index_node"])

    def fetch_params(self, update=False) -> bool:
        """
        Fill db with all existing params found in ESGF index nodes.

        1. Fetch index nodes from `Esgpull.fetch_index_nodes()`
        2. Fetch all facets (names + values) from all index nodes.

        Workaround method, since searching directly for all facets using
        `distrib=True` seems to crash the index node.
        """

        IGNORE_NAMES = [
            "cf_standard_name",
            "variable_long_name",
            "creation_date",
            "datetime_end",
        ]

        with self.db.select(Param) as ctx:
            params = ctx.scalars
        if params and not update:
            return False
        self.db.delete(*params)
        index_nodes = self.fetch_index_nodes()
        ctx = Context(distrib=False, max_concurrent=len(index_nodes))
        for index_node in index_nodes:
            query = ctx.query.add()
            query.index_node = index_node
        index_facets = ctx.facet_counts
        facet_counts: dict[str, set[str]] = {}
        for facets in index_facets:
            for name, values in facets.items():
                if name in IGNORE_NAMES or len(values) == 0:
                    continue
                facet_values = set()
                for value, count in values.items():
                    if count and len(value) <= 255:
                        facet_values.add(value)
                if facet_values:
                    facet_counts.setdefault(name, set())
                    facet_counts[name] |= facet_values
        new_params = []
        for name, values in facet_counts.items():
            for value in values:
                new_params.append(Param(name, value))
        self.db.add(*new_params)
        return True

    def scan_local_files(self, index_node=None) -> None:
        """
        Insert into db netcdf files, globbed from `fs.data` directory.
        Only files which metadata is found on `index_node` is added.

        FileStatus is `done` regardless of the file's size (no checks).
        """
        context = Context()
        if index_node is not None:
            context.query.index_node = index_node
        filename_version_dict: dict[str, str] = {}
        for path in self.fs.glob_netcdf():
            if self.db.has(filepath=path):
                continue
            filename = path.name
            version = path.parent.name
            filename_version_dict[filename] = version
            query = context.query.add()
            query.title = filename
        if filename_version_dict:
            search_results = context.search(file=True)
            new_files = []
            for metadata in search_results:
                file = File.from_dict(metadata)
                if file.version == filename_version_dict[file.filename]:
                    new_files.append(file)
            self.install(*new_files, status=FileStatus.done)
            nb_remaining = len(filename_version_dict) - len(new_files)
            print(f"Installed {len(new_files)} new files.")
            print(f"{nb_remaining} files remain installed (another index?).")
        else:
            print("No new files.")

    def install(
        self, *files: File, status: FileStatus = FileStatus.queued
    ) -> list[File]:
        """
        Insert `files` with specified `status` into db if not already there.
        """
        file_ids = [f.file_id for f in files]
        with self.db.select(File.file_id) as stmt:
            # required by mypy, since dataclass + Mapped types dont work
            # https://github.com/sqlalchemy/sqlalchemy2-stubs/issues/129
            file_id_attr = cast(InstrumentedAttribute, File.file_id)
            stmt.where(file_id_attr.in_(file_ids))
            existing_file_ids = stmt.scalars
        installed = [f for f in files if f.file_id not in existing_file_ids]
        for file in installed:
            file.status = status
        self.db.add(*installed)
        return installed

    def remove(self, *files: File) -> list[File]:
        """
        Remove `files` from db and delete from filesystem.
        """
        file_ids = [f.file_id for f in files]
        with self.db.select(File) as stmt:
            # required by mypy, since dataclass + Mapped types dont work
            # https://github.com/sqlalchemy/sqlalchemy2-stubs/issues/129
            file_id_attr = cast(InstrumentedAttribute, File.file_id)
            stmt.where(file_id_attr.in_(file_ids))
            deleted = stmt.scalars
        for file in files:
            self.fs.delete(file)
        self.db.delete(*deleted)
        return deleted

    def autoremove(self) -> list[File]:
        """
        Search duplicate files and keep latest version only.
        """
        deprecated = self.db.get_deprecated_files()
        return self.remove(*deprecated)

    async def download_queued(self, use_bar=True) -> tuple[int, int]:
        """
        Download all files from db for which status is `queued`.
        """
        queue = self.db.get_files_with_status(FileStatus.queued)
        for file in queue:
            file.status = FileStatus.starting
        self.db.add(*queue)
        processor = Processor(self.auth, queue)
        async for file, data in processor.process(use_bar):
            await self.fs.write(file, data)
            file.status = FileStatus.done
            self.db.add(file)
        return len(queue), sum(file.size for file in queue)
