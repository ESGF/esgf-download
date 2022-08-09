import os
import asyncio
from pathlib import Path

import esgpull
from esgpull.types import *
from esgpull.context import *
from esgpull.db import *
from esgpull.fs import *
from esgpull.download import *
from esgpull.utils import *

MAJOR, MINOR, PATCH = 4, 0, 0
__semver__ = Semver(MAJOR, MINOR, PATCH)
__version__ = str(__semver__)

# # workaround for notebooks with running event loop
# if asyncio.get_event_loop().is_running():
#     import nest_asyncio

#     nest_asyncio.apply()


class Esgpull:
    # def __init__(self, path: str = None) -> None:
    def __init__(
        self, path: str | Path = Path("/home/srodriguez/ipsl/data/synda")
    ) -> None:
        st_home = os.environ.get("ST_HOME")
        if path is not None:
            root = Path(path)
        elif st_home is not None:
            root = Path(st_home)
        else:
            raise errors.NoRootError
        self.fs = Filesystem(root)
        self.db = Database(str(self.fs.db / "sdt_new.db"))

    def fetch_params(self, update=False) -> bool:
        SKIP_FACETS = [
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
        ctx = Context(distrib=True)
        ctx.query.facets = "index_node"
        index_nodes = list(ctx.facet_counts[0]["index_node"])
        ctx = Context(distrib=False)
        for index_node in index_nodes:
            with ctx.query:
                ctx.query.index_node = index_node
        index_facets = ctx.facet_counts
        facet_counts: dict[str, set[str]] = {}
        for facets in index_facets:
            for name, values in facets.items():
                if name in SKIP_FACETS or len(values) == 0:
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
            with context.query:
                context.query.title = filename
        if filename_version_dict:
            search_results = context.search(file=True, todf=False)
            new_files = []
            for metadata in search_results:
                file = File.from_metadata(metadata)
                if file.version == filename_version_dict[file.filename]:
                    new_files.append(file)
            self.install(new_files, Status.done)
            nb_remaining = len(filename_version_dict) - len(new_files)
            print(f"Installed {len(new_files)} new files.")
            print(f"{nb_remaining} files remain installed (another index?).")
        else:
            print("No new files.")

    def install(
        self, files: list[File], status: Status = Status.waiting
    ) -> list[File]:
        # def install(
        #     self, files: list[File], status: Status = Status.waiting
        # ) -> list[File]:
        installed = []
        for file in files:
            with self.db.select(File) as stmt:
                stmt.where(File.file_id == file.file_id)
                if len(stmt.scalars) == 0:
                    file.status = status
                    installed.append(file)
        self.db.add(*installed)
        return installed

    def remove(self, files: list[File]) -> list[File]:
        deleted = []
        for file in files:
            path = self.fs.path_of(file)
            path.unlink(missing_ok=True)
            if path.parent.is_dir():
                path.parent.rmdir()
            with self.db.select(File) as stmt:
                stmt.where(File.file_id == file.file_id)
                matching = stmt.scalars
                for m in matching:
                    deleted.append(m)
        self.db.delete(*deleted)
        return deleted

    async def download_waiting(self, use_bar=True) -> tuple[int, int]:
        waiting = self.db.get_files_with_status(Status.waiting)
        processor = Processor(waiting)
        async for file, data in processor.process(use_bar):
            await self.fs.write(file, data)
            file.status = Status.done
            self.db.add(file)
        return len(waiting), sum(file.size for file in waiting)


__all__ = (
    ["Esgpull"]
    + esgpull.types.__all__
    + esgpull.context.__all__
    + esgpull.db.__all__
    + esgpull.fs.__all__
    + esgpull.download.__all__
    + esgpull.utils.__all__
)
