from typing import Iterable

from pathlib import Path
from dataclasses import dataclass

from esgpull import Storage, Context
from esgpull.storage.sqlite.tables import Status, File


@dataclass
class Drive:
    root: Path = Path("/home/srodriguez/ipsl/data/synda")

    def __post_init__(self):
        self.root = Path(self.root)

    @property
    def data(self) -> Path:
        return self.root / "data"

    def path_of(self, file: File) -> Path:
        return self.data / file.local_path / file.filename

    def glob_netcdf(self) -> Iterable[Path]:
        for path in self.data.glob("**/*.nc"):
            yield path

    def scan_local_files(
        self, storage: Storage, context: Context = None
    ) -> None:
        if context is None:
            context = Context()
        filename_table = storage.File.filename
        found_new_files = False
        filename_version_dict = {}
        for path in self.glob_netcdf():
            # [-]TODO: improve this somehow?
            with storage.select(filename_table) as stmt:
                # skip filenames already in database
                if len(stmt.where(filename_table == path.name).scalars) > 0:
                    continue
            found_new_files = True
            filename = path.name
            version = path.parent.name
            filename_version_dict[filename] = version
            # [-]TODO: improve this somehow?
            with context.query:
                context.query.title = filename
        if found_new_files:
            results = context.search(file=True, todf=False)
            files = []
            for metadata in results:
                f = storage.File.from_metadata(metadata)
                # fetching on `title` may return multiple versions
                if f.version == filename_version_dict[f.filename]:
                    f.status = Status.done
                    files.append(f)
            storage.session.add_all(files)
            storage.session.commit()
            print(f"Tracking {len(files)} new files.")
        else:
            print("No new files to track.")
