from __future__ import annotations

from collections.abc import Iterator, MutableMapping, Sequence
from typing import Any, Literal

import sqlalchemy as sa
from rich.console import Console, ConsoleOptions
from rich.table import Table
from rich.text import Text
from rich.tree import Tree
from sqlalchemy.orm import Mapped, mapped_column, object_session, relationship
from typing_extensions import NotRequired, TypedDict

from esgpull.exceptions import UntrackableQuery
from esgpull.models.base import Base, Sha
from esgpull.models.file import FileDict, FileStatus
from esgpull.models.options import Options
from esgpull.models.selection import FacetValues, Selection
from esgpull.models.tag import Tag
from esgpull.models.utils import (
    find_int,
    find_str,
    get_local_path,
    rich_measure_impl,
    short_sha,
)
from esgpull.utils import format_size

query_file_proxy = sa.Table(
    "query_file",
    Base.metadata,
    sa.Column("query_sha", Sha, sa.ForeignKey("query.sha"), primary_key=True),
    sa.Column("file_sha", Sha, sa.ForeignKey("file.sha"), primary_key=True),
)
query_tag_proxy = sa.Table(
    "query_tag",
    Base.metadata,
    sa.Column("query_sha", Sha, sa.ForeignKey("query.sha"), primary_key=True),
    sa.Column("tag_sha", Sha, sa.ForeignKey("tag.sha"), primary_key=True),
)


class File(Base):
    __tablename__ = "file"

    file_id: Mapped[str] = mapped_column(sa.String(255), unique=True)
    dataset_id: Mapped[str] = mapped_column(sa.String(255))
    master_id: Mapped[str] = mapped_column(sa.String(255))
    url: Mapped[str] = mapped_column(sa.String(255))
    version: Mapped[str] = mapped_column(sa.String(16))
    filename: Mapped[str] = mapped_column(sa.String(255))
    local_path: Mapped[str] = mapped_column(sa.String(255))
    data_node: Mapped[str] = mapped_column(sa.String(40))
    checksum: Mapped[str] = mapped_column(sa.String(64))
    checksum_type: Mapped[str] = mapped_column(sa.String(16))
    size: Mapped[int] = mapped_column(sa.BigInteger)
    status: Mapped[FileStatus] = mapped_column(
        sa.Enum(FileStatus), default=FileStatus.New
    )
    queries: Mapped[list[Query]] = relationship(
        secondary=query_file_proxy,
        default_factory=list,
        back_populates="files",
        repr=False,
    )

    def _as_bytes(self) -> bytes:
        self_tuple = (self.file_id, self.checksum)
        return str(self_tuple).encode()

    def compute_sha(self) -> None:
        Base.compute_sha(self)

    @classmethod
    def fromdict(cls, source: FileDict) -> File:
        result = cls(
            file_id=source["file_id"],
            dataset_id=source["dataset_id"],
            master_id=source["master_id"],
            url=source["url"],
            version=source["version"],
            filename=source["filename"],
            local_path=source["local_path"],
            data_node=source["data_node"],
            checksum=source["checksum"],
            checksum_type=source["checksum_type"],
            size=source["size"],
        )
        if "status" in source:
            result.status = FileStatus(source.get("source"))
        return result

    @classmethod
    def serialize(cls, source: dict) -> File:
        dataset_id = find_str(source["dataset_id"]).partition("|")[0]
        filename = find_str(source["title"])
        url = find_str(source["url"]).partition("|")[0]
        url = url.replace("http://", "https://")  # TODO: is this always true ?
        data_node = find_str(source["data_node"])
        checksum = find_str(source["checksum"])
        checksum_type = find_str(source["checksum_type"])
        size = find_int(source["size"])
        file_id = ".".join([dataset_id, filename])
        dataset_master, version = dataset_id.rsplit(".", 1)  # remove version
        master_id = ".".join([dataset_master, filename])
        local_path = get_local_path(source, version)
        result = cls.fromdict(
            {
                "file_id": file_id,
                "dataset_id": dataset_id,
                "master_id": master_id,
                "url": url,
                "version": version,
                "filename": filename,
                "local_path": local_path,
                "data_node": data_node,
                "checksum": checksum,
                "checksum_type": checksum_type,
                "size": size,
            }
        )
        result.compute_sha()
        return result

    def asdict(self) -> FileDict:
        return FileDict(
            file_id=self.file_id,
            dataset_id=self.dataset_id,
            master_id=self.master_id,
            url=self.url,
            version=self.version,
            filename=self.filename,
            local_path=self.local_path,
            data_node=self.data_node,
            checksum=self.checksum,
            checksum_type=self.checksum_type,
            size=self.size,
            status=self.status.name,
        )

    def clone(self, compute_sha: bool = True) -> File:
        result = File.fromdict(self.asdict())
        if compute_sha:
            result.compute_sha()
        return result


class QueryDict(TypedDict):
    tags: NotRequired[str | list[str]]
    tracked: NotRequired[Literal[True]]
    require: NotRequired[str]
    options: NotRequired[MutableMapping[str, bool | None]]
    selection: NotRequired[MutableMapping[str, FacetValues]]
    files: NotRequired[list[FileDict]]


class Query(Base):
    __tablename__ = "query"

    tags: Mapped[list[Tag]] = relationship(
        secondary=query_tag_proxy,
        default_factory=list,
    )
    tracked: Mapped[bool] = mapped_column(default=False)
    require: Mapped[str | None] = mapped_column(Sha, default=None)
    options_sha: Mapped[str] = mapped_column(
        Sha,
        sa.ForeignKey("options.sha"),
        init=False,
    )
    options: Mapped[Options] = relationship(default_factory=Options)
    selection_sha: Mapped[int] = mapped_column(
        Sha,
        sa.ForeignKey("selection.sha"),
        init=False,
    )
    selection: Mapped[Selection] = relationship(default_factory=Selection)
    files: Mapped[list[File]] = relationship(
        secondary=query_file_proxy,
        default_factory=list,
        back_populates="queries",
        repr=False,
    )

    def __init__(
        self,
        *,
        tracked: bool = False,
        require: str | None = None,
        tags: Sequence[Tag | str] | Tag | str | None = None,
        options: Options | MutableMapping[str, bool | None] | None = None,
        selection: Selection | MutableMapping[str, FacetValues] | None = None,
        files: list[FileDict] | None = None,
    ) -> None:
        self.tracked = tracked
        self.require = require
        self.tags = []
        if tags is not None:
            if isinstance(tags, (str, Tag)):
                tags = [tags]
            for tag in tags:
                if isinstance(tag, str):
                    self.tags.append(Tag(name=tag))
                elif isinstance(tag, Tag):
                    self.tags.append(tag)
        if selection is None:
            self.selection = Selection()
        elif isinstance(selection, dict):
            self.selection = Selection(**selection)
        elif isinstance(selection, Selection):
            self.selection = selection
        if options is None:
            self.options = Options()
        elif isinstance(options, dict):
            self.options = Options(**options)
        elif isinstance(options, Options):
            self.options = options
        self.files = []
        if files is not None:
            for file in files:
                self.files.append(File.fromdict(file))

    @property
    def has_files(self) -> bool:
        stmt: sa.Select[tuple[int]] = (
            sa.select(sa.func.count("*"))
            .join_from(query_file_proxy, File)
            .where(query_file_proxy.c.query_sha == self.sha)
        )
        session = object_session(self)
        if session is None:
            return bool(self.files)
        else:
            nb_files = session.scalar(stmt)
            return nb_files is not None and nb_files > 0

    def files_count_size(self, *status: FileStatus) -> tuple[int, int]:
        stmt: sa.Select[tuple[int, int]] = (
            sa.select(sa.func.count("*"), sa.func.sum(File.size))
            .join_from(query_file_proxy, File)
            .where(query_file_proxy.c.query_sha == self.sha)
        )
        session = object_session(self)
        if session is None:
            if status:
                files = [file for file in self.files if file.status in status]
            else:
                files = [file for file in self.files]
            count: int = len(files)
            size: int | None = sum([file.size for file in files])
        else:
            if status:
                stmt = stmt.where(File.status.in_(status))
            count, size = session.execute(stmt).all()[0]
        return count, size or 0

    def _as_bytes(self) -> bytes:
        self_tuple = (self.require, self.options.sha, self.selection.sha)
        return str(self_tuple).encode()

    def compute_sha(self) -> None:
        for tag in self.tags:
            tag.compute_sha()
        self.options.compute_sha()
        self.selection.compute_sha()
        super().compute_sha()

    @property
    def tag_name(self) -> str | None:
        if len(self.tags) == 1:
            return self.tags[0].name
        else:
            return None

    @property
    def name(self) -> str:
        # TODO: make these 2 lines useless
        if self.sha is None:
            self.compute_sha()
        elif ":" in self.sha:
            return self.sha.split(":")[0]
        return short_sha(self.sha)

    @property
    def rich_name(self) -> str:
        return f"[b green]{self.name}[/]"

    def items(self, include_name: bool = False) -> Iterator[tuple[str, Any]]:
        if include_name:
            yield "name", self.name
        if self.tags:
            yield "tags", [tag.name for tag in self.tags]
        if self.tracked:
            yield "tracked", self.tracked
        if self.require is not None:
            yield "require", short_sha(self.require)
        if self.options:
            yield "options", self.options
        if self.selection:
            yield "selection", self.selection

    def asdict(self) -> QueryDict:
        result: QueryDict = {}
        if len(self.tags) > 1:
            result["tags"] = [tag.name for tag in self.tags]
        elif len(self.tags) == 1:
            result["tags"] = self.tags[0].name
        if self.tracked:
            result["tracked"] = self.tracked
        if self.require is not None:
            result["require"] = self.require
        if self.options:
            result["options"] = self.options.asdict()
        if self.selection:
            result["selection"] = self.selection.asdict()
        return result

    def clone(self, compute_sha: bool = True) -> Query:
        instance = Query(**self.asdict())
        instance.files = list(self.files)
        if self.sha == "LEGACY":
            instance.sha = "LEGACY"
        elif compute_sha:
            instance.compute_sha()
        else:
            instance.sha = self.sha
        return instance

    def get_tag(self, name: str) -> Tag | None:
        result: Tag | None = None
        for tag in self.tags:
            if tag.name == name:
                result = tag
                break
        return result

    def add_tag(
        self,
        name: str,
        description: str | None = None,
        compute_sha: bool = True,
    ) -> None:
        if self.get_tag(name) is not None:
            raise ValueError(f"Tag {name!r} already exists.")
        tag = Tag(name=name, description=description)
        if compute_sha:
            tag.compute_sha()
        self.tags.append(tag)

    def update_tag(self, name: str, description: str | None) -> None:
        tag = self.get_tag(name)
        if tag is None:
            raise ValueError(f"Tag {name!r} does not exist.")
        else:
            tag.description = description

    def remove_tag(self, name: str) -> bool:
        tag = self.get_tag(name)
        if tag is not None:
            self.tags.remove(tag)
        return tag is not None

    def no_require(self) -> Query:
        cl = self.clone(compute_sha=False)
        cl._rich_no_require = True  # type: ignore [attr-defined]
        return cl

    def __lshift__(self, child: Query) -> Query:
        result = self.clone(compute_sha=False)
        # if self.name != child.require:
        #     raise ValueError(f"{self.name} is not required by {child.name}")
        for tag in child.tags:
            if tag not in result.tags:
                result.tags.append(tag)
        for name, option in child.options.items():
            setattr(result.options, name, option)
        for name, values in child.selection.items():
            result.selection[name] = values
        result.tracked = child.tracked
        result.compute_sha()
        files_shas = {f.sha for f in result.files}
        for file in child.files:
            if file.sha not in files_shas:
                result.files.append(file)
        return result

    @classmethod
    def _from_detailed_dict(cls, source: dict) -> Query:
        result = cls(tracked=True)
        for name, values in source.items():
            try:
                result.selection[name] = values
            except KeyError:
                ...
        result.compute_sha()
        return result

    def __rich_repr__(self) -> Iterator:
        yield from self.items(include_name=True)

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        items = [f"{k}={v}" for k, v in self.items(include_name=True)]
        return f"{cls_name}(" + ", ".join(items) + ")"

    __rich_measure__ = rich_measure_impl

    def _rich_tree(self) -> Tree:
        title = Text.from_markup(self.rich_name)
        if not self.tracked:
            title.append(" untracked", style="i red")
        contents = Table.grid(padding=(0, 1))
        if not hasattr(self, "_rich_no_require") and self.require is not None:
            if len(self.require) == 40:
                require = Text(short_sha(self.require), style="i green")
            else:
                if hasattr(self, "_unknown_require"):
                    require = Text(f"{self.require} [?]", style="red")
                else:
                    require = Text(self.require, style="magenta")
            contents.add_row("require:", require)
        if self.tags:
            text = Text()
            text.append("tags", style="magenta")
            text.append(":")
            contents.add_row(text, ", ".join([tag.name for tag in self.tags]))
        for name, option in self.options.items():
            text = Text()
            text.append(name, style="yellow")
            text.append(":")
            contents.add_row(text, str(option.value[1]))
        for name, values in self.selection.items():
            text = Text()
            if name != "query":
                text.append(name, style="blue")
                text.append(":")
            if len(values) == 1:
                values_str = values[0]
            else:
                values_str = ", ".join(values)
            contents.add_row(text, values_str)
        if self.has_files:
            count_ondisk, size_ondisk = self.files_count_size(FileStatus.Done)
            count_total, size_total = self.files_count_size()
            sizes = f"{format_size(size_ondisk)} / {format_size(size_total)}"
            lens = f"{count_ondisk}/{count_total}"
            contents.add_row(
                "files:", Text(f"{sizes} [{lens}]", style="magenta")
            )
        tree = Tree("", hide_root=True, guide_style="dim").add(title)
        if contents.row_count:
            tree.add(contents)
        return tree

    def __rich_console__(
        self,
        console: Console,
        opts: ConsoleOptions,
    ) -> Iterator[Tree]:
        yield self._rich_tree()

    def trackable(self) -> bool:
        return self.options.trackable()

    def track(self, options: Options | None = None, compute_sha: bool = True):
        if options is not None:
            self.options.apply_defaults(options)
        elif not self.options.trackable():
            raise UntrackableQuery(self.name)
        self.tracked = True
        if compute_sha:
            self.compute_sha()

    def untrack(self):
        self.tracked = False


LegacyQuery = Query()
LegacyQuery.compute_sha()  # compute shas for empty selection/options/...
LegacyQuery.sha = "LEGACY"
LegacyQuery.compute_sha = lambda: None  # type: ignore [assignment]
