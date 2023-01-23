from __future__ import annotations

from typing import Any, Iterator, Literal, Mapping

import sqlalchemy as sa
from rich.console import Console, ConsoleOptions
from rich.padding import Padding
from rich.text import Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing_extensions import NotRequired, TypedDict

from esgpull.models.base import Base, Sha
from esgpull.models.file import File, FileStatus
from esgpull.models.options import Options
from esgpull.models.selection import FacetValues, Selection
from esgpull.models.tag import Tag
from esgpull.models.utils import rich_measure_impl, short_sha
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


class QueryDict(TypedDict):
    tags: NotRequired[str | list[str]]
    tracked: NotRequired[Literal[True]]
    require: NotRequired[str]
    options: NotRequired[Mapping[str, bool | None]]
    selection: NotRequired[Mapping[str, FacetValues]]
    # children: NotRequired[list[QueryDict]]


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
    )

    def _as_bytes(self) -> bytes:
        self_tuple = (self.require, self.options.sha, self.selection.sha)
        return str(self_tuple).encode()

    def compute_sha(self) -> None:
        for tag in self.tags:
            tag.compute_sha()
        self.options.compute_sha()
        self.selection.compute_sha()
        super().compute_sha()

    # TODO: improve typing
    def __setattr__(self, name: str, value: Any) -> None:
        if name == "tags" and isinstance(value, (str, Tag)):
            value = [value]
        match name, value:
            case "tags", list() | tuple():
                tags: list[Tag] = []
                for tag in value:
                    if isinstance(tag, str):
                        tags.append(Tag(name=tag))
                    else:
                        tags.append(tag)
                value = tags
            case "options", dict():
                value = Options(**value)
            case "selection", dict():
                value = Selection(**value)
            case _, _:
                ...
        super().__setattr__(name, value)

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
        return short_sha(self.sha)

    def items(
        self,
        include_name: bool = False,
    ) -> Iterator[tuple[str, Any]]:
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
        if compute_sha:
            instance.compute_sha()
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
            raise ValueError(f"Tag '{name}' already exists.")
        tag = Tag(name=name, description=description)
        if compute_sha:
            tag.compute_sha()
        self.tags.append(tag)

    def update_tag(self, name: str, description: str | None) -> None:
        tag = self.get_tag(name)
        if tag is None:
            raise ValueError(f"Tag '{name}' does not exist.")
        else:
            tag.description = description

    def remove_tag(self, name: str) -> bool:
        tag = self.get_tag(name)
        if tag is not None:
            self.tags.remove(tag)
        return tag is not None

    def no_require(self) -> Query:
        cl = self.clone(compute_sha=True)
        cl._rich_no_require = True
        return cl

    def __lshift__(self, other: Query) -> Query:
        result = self.clone(compute_sha=False)
        # if self.name != other.require:
        #     raise ValueError(f"{self.name} is not required by {other.name}")
        for tag in other.tags:
            if tag not in result.tags:
                result.tags.append(tag)
        for name, option in other.options.items():
            setattr(result.options, name, option)
        for name, values in other.selection.items():
            result.selection[name] = values
        result.tracked = other.tracked
        result.compute_sha()
        files_shas = set([f.sha for f in result.files])
        for file in other.files:
            if file.sha not in files_shas:
                result.files.append(file)
        return result

    def __rich_repr__(self) -> Iterator:
        yield from self.items(include_name=True)

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        items = [f"{k}={v}" for k, v in self.items(include_name=True)]
        return f"{cls_name}(" + ", ".join(items) + ")"

    def __guide(self, text: Text, size: int = 2) -> Text:
        return text.with_indent_guides(size, style="dim default")

    def __wrap_values(
        self,
        text: Text,
        values: list[str],
        maxlen: int = 40,
    ) -> Text:
        text.append("[")  # ]
        textlen = len(text)
        maxlen = 40 - textlen
        padding = " " * textlen
        lines: list[str] = []
        curline: list[str] = []
        for value in values:
            newline = curline + [value]
            strline = ", ".join(newline)
            if len(strline) < maxlen:
                curline = newline
            else:
                curline = []
                lines.append(strline)
        if not lines:
            lines = [", ".join(curline)]
        text.append(lines[0])
        for line in lines[1:]:
            text.append(f",\n{padding}{line}")
            text = self.__guide(text, textlen)
        text.append("]")
        return text

    __rich_measure__ = rich_measure_impl

    def __rich_console__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> Iterator[Text | Padding]:
        text = Text()
        text.append(self.name, style="b green")
        if not self.tracked:
            text.append(" <untracked>", style="i red")
        yield text
        if not hasattr(self, "_rich_no_require") and self.require is not None:
            text = Text("  require: ")
            if len(self.require) == 40:
                text.append(short_sha(self.require), style="i green")
            else:
                if hasattr(self, "_unknown_require"):
                    text.append(self.require, style="red")
                    text.append(" [?]")
                else:
                    text.append(self.require, style="magenta")
            yield self.__guide(text)
        if self.files:
            text = Text("  ")
            ondisk = [f for f in self.files if f.status == FileStatus.Done]
            size_ondisk = format_size(sum([f.size for f in ondisk]))
            size_total = format_size(sum([f.size for f in self.files]))
            text.append(str(len(ondisk)), style="b magenta")
            text.append(" / ")
            text.append(str(len(self.files)), style="b magenta")
            text.append(" files on disk [")
            text.append(size_ondisk, style="b magenta")
            text.append(" / ")
            text.append(size_total, style="b magenta")
            text.append("]")
            yield self.__guide(text)
        if self.tags:
            text = Text("  ")
            text.append("tags", style="magenta")
            text.append(": ")
            text = self.__wrap_values(text, [tag.name for tag in self.tags])
            yield self.__guide(text)
        for name, option in self.options.items():
            text = Text("  ")
            text.append(name, style="yellow")
            text.append(f": {option.value}")
            yield self.__guide(text)
        query_term: list[str] | None = None
        for name, values in self.selection.items():
            if name == "query":
                query_term = values
                continue
            item = Text("  ")
            item.append(name, style="blue")
            if len(values) == 1:
                item.append(f": {values[0]}")
            else:
                item.append(": ")
                item = self.__wrap_values(item, values)
            yield self.__guide(item)
        if query_term is not None:
            item = Text("  ", style="blue")
            if len(query_term) == 1:
                item.append(query_term[0])
            else:
                item = self.__wrap_values(item, query_term)
            yield self.__guide(item)
