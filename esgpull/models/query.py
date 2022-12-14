from __future__ import annotations

from typing import Any, Iterator, Literal

import sqlalchemy as sa
from rich.console import Console, ConsoleOptions
from rich.measure import Measurement, measure_renderables
from rich.padding import Padding
from rich.text import Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from typing_extensions import NotRequired, TypedDict

from esgpull.models.base import Base, Sha
from esgpull.models.file import File
from esgpull.models.options import Options
from esgpull.models.selection import FacetValues, Selection
from esgpull.models.tag import Tag

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
    transient: NotRequired[Literal[True]]
    require: NotRequired[str]
    options: NotRequired[dict[str, bool | None]]
    selection: NotRequired[dict[str, FacetValues]]


class Query(Base):
    __tablename__ = "query"

    tags: Mapped[list[Tag]] = relationship(
        secondary=query_tag_proxy,
        default_factory=list,
    )
    transient: Mapped[bool] = mapped_column(default=False)
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

    def __post_init__(self) -> None:
        self._sha_name: bool = False

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

    def _as_bytes(self) -> bytes:
        self_tuple = (self.require, self.options.sha, self.selection.sha)
        return str(self_tuple).encode()

    def compute_sha(self) -> None:
        for tag in self.tags:
            tag.compute_sha()
        self.options.compute_sha()
        self.selection.compute_sha()
        super().compute_sha()

    def items(
        self,
        include_name: bool = False,
        keep_single_tag: bool = False,
    ) -> Iterator[tuple[str, Any]]:
        if include_name:
            yield "name", self.name
        if len(self.tags) > 1:
            yield "tags", [tag.name for tag in self.tags]
        elif len(self.tags) == 1 and keep_single_tag:
            yield "tags", self.tags[0].name
        if self.transient:
            yield "transient", self.transient
        if self.require is not None:
            yield "require", self.require
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
        if self.transient:
            result["transient"] = self.transient
        if self.require is not None:
            result["require"] = self.require
        if self.options:
            result["options"] = self.options.asdict()
        if self.selection:
            result["selection"] = self.selection.asdict()
        return result

    def clone(self, compute_sha: bool = True) -> Query:
        instance = Query(**self.asdict())
        if compute_sha:
            instance.compute_sha()
        return instance

    def no_require(self) -> Query:
        cl = self.clone(compute_sha=True)
        cl._rich_no_require = True
        return cl

    @property
    def name(self) -> str:
        # TODO: make these 2 lines useless
        if self.sha is None:
            self.compute_sha()
        if not hasattr(self, "_sha_name"):
            self._sha_name = False
        if len(self.tags) == 1 and not self._sha_name:
            return self.tags[0].name
        else:
            return f"#{self.sha[:6]}"

    @property
    def full_name(self) -> str:
        sha = f"#{self.sha[:6]}"
        if self.tags:
            tags = ", ".join(tag.name for tag in self.tags)
            return f"{sha} [{tags}]"
        else:
            return sha

    def __lshift__(self, other: Query) -> Query:
        result = self.clone(compute_sha=False)
        # if self.name != other.require:
        #     raise ValueError(f"{self.name} is not required by {other.name}")
        for tag in other.tags:
            if tag not in self.tags:
                result.tags.append(tag)
        for name, option in other.options.items():
            setattr(self.options, name, option)
        for name, facet in other.selection.items():
            result.selection[name] = facet
        result.transient = other.transient
        result.compute_sha()
        return result

    def __rich_repr__(self) -> Iterator:
        yield from self.items(include_name=True, keep_single_tag=False)

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        items = [
            f"{k}={v}"
            for k, v in self.items(include_name=True, keep_single_tag=False)
        ]
        return f"{cls_name}(" + ", ".join(items) + ")"

    def __rich_console__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> Iterator[Text | Padding]:
        def guide(t: Text, size: int = 2) -> Text:
            return t.with_indent_guides(size, style="dim default")

        text = Text()
        text.append(self.full_name, style="b green")
        if self.transient:
            text.append(" <transient>", style="i red")
        if not hasattr(self, "_rich_no_require") and self.require is not None:
            text.append(" [require: ")
            text.append(self.require, style="green")
            text.append("]")
        yield text
        for name, option in self.options.items():
            text = Text("  ")
            text.append(name, style="yellow")
            text.append(f": {option.value}")
            yield guide(text)
        for name, facet in self.selection.items():
            item = guide(Text(f"  {name}", style="blue"))
            if len(facet) == 1:
                item.append(f": {facet[0]}", style="default")
            else:
                item.append(":")
                for value in facet:
                    item.append(f"\n    - {value}", style="default")
                    item = guide(item, 4)
            yield item

    def __rich_measure__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> Measurement:
        renderables = list(self.__rich_console__(console, options))
        return measure_renderables(console, options, renderables)
