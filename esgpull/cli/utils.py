import sys
from collections import OrderedDict
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Sequence

import click
from click.exceptions import BadArgumentUsage
from rich.box import MINIMAL_DOUBLE_HEAD
from rich.table import Table
from rich.text import Text

from esgpull.graph import Graph
from esgpull.models import Dataset, File, Option, Options, Query, Selection
from esgpull.tui import UI
from esgpull.utils import format_size


class Messages:
    @staticmethod
    def no_such_query(name: str) -> str:
        return f":stop_sign: No such query [green]{name}[/]"

    @staticmethod
    def none_tagged(tag: str) -> str:
        return f":stop_sign: No query tagged with [magenta]{tag}[/]"

    @staticmethod
    def multimatch(name: str) -> str:
        return f"Found multiple queries starting with [b cyan]{name}[/]"


class EnumParam(click.Choice):
    name = "enum"

    def __init__(self, enum: type[Enum]):
        self.__enum = enum
        super().__init__(choices=[item.value for item in enum])

    def convert(self, value, param, ctx) -> Enum:
        converted_str = super().convert(value, param, ctx)
        return self.__enum(converted_str)


def filter_keys(
    docs: Sequence[File | Dataset],
    size: bool = True,
    data_node: bool = False,
    # date: bool = False,
) -> list[OrderedDict[str, Any]]:
    result: list[OrderedDict[str, Any]] = []
    for i, doc in enumerate(docs):
        od: OrderedDict[str, Any] = OrderedDict()
        if isinstance(doc, File):
            od["file"] = doc.file_id
        else:
            od["dataset"] = doc.dataset_id
            od["files"] = doc.number_of_files
        if size:
            od["size"] = doc.size
        if data_node:
            od["data_node"] = doc.data_node
        # if date:
        #     od["date"] = doc.get("timestamp") or doc.get("_timestamp")
        result.append(od)
    return result


def totable(docs: list[OrderedDict[str, Any]]) -> Table:
    table = Table(box=MINIMAL_DOUBLE_HEAD)
    for key in docs[0].keys():
        justify: Literal["left", "right", "center"]
        if key == "size":
            justify = "right"
        elif key == "files":
            justify = "center"
        else:
            justify = "left"
        table.add_column(
            Text(key, justify="center"),
            justify=justify,
        )
    for doc in docs:
        row: list[str] = []
        for key, value in doc.items():
            if key == "size":
                value = format_size(value)
            row.append(str(value))
        table.add_row(*row)
    return table


def parse_facets(facets: list[str]) -> Selection:
    facet_dict: dict[str, list[str]] = {}
    exact_terms: list[str] | None = None
    for facet in facets:
        match facet.split(":"):
            case [value]:
                name = "query"
            case [name, value] if name and value:
                ...
            case _:
                raise BadArgumentUsage(f"'{facet}' is not valid syntax.")
        if value.startswith("/"):
            if exact_terms is not None:
                raise BadArgumentUsage("Nested exact string is forbidden.")
            exact_terms = []
        if exact_terms is not None:
            if name != "query":
                raise BadArgumentUsage(
                    "Cannot use facet term inside an exact string."
                )
            exact_terms.append(value)
            if value.endswith("/"):
                final_exact_str = " ".join(exact_terms)
                value = '"' + final_exact_str.strip("/") + '"'
                exact_terms = None
            else:
                continue
            facet_dict.setdefault(name, [])
            facet_dict[name].append(value)
        else:
            values = value.split(",")
            facet_dict.setdefault(name, [])
            facet_dict[name].extend(values)
    selection = Selection()
    for name, values in facet_dict.items():
        selection[name] = values
    return selection


def parse_query(
    facets: list[str],
    # query options
    tags: list[str],
    require: str | None,
    distrib: str | None,
    latest: str | None,
    replica: str | None,
    retracted: str | None,
) -> Query:
    options = Options(
        distrib=distrib or Option.notset,
        latest=latest or Option.notset,
        replica=replica or Option.notset,
        retracted=retracted or Option.notset,
    )
    selection = parse_facets(facets)
    return Query(
        tags=tags,
        require=require,
        options=options,
        selection=selection,
    )


def valid_name_tag(
    graph: Graph,
    ui: UI,
    sha_or_name: str | None,
    tag: str | None,
) -> bool:
    result = True
    if sha_or_name is not None:
        shas = graph.matching_shas(sha_or_name, graph._shas)
        if len(shas) > 1:
            ui.print(Messages.multimatch(sha_or_name))
            ui.print(shas, json=True)
            result = False
        elif len(shas) == 0:
            ui.print(Messages.no_such_query(sha_or_name), err=True)
            result = False
    elif tag is not None:
        tags = [t.name for t in graph.get_tags()]
        if tag not in tags:
            ui.print(Messages.none_tagged(tag), err=True)
            result = False
    return result


def get_queries(
    graph: Graph,
    sha_or_name: str | None,
    tag: str | None,
    children: bool = False,
) -> list[Query]:
    queries: list[Query] = []
    if sha_or_name is not None:
        query = graph.get(name=sha_or_name)
        if query is not None:
            queries = [query]
    elif tag is not None:
        queries = graph.with_tag(tag)
    if children:
        for i in range(len(queries)):
            query = queries[0]
            kids = graph.get_all_children(query.sha)
            queries.extend(kids)
    return queries


def get_command() -> str:
    exe, *args = sys.argv
    args = [arg for arg in args if arg != "--record"]
    return " ".join(["$", Path(exe).name, *args])
