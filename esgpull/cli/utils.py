import sys
from collections import OrderedDict
from collections.abc import MutableMapping, Sequence
from enum import Enum
from pathlib import Path
from typing import Any, Literal

import click
import yaml
from click.exceptions import BadArgumentUsage
from rich.box import MINIMAL_DOUBLE_HEAD
from rich.table import Table
from rich.text import Text

from esgpull import Esgpull
from esgpull.graph import Graph
from esgpull.models import Dataset, File, Option, Options, Query, Selection
from esgpull.tui import UI, TempUI, Verbosity, logger
from esgpull.utils import format_size


def get_command() -> Text:
    exe, *args = sys.argv
    args = [arg for arg in args if arg != "--record"]
    return Text(" ".join(["$", Path(exe).name, *args]))


def init_esgpull(
    verbosity: Verbosity,
    safe: bool = True,
    record: bool = False,
    load_db: bool = True,
    no_default_query: bool = False,
) -> Esgpull:
    TempUI.verbosity = Verbosity.Errors
    with TempUI.logging():
        esg = Esgpull(
            verbosity=verbosity,
            safe=safe,
            record=record,
            load_db=load_db,
        )
        if no_default_query:
            esg.config.api.default_query_id = ""
        if record:
            esg.ui.print(get_command())
    return esg


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
    ids: range,
    size: bool = True,
    data_node: bool = False,
    # date: bool = False,
) -> list[OrderedDict[str, Any]]:
    result: list[OrderedDict[str, Any]] = []
    for i, doc in zip(ids, docs):
        od: OrderedDict[str, Any] = OrderedDict()
        od["id"] = str(i)
        if isinstance(doc, File):
            od["file"] = doc.file_id
        else:
            od["dataset"] = doc.dataset_id
            od["#"] = str(doc.number_of_files)
        if size:
            od["size"] = doc.size
        if data_node:
            od["data_node"] = doc.data_node
        # if date:
        #     od["date"] = doc.get("timestamp") or doc.get("_timestamp")
        result.append(od)
    return result


def totable(docs: list[OrderedDict[str, Any]]) -> Table:
    table = Table(box=MINIMAL_DOUBLE_HEAD, show_edge=False)
    for key in docs[0].keys():
        justify: Literal["left", "right", "center"]
        if key in ["file", "dataset"]:
            justify = "left"
        else:
            justify = "right"
        table.add_column(
            Text(key, justify="center"),
            justify=justify,
        )
    for doc in docs:
        row: list[str] = []
        for key, value in doc.items():
            if key == "size":
                value = format_size(value)
            row.append(value)
        table.add_row(*row)
    return table


def safe_value(value: str) -> str:
    if " " in value:
        return f'"{value}"'
    else:
        return value


def parse_facets(facets: list[str]) -> Selection:
    facet_dict: dict[str, list[str]] = {}
    for facet in facets:
        match facet.split(":"):
            case [value]:
                name = "query"
            case [name, value] if name and value:
                ...
            case _:
                raise BadArgumentUsage(f"{facet!r} is not valid syntax.")
        values = list(map(safe_value, value.split(",")))
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
    logger.info(f"{facets=}")
    logger.info(f"{tags=}")
    logger.info(f"{require=}")
    logger.info(f"{distrib=}")
    logger.info(f"{latest=}")
    logger.info(f"{replica=}")
    logger.info(f"{retracted=}")
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


def is_list_of_maps(seq: Sequence) -> bool:
    return all(isinstance(item, MutableMapping) for item in seq)


def serialize_queries_from_file(path: Path) -> list[Query]:
    with path.open() as f:
        content = yaml.safe_load(f)
    queries: list[MutableMapping]
    if isinstance(content, list):
        queries = content
    elif isinstance(content, MutableMapping):
        values = list(content.values())
        if is_list_of_maps(values):
            queries = values
        else:
            queries = [content]
    else:
        raise ValueError(content)
    return [Query(**query) for query in queries]


def valid_name_tag(
    graph: Graph,
    ui: UI,
    query_id: str | None,
    tag: str | None,
) -> bool:
    result = True
    if query_id is not None:
        shas = graph.matching_shas(query_id, graph._shas)
        if len(shas) > 1:
            ui.print(Messages.multimatch(query_id))
            ui.print(shas, json=True)
            result = False
        elif len(shas) == 0:
            ui.print(Messages.no_such_query(query_id), err=True)
            result = False
    elif tag is not None:
        tags = [t.name for t in graph.get_tags()]
        if tag not in tags:
            ui.print(Messages.none_tagged(tag), err=True)
            result = False
    return result


def get_queries(
    graph: Graph,
    query_id: str | None,
    tag: str | None,
    children: bool = False,
) -> list[Query]:
    queries: list[Query] = []
    if query_id is not None:
        query = graph.get(name=query_id)
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
