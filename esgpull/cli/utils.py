from collections import OrderedDict
from enum import Enum
from typing import Any

import click
import rich
import tomlkit
import yaml
from click.exceptions import BadArgumentUsage
from click_params import ListParamType
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from esgpull.graph import Graph
from esgpull.models import (
    Dataset,
    File,
    Option,
    Options,
    Query,
    QueryDict,
    Selection,
)
from esgpull.utils import format_size


def yaml_syntax(data: dict | QueryDict) -> Syntax:
    return Syntax(yaml.dump(data), "yaml", theme="ansi_dark")


def toml_syntax(data: dict | QueryDict) -> Syntax:
    return Syntax(tomlkit.dumps(data), "toml", theme="ansi_dark")


class EnumParam(click.Choice):
    name = "enum"

    def __init__(self, enum: type[Enum]):
        self.__enum = enum
        super().__init__(choices=[item.value for item in enum])

    def convert(self, value, param, ctx) -> Enum:
        converted_str = super().convert(value, param, ctx)
        return self.__enum(converted_str)


class SliceParam(ListParamType):
    name = "slice"

    def __init__(self) -> None:
        super().__init__(click.INT, separator=":", name="integers")

    def convert(self, value: str, param, ctx) -> slice:
        # https://github.com/click-contrib/click_params/blob/master/click_params/base.py#L115-L117
        if isinstance(value, str):
            self._convert_called = False
        converted_list = super().convert(value, param, ctx)
        start: int
        stop: int
        match converted_list:
            case [start, stop] if start < stop:
                ...
            case [stop]:
                start = 0
            case _:
                error_message = self._error_message.format(
                    errors=converted_list
                )
                self.fail(error_message, param, ctx)
        return slice(start, stop)


def filter_keys(
    docs: list[File] | list[Dataset],
    indices: bool = True,
    size: bool = True,
    # data_node: bool = False,
    # date: bool = False,
    offset: int = 0,
) -> list[OrderedDict[str, Any]]:
    result: list[OrderedDict[str, Any]] = []
    for i, doc in enumerate(docs):
        od: OrderedDict[str, Any] = OrderedDict()
        # is_file = doc["type"].lower() == "file"
        if indices:
            od["#"] = i + offset
        if size:
            od["size"] = doc.size
        if isinstance(doc, File):
            od["file"] = doc.file_id
        else:
            od["dataset"] = doc.dataset_id
        # if data_node:
        #     od["data_node"] = doc.data_node
        # if date:
        #     od["date"] = doc.get("timestamp") or doc.get("_timestamp")
        result.append(od)
    return result


def totable(docs: list[OrderedDict[str, Any]]) -> Table:
    table = Table(box=rich.box.MINIMAL)
    for key in docs[0].keys():
        table.add_column(
            Text(key, justify="center"),
            justify="right" if key in ["#", "size"] else "left",
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


def get_queries(
    graph: Graph,
    sha_or_name: str | None,
    tag: str | None,
) -> tuple[list[Query], str]:
    if sha_or_name is not None:
        if not graph.has(name=sha_or_name):
            return [], f":stop_sign: No such query [green]{sha_or_name}[/]"
        queries = [graph.get(name=sha_or_name)]
    elif tag is not None:
        queries = graph.with_tag(tag)
        if not queries:
            return [], f":stop_sign: No query tagged with [magenta]{tag}[/]"
    return queries, ""
