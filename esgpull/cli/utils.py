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

from esgpull.query import Query
from esgpull.utils import format_size


def yaml_syntax(data: dict) -> Syntax:
    yml = yaml.dump(data)
    return Syntax(yml, "yaml", theme="ansi_dark")


def toml_syntax(data: dict) -> Syntax:
    tml = tomlkit.dumps(data)
    return Syntax(tml, "toml", theme="ansi_dark")


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
        # https://github.com/click-contrib/click_params/blob/master/click_params/base.py#L115
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


def filter_docs(
    docs: list[dict],
    indices: bool = True,
    size: bool = True,
    node: bool = False,
    date: bool = False,
    offset: int = 0,
) -> list[OrderedDict[str, Any]]:
    result: list[OrderedDict[str, Any]] = []
    for i, doc in enumerate(docs):
        od: OrderedDict[str, Any] = OrderedDict()
        if indices:
            od["#"] = i + offset
        if size:
            od["size"] = doc["size"]
        od["id"] = doc["id"].partition("|")[0]
        if node:
            od["node"] = doc["data_node"]
        if date:
            od["date"] = doc.get("timestamp") or doc.get("_timestamp")
        result.append(od)
    return result


def totable(
    docs: list[OrderedDict[str, Any]],
) -> Table:
    rows: list[map | list]
    table = Table(box=rich.box.MINIMAL)
    for key in docs[0].keys():
        table.add_column(key, justify="right")
    for doc in docs:
        row: list[str] = []
        for key, value in doc.items():
            if key == "size":
                value = format_size(value)
            row.append(str(value))
        table.add_row(*row)
    return table


def load_facets(
    query: Query, facets: list[str], selection_file: str | None
) -> None:
    facet_dict: dict[str, set[str]] = {}
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
                    "Using facet terms is forbidden "
                    "inside an exact string term."
                )
            exact_terms.append(value)
            if value.endswith("/"):
                final_exact_str = " ".join(exact_terms)
                value = '"' + final_exact_str.strip("/") + '"'
                exact_terms = None
            else:
                continue
        facet_dict.setdefault(name, set())
        facet_dict[name].add(value)
    query.load(facet_dict)  # type: ignore
    if selection_file is not None:
        query.load_file(selection_file)
