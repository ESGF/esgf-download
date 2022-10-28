from enum import Enum
from typing import Any

import click
import rich
import tomlkit
import yaml
from click.exceptions import BadArgumentUsage
from click_params import ListParamType

from esgpull.query import Query
from esgpull.utils import format_size


def print_yaml(data: Any) -> None:
    yml = yaml.dump(data)
    syntax = rich.syntax.Syntax(yml, "yaml", theme="ansi_dark")
    rich.print(syntax)


def print_toml(data: Any) -> None:
    tml = tomlkit.dumps(data)
    syntax = rich.syntax.Syntax(tml, "toml", theme="ansi_dark")
    rich.print(syntax)


class EnumParam(click.Choice):
    name = "enum"

    def __init__(self, enum: type[Enum]):
        self.__enum = enum
        super().__init__(choices=[item.name for item in enum])

    def convert(self, value, param, ctx) -> Enum:
        converted_str = super().convert(value, param, ctx)
        return self.__enum[converted_str]


class SliceParam(ListParamType):
    name = "slice"

    def __init__(self, separator: str = ":") -> None:
        super().__init__(click.INT, separator=separator, name="integers")

    def convert(self, value: str, param, ctx) -> slice:
        converted_list = super().convert(value, param, ctx)
        start: int
        stop: int
        match converted_list:
            case [start, stop] if start < stop:
                ...
            case [stop]:
                start = 0
            case _:
                error_message = self._error_message.format(errors="Bad value")
                self.fail(error_message, param, ctx)
        return slice(start, stop)


def pretty_id(id: str) -> str:
    return id.partition("|")[0]


def totable(
    results: list[dict],
    node: bool = False,
    date: bool = False,
    _slice: slice = None,
) -> rich.table.Table:
    if _slice is None:
        _slice = slice(0, len(results))
    _slice_no_offset = slice(0, _slice.stop - _slice.start)
    rows: list[map | list]
    # table = rich.table.Table(box=rich.box.MINIMAL_DOUBLE_HEAD)
    table = rich.table.Table(box=rich.box.MINIMAL)
    table.add_column("#", justify="right")
    table.add_column("size", justify="right")
    table.add_column("id", justify="left")
    indices = map(str, range(_slice.start, _slice.stop))
    sizes = map(format_size, [r["size"] for r in results][_slice_no_offset])
    ids = map(pretty_id, [r["id"] for r in results][_slice_no_offset])
    rows = [indices, sizes, ids]
    if node:
        table.add_column("node", justify="right")
        nodes = [r["data_node"] for r in results][_slice_no_offset]
        rows.append(nodes)
    if date:
        table.add_column("date", justify="right")
        timestamp = [r.get("timestamp", r.get("_timestamp")) for r in results]
        dates = timestamp[_slice_no_offset]
        rows.append(dates)
    for row in zip(*rows):
        table.add_row(*row)
    return table


def load_facets(
    query: Query, facets: list[str], selection_file: str | None
) -> None:
    facet_dict: dict[str, set[str]] = {}
    for facet in facets:
        match facet.split(":"):
            case [value]:
                name = "query"
            case [name, value] if name and value:
                ...
            case _:
                raise BadArgumentUsage(f"'{facet}' is not valid syntax.")
        facet_dict.setdefault(name, set())
        facet_dict[name].add(value)
    query.load(facet_dict)  # type: ignore
    if selection_file is not None:
        query.load_file(selection_file)
