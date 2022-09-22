import rich
import click
from click_params import ListParamType

from esgpull.utils import naturalsize


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
    table = rich.table.Table()
    table.add_column("#", justify="right")
    table.add_column("size", justify="right")
    table.add_column("id", justify="left")
    indices = map(str, range(_slice.start, _slice.stop))
    sizes = map(naturalsize, [r["size"] for r in results][_slice_no_offset])
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


__all__ = ["SliceParam", "totable"]
