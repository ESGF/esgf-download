import rich
import click
from click_params import ListParamType

from esgpull.utils import naturalsize


class SliceParam(ListParamType):
    name = "slice"

    def __init__(self, separator: str = "-") -> None:
        super().__init__(click.INT, separator=separator, name="integers")

    def convert(self, value: str, param, ctx) -> slice:
        converted_list = super().convert(value, param, ctx)
        result: slice
        match converted_list:
            case [stop]:
                result = slice(0, stop)
            case [start, stop]:
                result = slice(start, stop)
            case _:
                self.fail(
                    self._error_message.format(errors="Bad value"), param, ctx
                )
        return result


def pretty_id(id: str) -> str:
    bar_idx = id.find("|")
    if bar_idx > 0:
        return id[:bar_idx]
    else:
        return id


def totable(
    results: list[dict], node: bool = False, _slice: slice = None
) -> rich.table.Table:
    if _slice is None:
        _slice = slice(0, len(results))
    rows: zip[tuple]
    table = rich.table.Table()
    table.add_column("#", justify="right")
    table.add_column("size", justify="right")
    table.add_column("id", justify="left")
    if node:
        table.add_column("node", justify="right")
    table.add_column("date", justify="right")
    timestamp = [r.get("timestamp", r.get("_timestamp")) for r in results]
    numids = map(str, range(_slice.start, _slice.stop))
    _slice = slice(0, _slice.stop - _slice.start)
    sizes = map(naturalsize, [r["size"] for r in results][_slice])
    ids = map(pretty_id, [r["id"] for r in results][_slice])
    dates = timestamp[_slice]
    if node:
        nodes = [r["data_node"] for r in results][_slice]
        rows = zip(numids, sizes, ids, nodes, dates)
    else:
        rows = zip(numids, sizes, ids, dates)
    for row in rows:
        table.add_row(*row)
    return table


__all__ = ["SliceParam", "totable"]
