from __future__ import annotations
from typing import Optional

import rich
import click

from esgpull.query import Query
from esgpull.context import Context
from esgpull.utils import naturalsize
from esgpull.cli.utils import SliceParam


def pretty_id(id: str) -> str:
    bar_idx = id.find("|")
    if bar_idx > 0:
        return id[:bar_idx]
    else:
        return id


def totable(
    results: list[dict], node: bool, _slice: slice
) -> rich.table.Table:
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


@click.command()
@click.option("--selection-file", "-s")
@click.option("--file", "-f", is_flag=True)
@click.option("--distrib", "-d", is_flag=True)
@click.option("--dry-run", "-z", is_flag=True)
@click.option("--latest/--no-latest", "-l/-L", is_flag=True, default=None)
@click.option("--data-node", "-n", is_flag=True, default=False)
@click.option("--print-slice", "-S", type=SliceParam(), default="0-20")
@click.argument("facets", nargs=-1)
def search(
    facets: list[str],
    selection_file: Optional[str],
    file: bool,
    distrib: bool,
    dry_run: bool,
    data_node: bool,
    print_slice: slice,
    latest: bool = None,
) -> None:
    # TODO: bug with print_slice:
    # -> numeric ids are not consistent due to sort by instance_id
    ctx = Context(distrib=distrib, latest=latest)
    offset = print_slice.start
    size = print_slice.stop - print_slice.start
    for facet in facets:
        name, value = facet.split(":", 1)
        ctx.query[name] = value
    if selection_file is not None:
        other = Query.from_file(selection_file)
        ctx.query.update(other)
    if file:
        hits = ctx.file_hits
    else:
        hits = ctx.hits
    if dry_run:
        queries = ctx._build_queries_search(
            hits, file=file, max_results=size, offset=offset
        )
        rich.print(queries)
    else:
        results = ctx.search(file=file, max_results=size, offset=offset)
        nb = sum(hits)
        rich.print(f"Found {nb} result{'s' if nb > 1 else ''}.")
        if len(results):
            rich.print(totable(results, data_node, print_slice))
