from __future__ import annotations
from typing import Optional

import rich
import click
import pandas
from humanize import naturalsize

from esgpull import Context
from esgpull.cli.utils import SliceParam


def pretty_id(id: str) -> str:
    bar_idx = id.find("|")
    if bar_idx > 0:
        return id[:bar_idx]
    else:
        return id


def totable(
    df: pandas.DataFrame, node: bool, _slice: slice
) -> rich.table.Table:
    rows: zip[tuple]
    table = rich.table.Table()
    table.add_column("", justify="right")
    table.add_column("size", justify="right")
    table.add_column("id", justify="left")
    if node:
        table.add_column("node", justify="right")
    table.add_column("date", justify="right")
    timestamp = df.get("timestamp", df["_timestamp"])
    numids = map(str, range(_slice.start, _slice.stop))
    sizes = map(naturalsize, df["size"][_slice])
    ids = map(pretty_id, df["id"][_slice])
    dates = timestamp[_slice]
    if node:
        nodes = df["data_node"][_slice]
        rows = zip(numids, sizes, ids, nodes, dates)
    else:
        rows = zip(numids, sizes, ids, dates)
    for row in rows:
        table.add_row(*row)
    return table


@click.command()
@click.option("--selection-file", "-s")
@click.option("--dry-run", "-z", is_flag=True)
@click.option("--latest", "-l/-L", is_flag=True, default=None)
@click.option("--print-node", "-n", is_flag=True, default=False)
@click.option("--print-slice", "-S", type=SliceParam(), default="0-20")
@click.argument("facets", nargs=-1)
def search(
    facets: list[str],
    selection_file: Optional[str],
    dry_run: bool,
    print_node: bool,
    print_slice: slice,
    latest: bool = None,
) -> None:
    ctx = Context(latest=latest)
    for facet in facets:
        name, value = facet.split(":")
        ctx.query[name] = value
    if selection_file is not None:
        ctx.query.load(selection_file)
    if dry_run:
        queries = ctx._build_queries_search(ctx.hits, file=False)
        rich.print(queries)
    else:
        df = ctx.search(todf=True)
        rich.print(f"Found {len(df)} results.")
        if len(df):
            rich.print(totable(df, print_node, print_slice))
