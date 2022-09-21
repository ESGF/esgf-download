from __future__ import annotations
from typing import Optional

import rich
import click
from click_params import ListParamType

from esgpull.query import Query
from esgpull.context import Context
from esgpull.cli.utils import arg, opt, SliceParam, totable


@click.command()
@opt.selection_file
@opt.distrib
@opt.dry_run
@opt.date
@click.option("--file", "-f", is_flag=True)
# @click.option("--local", "-l")
@click.option("--latest/--no-latest", "-l/-L", is_flag=True, default=None)
@click.option("--data-node", "-n", is_flag=True, default=False)
@click.option("--options", "-o", type=ListParamType(click.STRING, ","), default=None)
@click.option("--print-slice", "-S", type=SliceParam(), default="0-20")
@arg.facets
def search(
    facets: list[str],
    selection_file: Optional[str],
    file: bool,
    distrib: bool,
    dry_run: bool,
    date: bool,
    data_node: bool,
    options: list[str],
    print_slice: slice,
    latest: Optional[bool],
) -> None:
    """
    Search datasets/files on ESGF

    More info
    """

    # TODO: bug with print_slice:
    # -> numeric ids are not consistent due to sort by instance_id
    ctx = Context(distrib=distrib, latest=latest)
    offset = print_slice.start
    size = print_slice.stop - print_slice.start
    for facet in facets:
        parts = facet.split(":")
        if len(parts) == 1:
            ctx.query.query + parts
        elif len(parts) == 2:
            name, value = parts
            if name:
                ctx.query[name] + value
            else:
                ctx.query.query + value
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
    elif options:
        ctx.query.facets = options
        results = ctx.options()
        rich.print(results)
    else:
        results = ctx.search(file=file, max_results=size, offset=offset)
        nb = sum(hits)
        rich.print(f"Found {nb} result{'s' if nb > 1 else ''}.")
        if len(results):
            rich.print(totable(results, data_node, date, print_slice))
