from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import load_facets, totable, yaml_syntax
from esgpull.tui import Verbosity


@click.command()
@args.facets
@opts.date
@opts.data_node
@opts.distrib
@opts.dry_run
@opts.dump
@opts.file
@opts.latest
@opts.one
@opts.options
@opts.replica
@opts.selection_file
@opts.since
@opts.slice
@opts.verbosity
@opts.zero
def search(
    facets: list[str],
    date: bool,
    data_node: bool,
    distrib: bool,
    dry_run: bool,
    dump: bool,
    file: bool,
    latest: bool | None,
    one: bool,
    options: list[str],
    replica: bool | None,
    selection_file: str | None,
    since: str | None,
    slice_: slice,
    verbosity: Verbosity,
    zero: bool,
) -> None:
    """
    Search datasets/files on ESGF

    More info
    """

    esg = Esgpull.with_verbosity(verbosity)
    # TODO: bug with slice_:
    # -> numeric ids are not consistent due to sort by instance_id
    if zero:
        slice_ = slice(0, 0)
    elif one:
        slice_ = slice(0, 1)
    offset = slice_.start
    size = slice_.stop - slice_.start
    with esg.context() as ctx, esg.ui.logging("search", onraise=Abort):
        ctx.distrib = distrib
        ctx.latest = latest
        ctx.since = since
        ctx.replica = replica
        load_facets(ctx.query, facets, selection_file)
        if file:
            hits = ctx.file_hits
        else:
            hits = ctx.hits
        if dry_run:
            queries = ctx._build_queries_search(
                hits, file=file, max_results=size, offset=offset
            )
            esg.ui.print(queries)
            raise Exit(0)
        if options:
            ctx.query.facets = options
            results = ctx.options()
            esg.ui.print(results)
            raise Exit(0)
        if dump:
            esg.ui.print(yaml_syntax(ctx.query.dump()))
            raise Exit(0)
        results = ctx.search(
            file=file,
            max_results=size,
            offset=offset,
            hits=hits,
        )
        nb = sum(hits)
        item_type = "file" if file else "dataset"
        esg.ui.print(f"Found {nb} {item_type}{'s' if nb > 1 else ''}.")
        if results:
            esg.ui.print(totable(results, data_node, date, slice_))
