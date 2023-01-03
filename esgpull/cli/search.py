from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import filter_keys, parse_facets, totable, yaml_syntax
from esgpull.exceptions import SliceIndexError
from esgpull.models import Option, Options, Query
from esgpull.tui import Verbosity


@click.command()
@args.facets
@groups.query
@groups.display
# @opts.date
@opts.data_node
@opts.dry_run
@opts.dump
# @opts.file
@opts.hints
@opts.json
@opts.show
@opts.verbosity
# @opts.selection_file
def search(
    facets: list[str],
    # query options
    tags: list[str],
    require: str | None,
    distrib: Option | None,
    latest: Option | None,
    replica: Option | None,
    retracted: Option | None,
    # since: str | None,
    # display
    all_: bool,
    zero: bool,
    one: bool,
    slice_: slice,
    # ungrouped
    # date: bool,
    data_node: bool,
    dry_run: bool,
    dump: bool,
    # file: bool,
    hints: list[str] | None,
    json: bool,
    show: bool,
    # selection_file: str | None,
    verbosity: Verbosity,
) -> None:
    """
    Search files on ESGF

    More info
    """
    esg = Esgpull.with_verbosity(verbosity)
    # TODO: bug with slice_:
    # -> numeric ids are not consistent due to sort by instance_id
    options = Options()
    options.distrib = distrib or Option.notset
    options.latest = latest or Option.notset
    options.replica = replica or Option.notset
    options.retracted = retracted or Option.notset
    selection = parse_facets(facets)
    query = Query(
        tags=tags,
        require=require,
        options=options,
        selection=selection,
    )
    with esg.ui.logging("search", onraise=Abort):
        hits = esg.context.hits(query, file=True)
        nb = sum(hits)
        if zero:
            slice_ = slice(0, 0)
        elif one:
            slice_ = slice(0, 1)
        elif all_:
            slice_ = slice(0, nb)
        offset = slice_.start
        max_hits = slice_.stop - slice_.start
        if nb > 0 and slice_.start >= nb:
            raise SliceIndexError(slice_, nb)
        if dry_run:
            results = esg.context._init_search(
                query,
                file=True,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
            )
            for result in results:
                esg.ui.print(result.request.url)
            raise Exit(0)
        if hints is not None:
            if hints[0] in "*/?.":
                not_distrib_query = query << Query(options=dict(distrib=False))
                facet_counts = esg.context.hints(
                    not_distrib_query,
                    file=True,
                    facets=["*"],
                )
                esg.ui.print(list(facet_counts[0]), json=True)
                raise Exit(0)
            facet_counts = esg.context.hints(query, file=True, facets=hints)
            esg.ui.print(facet_counts, json=True)
            raise Exit(0)
        if dump:
            if json:
                esg.ui.print(query.asdict(), json=True)
            else:
                esg.ui.print(yaml_syntax(query.asdict()))
            raise Exit(0)
        if show:
            esg.ui.print(query)
            raise Exit(0)
        if max_hits > 200:
            nb_req = max_hits // esg.config.search.page_limit
            message = f"{nb_req} requests will be send to ESGF. Continue?"
            click.confirm(message, default=True, abort=True)
        if nb:
            query.files = esg.context.search_files(
                query,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
            )
        if json:
            esg.ui.print([f.asdict() for f in query.files], json=True)
            raise Exit(0)
        esg.ui.print(f"Found {nb} file{'s' if nb != 1 else ''}.")
        if query.files:
            docs = filter_keys(
                query.files,
                data_node=data_node,
                # date=date,
                offset=offset,
            )
            esg.ui.print(totable(docs))
