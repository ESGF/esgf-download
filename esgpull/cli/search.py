from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import parse_facets, yaml_syntax
from esgpull.models import Option, Options, Query
from esgpull.tui import Verbosity

# from esgpull.cli.utils import filter_docs, totable


@click.command()
@args.facets
@groups.query
@groups.display
@opts.date
@opts.data_node
@opts.dry_run
@opts.dump
@opts.file
@opts.hints
@opts.json
@opts.quiet
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
    date: bool,
    data_node: bool,
    dry_run: bool,
    dump: bool,
    file: bool,
    hints: list[str] | None,
    json: bool,
    quiet: bool,
    show: bool,
    # selection_file: str | None,
    verbosity: Verbosity,
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
    # options = load_options(options_raw)
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
        hits = esg.context.hits(query, file=file)
        if all_:
            size = sum(hits)
            slice_ = slice(0, size)
            offset = 0
        # if dry_run:
        #     queries = ctx._build_queries_search(
        #         hits, file=file, max_results=size, offset=offset
        #     )
        #     if json:
        #         esg.ui.print(queries)
        #     else:
        #         client = Client()
        #         for query in queries:
        #             url = query.pop("url")
        #             request = client.build_request("GET", url, params=query)
        #             esg.ui.print(request.url)
        #     raise Exit(0)
        if hints is not None:
            if hints[0] in "*/?.":
                not_distrib_query = query << Query(options=dict(distrib=False))
                facet_names = list(
                    esg.context.hints(
                        not_distrib_query,
                        file=file,
                        facets=["*"],
                    )[0]
                )
                esg.ui.print(facet_names)
                raise Exit(0)
            results = esg.context.hints(file=file, facets=hints)
            esg.ui.print(results)
            raise Exit(0)
        if dump:
            esg.ui.print(yaml_syntax(query.asdict()))
            raise Exit(0)
        if show:
            esg.ui.print(query)
            raise Exit(0)
        if size > 200:
            nb_req = size // esg.context.search.page_limit
            message = f"{nb_req} requests will be send to ESGF. Continue?"
            click.confirm(message, default=True, abort=True)
        results = esg.context.search_files(
            query,
            file=file,
            max_results=size,
            offset=offset,
            hits=hits,
        )
        # nb = sum(hits)
        # item_type = "file" if file else "dataset"
        # if not quiet and not json:
        #     esg.ui.print(f"Found {nb} {item_type}{'s' if nb > 1 else ''}.")
        # if results:
        #     docs = filter_docs(
        #         results,
        #         node=data_node,
        #         date=date,
        #         offset=offset,
        #     )
        #     if quiet:
        #         for doc in docs:
        #             esg.ui.print(doc["id"])
        #     elif json:
        #         esg.ui.print([dict(d) for d in docs])
        #     else:
        #         esg.ui.print(totable(docs))
