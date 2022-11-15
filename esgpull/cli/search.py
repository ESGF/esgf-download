from __future__ import annotations

import click
from click.exceptions import Abort, Exit
from httpx import Client

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import filter_docs, load_facets, totable, yaml_syntax
from esgpull.tui import Verbosity


@click.command()
@args.facets
@opts.date
@opts.data_node
@opts.distrib
@opts.dry_run
@opts.dump
@opts.file
@opts.json
@opts.latest
@opts.one
@opts.options
@opts.quiet
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
    json: bool,
    latest: bool,
    one: bool,
    options: list[str] | None,
    quiet: bool,
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
    with (
        esg.context(
            distrib=distrib,
            latest=latest,
            since=since,
            replica=replica,
        ) as ctx,
        esg.ui.logging("search", onraise=Abort),
    ):
        load_facets(ctx.query, facets, selection_file)
        if file:
            hits = ctx.file_hits
        else:
            hits = ctx.hits
        if dry_run:
            queries = ctx._build_queries_search(
                hits, file=file, max_results=size, offset=offset
            )
            if json:
                esg.ui.print(queries)
            else:
                client = Client()
                for query in queries:
                    url = query.pop("url")
                    request = client.build_request("GET", url, params=query)
                    esg.ui.print(request.url)
            raise Exit(0)
        if options is not None:
            if options[0] in "*/?.":
                ctx.distrib = False
                results = ctx.options(file=file)
                facet_names = [list(r) for r in results]
                esg.ui.print(facet_names)
                raise Exit(0)
            results = ctx.options(file=file, facets=options)
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
        if not quiet and not json:
            esg.ui.print(f"Found {nb} {item_type}{'s' if nb > 1 else ''}.")
        if results:
            docs = filter_docs(
                results,
                node=data_node,
                date=date,
                offset=offset,
            )
            if quiet:
                for doc in docs:
                    esg.ui.print(doc["id"])
            elif json:
                esg.ui.print([dict(d) for d in docs])
            else:
                esg.ui.print(totable(docs))
