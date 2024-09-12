from __future__ import annotations

from pathlib import Path

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import (
    init_esgpull,
    parse_query,
    serialize_queries_from_file,
)
from esgpull.graph import Graph
from esgpull.models import Query
from esgpull.tui import Verbosity


@click.command()
@args.facets
@groups.query_def
@opts.query_file
@opts.track
@opts.no_default_query
@opts.record
@opts.verbosity
def add(
    facets: list[str],
    ## query_def
    tags: list[str],
    require: str | None,
    distrib: str | None,
    latest: str | None,
    replica: str | None,
    retracted: str | None,
    ## ungrouped
    query_file: Path | None,
    track: bool,
    no_default_query: bool,
    record: bool,
    verbosity: Verbosity,
) -> None:
    """
    Add queries to the database

    OPTIONS / FACETS examples:

        esgpull add --distrib true variable_id:co2,co3 mip_era:CMIP6

            Syntax reference: http://www.esgf.io/esgf-download/search/

        esgpull add --query-file path/to/query.yaml

            Valid query files are usually created with either `show --json/--yaml` or `convert` commands.

    Queries are `untracked` by default.

    To fetch files from ESGF and link them to a query, see the `track` and `update` commands.
    """
    esg = init_esgpull(
        verbosity,
        record=record,
        no_default_query=no_default_query,
    )
    with esg.ui.logging("add", onraise=Abort):
        if query_file is not None:
            queries = serialize_queries_from_file(query_file)
        else:
            query = parse_query(
                facets=facets,
                tags=tags,
                require=require,
                distrib=distrib,
                latest=latest,
                replica=replica,
                retracted=retracted,
            )
            esg.graph.resolve_require(query)
            if track:
                if query.require is not None:
                    expanded = esg.graph.expand(query.require)
                else:
                    expanded = query
                query.track(expanded.options)
            queries = [query]
        queries = esg.insert_default_query(*queries)
        subgraph = Graph(None)
        subgraph.add(*queries)
        esg.ui.print(subgraph)
        empty = Query()
        empty.compute_sha()
        for query in queries:
            query.compute_sha()
            esg.graph.resolve_require(query)
            if query.sha == empty.sha:
                esg.ui.print(":stop_sign: Trying to add empty query.")
                esg.ui.raise_maybe_record(Exit(1))
            if query.sha in esg.graph:  # esg.graph.has(sha=query.sha):
                esg.ui.print(f"Skipping existing query: {query.rich_name}")
            else:
                esg.graph.add(query)
                esg.ui.print(f"New query added: {query.rich_name}")
        new_queries = esg.graph.merge()
        nb = len(new_queries)
        ies = "ies" if nb > 1 else "y"
        if new_queries:
            esg.ui.print(f":+1: {nb} new quer{ies} added.")
        else:
            esg.ui.print(":stop_sign: No new query was added.")
        esg.ui.raise_maybe_record(Exit(0))
