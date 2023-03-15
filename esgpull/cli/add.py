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
@opts.record
@opts.verbosity
def add(
    facets: list[str],
    # query options
    tags: list[str],
    require: str | None,
    distrib: str | None,
    latest: str | None,
    replica: str | None,
    retracted: str | None,
    # since: str | None,
    query_file: Path | None,
    track: bool,
    record: bool,
    verbosity: Verbosity,
) -> None:
    """
    Add one or more queries to the database.

    Adding a query will mark it as `untracked` by default.
    To associate files to this query, run the update command.
    """
    esg = init_esgpull(verbosity, record=record)
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
            query.tracked = track
            esg.graph.resolve_require(query)
            queries = [query]
        subgraph = Graph(None, *queries)
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
        new_queries = esg.graph.merge(commit=True)
        nb = len(new_queries)
        ies = "ies" if nb > 1 else "y"
        if new_queries:
            esg.ui.print(f":+1: {nb} new quer{ies} added.")
        else:
            esg.ui.print(":stop_sign: No new query was added.")
        esg.ui.raise_maybe_record(Exit(0))
