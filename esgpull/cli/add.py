from __future__ import annotations

from pathlib import Path

import click
import yaml
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import parse_query
from esgpull.graph import Graph
from esgpull.models import Query
from esgpull.tui import Verbosity


@click.command()
@args.facets
@groups.query_def
@opts.query_file
@opts.track
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
    verbosity: Verbosity,
) -> None:
    """
    Add one or more queries to the database.

    Adding a query will mark it as `untracked` by default.
    To associate files to this query, run the update command.
    """
    esg = Esgpull.with_verbosity(verbosity)
    with esg.ui.logging("add", onraise=Abort):
        if query_file is not None:
            with query_file.open() as f:
                content = yaml.safe_load(f)
            if isinstance(content, list):
                queries = [Query(**item) for item in content]
            else:
                queries = [Query(**content)]
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
            query.transient = not track
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
                raise Exit(1)
            query_name = f"[b green]{query.name}[/]"
            if query.sha in esg.graph:  # esg.graph.has(sha=query.sha):
                esg.ui.print(f"Skipping existing query: {query_name}")
            else:
                esg.graph.add(query)
                esg.ui.print(f"New query added: {query_name}")
        new_queries = esg.graph.merge(commit=True)
        nb = len(new_queries)
        ies = "ies" if nb > 1 else "y"
        if new_queries:
            esg.ui.print(f":thumbs_up: {nb} new quer{ies} added.")
        else:
            esg.ui.print(":stop_sign: No new query was added.")
