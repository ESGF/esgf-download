from __future__ import annotations

import click
from click.exceptions import Abort

from esgpull import Esgpull
from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import parse_query
from esgpull.tui import Verbosity


@click.command()
@args.facets
@groups.query_def
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
    verbosity: Verbosity,
) -> None:
    """
    Add a query to the database.

    Adding a query will mark it as `transient` by default.
    To update the files associated with this query, use `update`.
    """
    esg = Esgpull.with_verbosity(verbosity)
    query = parse_query(
        facets=facets,
        tags=tags,
        require=require,
        distrib=distrib,
        latest=latest,
        replica=replica,
        retracted=retracted,
    )
    query.compute_sha()
    with esg.ui.logging("add", onraise=Abort):
        esg.graph.resolve_require(query)
        esg.ui.print(query)
        if esg.graph.has(sha=query.sha):
            esg.ui.print(":stop_sign: Query already exists.")
        else:
            query.transient = True
            esg.graph.add(query)
            new_queries = esg.graph.commit()
            if query.sha in new_queries:
                esg.ui.print(":thumbs_up: New query added")
            else:
                esg.ui.print(":stop_sign: Could not add query.")
