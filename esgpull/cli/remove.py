from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, valid_name_tag
from esgpull.graph import Graph
from esgpull.tui import Verbosity


@click.command()
@args.sha_or_name
@opts.tag
@opts.children
@opts.verbosity
def remove(
    sha_or_name: str | None,
    tag: str | None,
    children: bool,
    verbosity: Verbosity,
) -> None:
    """
    Remove queries
    """
    esg = Esgpull(verbosity=verbosity)
    with esg.ui.logging("remove", onraise=Abort):
        if sha_or_name is None and tag is None:
            raise click.UsageError("No query or tag provided.")
        if not valid_name_tag(esg.graph, esg.ui, sha_or_name, tag):
            raise Exit(1)
        queries = get_queries(
            esg.graph,
            sha_or_name,
            tag,
            children=children,
        )
        nb = len(queries)
        ies = "ies" if nb > 1 else "y"
        esg.ui.print(Graph(None, *queries))
        msg = f"Remove {nb} quer{ies}?"
        if not esg.ui.ask(msg, default=True):
            raise Abort
        for query in queries:
            if not children and esg.graph.get_children(query.sha):
                esg.ui.print(
                    ":stop_sign: Some queries block "
                    f"removal of {query.rich_name}."
                )
                if esg.ui.ask("Show blocking queries?", default=False):
                    esg.ui.print(esg.graph.subgraph(query, children=True))
                raise Exit(1)
        esg.db.delete(*queries)
        esg.ui.print(":+1:")
