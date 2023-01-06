from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import get_queries
from esgpull.tui import Verbosity


@click.command()
@args.sha_or_name
@opts.tag
@groups.show
# @opts.dump
# @opts.json
@opts.verbosity
def show(
    sha_or_name: str | None,
    tag: str | None,
    children: bool,
    parents: bool,
    expand: bool,
    # dump: bool,
    # json: bool,
    verbosity: Verbosity,
) -> None:
    """
    Show recorded query data
    """
    esg = Esgpull.with_verbosity(verbosity)
    with esg.ui.logging("show", onraise=Abort):
        if expand and sha_or_name is not None:
            esg.ui.print(esg.graph.expand(sha_or_name))
            raise Exit(0)
        if sha_or_name is None and tag is None:
            esg.graph.load_db()
            graph = esg.graph
        else:
            queries, err_msg = get_queries(esg.graph, sha_or_name, tag)
            if err_msg:
                esg.ui.print(err_msg)
                raise Exit(1)
            graph = esg.graph.subgraph(
                *queries,
                kids=children,
                parents=parents,
            )
        esg.ui.print(graph)
