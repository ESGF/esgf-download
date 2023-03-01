from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import init_esgpull, valid_name_tag
from esgpull.tui import Verbosity


@click.command()
@args.multi_sha_or_name
@opts.verbosity
def track(
    sha_or_name: tuple[str],
    verbosity: Verbosity,
) -> None:
    """
    Remove queries
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("track", onraise=Abort):
        for sha in sha_or_name:
            if not valid_name_tag(esg.graph, esg.ui, sha, None):
                raise Exit(1)
            query = esg.graph.get(sha)
            if query.tracked:
                esg.ui.print(f"Query {query.rich_name} is already tracked.")
                raise Exit(0)
            if esg.graph.get_children(query.sha):
                msg = "Query has children, track anyway?"
                if not esg.ui.ask(msg, default=False):
                    raise Abort
            query.tracked = True
            esg.graph.merge(commit=True)
            esg.ui.print(f":+1: Query {query.rich_name} is now tracked.")


@click.command()
@args.multi_sha_or_name
@opts.verbosity
def untrack(
    sha_or_name: tuple[str],
    verbosity: Verbosity,
) -> None:
    """
    Remove queries
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("track", onraise=Abort):
        for sha in sha_or_name:
            if not valid_name_tag(esg.graph, esg.ui, sha, None):
                raise Exit(1)
            query = esg.graph.get(sha)
            if not query.tracked:
                esg.ui.print(f"Query {query.rich_name} is already untracked.")
                raise Exit(0)
            query.tracked = False
            esg.graph.merge(commit=True)
            esg.ui.print(f":+1: Query {query.rich_name} is no longer tracked.")
