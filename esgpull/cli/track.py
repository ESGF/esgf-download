from __future__ import annotations

import click
from click.exceptions import Abort, Exit
from rich.box import MINIMAL_DOUBLE_HEAD
from rich.table import Table
from rich.text import Text

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import init_esgpull, valid_name_tag
from esgpull.tui import Verbosity


@click.command()
@args.query_ids
@opts.record
@opts.verbosity
def track(
    query_ids: tuple[str],
    record: bool,
    verbosity: Verbosity,
) -> None:
    """
    Track queries

    As a side effect, tracking a query applies all default options to the query,
    so that modifications of the config's default options have no impact on
    previouly tracked queries.
    """
    esg = init_esgpull(verbosity, record=record)
    with esg.ui.logging("track", onraise=Abort):
        for sha in query_ids:
            if not valid_name_tag(esg.graph, esg.ui, sha, None):
                esg.ui.raise_maybe_record(Exit(1))
            query = esg.graph.get(sha)
            if query.tracked:
                esg.ui.print(f"{query.rich_name} is already tracked.")
                esg.ui.raise_maybe_record(Exit(0))
            if esg.graph.get_children(query.sha):
                msg = f"{query.rich_name} has children, track anyway?"
                if not esg.ui.ask(msg, default=False):
                    esg.ui.raise_maybe_record(Abort)
            expanded = esg.graph.expand(query.sha)
            tracked_query = query.clone(compute_sha=False)
            tracked_query.track(expanded.options)
            if query.sha != tracked_query.sha:
                msg = f"For {query.rich_name} to become tracked, options must be set."
                esg.ui.print(msg)
                table = Table(
                    box=MINIMAL_DOUBLE_HEAD,
                    show_edge=False,
                    show_lines=True,
                )
                table.add_column(Text("before", justify="center"))
                table.add_column(Text("after", justify="center"))
                table.add_row(query, tracked_query)
                esg.ui.print(table)
                if not esg.ui.ask("Apply changes?"):
                    esg.ui.raise_maybe_record(Abort)
            esg.graph.replace(query, tracked_query)
            esg.graph.merge()
            esg.ui.print(f":+1: {tracked_query.rich_name} is now tracked.")
        esg.ui.raise_maybe_record(Exit(0))


@click.command()
@args.query_ids
@opts.verbosity
def untrack(
    query_ids: tuple[str],
    verbosity: Verbosity,
) -> None:
    """
    Untrack queries
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("untrack", onraise=Abort):
        for sha in query_ids:
            if not valid_name_tag(esg.graph, esg.ui, sha, None):
                raise Exit(1)
            query = esg.graph.get(sha)
            if not query.tracked:
                esg.ui.print(f"Query {query.rich_name} is already untracked.")
                raise Exit(0)
            query.untrack()
            esg.graph.merge()
            esg.ui.print(f":+1: Query {query.rich_name} is no longer tracked.")
