from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.graph import Graph
from esgpull.models import FileStatus
from esgpull.tui import Verbosity
from esgpull.utils import format_size


@click.command()
@args.query_id
@opts.tag
@opts.children
@opts.verbosity
def remove(
    query_id: str | None,
    tag: str | None,
    children: bool,
    verbosity: Verbosity,
) -> None:
    """
    Remove queries from the database

    A query required by other queries cannot be removed.

    Upon removal, no files are deleted from the database nor from the filesystem, only links to that query are deleted.

    There is currently no mechanism to delete "orphaned" files, as this functionality should be designed carefully and rooted in practical needs.
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("remove", onraise=Abort):
        if query_id is None and tag is None:
            raise click.UsageError("No query or tag provided.")
        if not valid_name_tag(esg.graph, esg.ui, query_id, tag):
            raise Exit(1)
        queries = get_queries(
            esg.graph,
            query_id,
            tag,
            children=children,
        )
        nb = len(queries)
        ies = "ies" if nb > 1 else "y"
        graph = Graph(None)
        graph.add(*queries)
        esg.ui.print(graph)
        msg = f"Remove {nb} quer{ies}?"
        if not esg.ui.ask(msg, default=True):
            raise Abort
        for query in queries:
            if query.has_files:
                nb, size = query.files_count_size(FileStatus.Done)
                if nb:
                    esg.ui.print(
                        f":stop_sign: {query.rich_name} is linked"
                        f" to {nb} downloaded files ({format_size(size)})."
                    )
                    if not esg.ui.ask("Delete anyway?", default=False):
                        raise Abort
            if not children and esg.graph.get_children(query.sha):
                esg.ui.print(
                    ":stop_sign: Some queries block"
                    f" removal of {query.rich_name}."
                )
                if esg.ui.ask("Show blocking queries?", default=False):
                    esg.ui.print(esg.graph.subgraph(query, children=True))
                raise Exit(1)
        esg.db.delete(*queries)
        esg.ui.print(":+1:")
