from __future__ import annotations

import click
from click.exceptions import Abort, BadArgumentUsage, Exit

from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.tui import Verbosity


@click.command()
@args.query_id
@opts.tag
@groups.show
@groups.json_yaml
@opts.files
@opts.shas
@opts.verbosity
def show(
    query_id: str | None,
    tag: str | None,
    children: bool,
    parents: bool,
    expand: bool,
    files: bool,
    json: bool,
    yaml: bool,
    shas: bool,
    verbosity: Verbosity,
) -> None:
    """
    View query tree
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("show", onraise=Abort):
        if not valid_name_tag(esg.graph, esg.ui, query_id, tag):
            raise Exit(1)
        if expand and query_id is not None:
            esg.ui.print(esg.graph.expand(query_id))
            raise Exit(0)
        if query_id is None and tag is None:
            esg.graph.load_db()
            graph = esg.graph
        else:
            queries = get_queries(esg.graph, query_id, tag)
            graph = esg.graph.subgraph(
                *queries,
                children=children,
                parents=parents,
                keep_db=True,
            )
        if tag is not None:
            tag_db = esg.graph.get_tag(tag)
            if tag_db is not None and tag_db.description is not None:
                esg.ui.print(tag_db.description)
        if shas:
            esg.ui.print(list(graph.queries.keys()), json=True)
        if json:
            esg.ui.print(graph.asdict(files=files), json=True)
        elif yaml:
            esg.ui.print(graph.asdict(files=files), yaml=True)
        elif files:
            msg = "--files can only be used with --json or --yaml"
            raise BadArgumentUsage(msg)
        else:
            esg.ui.print(graph)
