from __future__ import annotations

import click
from click.exceptions import Abort, BadArgumentUsage, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import get_queries, valid_name_tag
from esgpull.tui import TempUI, Verbosity


@click.command()
@args.sha_or_name
@opts.tag
@groups.show
@opts.files
@opts.json
@opts.yaml
@opts.shas
@opts.verbosity
def show(
    sha_or_name: str | None,
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
    Show recorded query data
    """
    with TempUI.logging():
        esg = Esgpull(verbosity=verbosity, safe=True)
    with esg.ui.logging("show", onraise=Abort):
        if not valid_name_tag(esg.graph, esg.ui, sha_or_name, tag):
            raise Exit(1)
        if expand and sha_or_name is not None:
            esg.ui.print(esg.graph.expand(sha_or_name))
            raise Exit(0)
        if sha_or_name is None and tag is None:
            esg.graph.load_db()
            graph = esg.graph
        else:
            queries = get_queries(esg.graph, sha_or_name, tag)
            graph = esg.graph.subgraph(
                *queries,
                children=children,
                parents=parents,
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
