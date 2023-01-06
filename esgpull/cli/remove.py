from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries
from esgpull.tui import Verbosity


@click.command()
@args.sha_or_name
@opts.tag
# @opts.status
@opts.verbosity
def remove(
    sha_or_name: str | None,
    tag: str | None,
    # status: list[FileStatus],
    verbosity: Verbosity,
) -> None:
    """
    Remove queries
    """
    esg = Esgpull.with_verbosity(verbosity)
    with esg.ui.logging("remove", onraise=Abort):
        if sha_or_name is None and tag is None:
            raise click.UsageError("No query or tag provided.")
        else:
            queries, err_msg = get_queries(esg.graph, sha_or_name, tag)
            if err_msg:
                esg.ui.print(err_msg)
                raise Exit(1)
            nb = len(queries)
            ies = "ies" if nb > 1 else "y"
            for query in queries:
                esg.ui.print(query)
            msg = f"Remove {nb} quer{ies}?"
            click.confirm(msg, default=True, abort=True)
            for query in queries:
                kids = esg.graph.get_all_kids(query.sha, esg.graph._shas_)
                nb = len(kids)
                if kids:
                    esg.ui.print(f":stop_sign: {nb} queries block removal.")
                    if click.confirm("Show blocking queries?", default=False):
                        for kid in kids:
                            esg.ui.print(kid)
                    raise Exit(1)
            esg.db.delete(*queries)
            esg.ui.print(":thumbs_up:")
