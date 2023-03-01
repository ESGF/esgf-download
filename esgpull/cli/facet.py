import click
from click.exceptions import Abort

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import init_esgpull
from esgpull.models import sql
from esgpull.tui import Verbosity


@click.command()
@args.key
@opts.verbosity
def facet(
    key: str | None,
    verbosity: Verbosity,
):
    esg = init_esgpull(verbosity)
    with esg.ui.logging("facet", onraise=Abort):
        if key is None:
            results = esg.db.scalars(sql.facet.names())
        else:
            results = esg.db.scalars(sql.facet.values(key))
        esg.ui.print(sorted(results))
