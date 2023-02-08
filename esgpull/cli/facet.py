import click
from click.exceptions import Abort

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.models import sql
from esgpull.tui import TempUI, Verbosity


@click.command()
@args.key
@opts.verbosity
def facet(
    key: str | None,
    verbosity: Verbosity,
):
    with TempUI.logging():
        esg = Esgpull(verbosity=verbosity, safe=True)
    with esg.ui.logging("facet", onraise=Abort):
        if key is None:
            results = esg.db.scalars(sql.facet.names)
        else:
            results = esg.db.scalars(sql.facet.values(key))
        esg.ui.print(sorted(results))
