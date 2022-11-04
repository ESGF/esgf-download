import click

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.db.models import Param
from esgpull.tui import Verbosity


@click.command()
@args.key
@opts.verbosity
def facet(
    key: str | None,
    verbosity: Verbosity,
):
    esg = Esgpull.with_verbosity(verbosity)
    with esg.ui.logging("facet"):
        if key is None:
            with esg.db.select(Param.name) as stmt:
                params = stmt.distinct().scalars
        else:
            with esg.db.select(Param.value) as stmt:
                params = stmt.where(Param.name == key).scalars
        esg.ui.print(sorted(params))
