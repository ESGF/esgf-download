import click
from click.exceptions import Abort

from esgpull import Esgpull
from esgpull.cli.decorators import opts
from esgpull.tui import Verbosity
from esgpull.utils import Root


@click.command()
@opts.verbosity
def init(
    verbosity: Verbosity,
):
    root = Root.get(mkdir=True)
    esg = Esgpull.with_verbosity(verbosity, root)
    with esg.ui.logging("init", onraise=Abort):
        with esg.ui.spinner("Fetching params"):
            if esg.fetch_facets(update=False):
                esg.ui.print("Params are initialised.")
            else:
                esg.ui.print("Params already initialised.")
