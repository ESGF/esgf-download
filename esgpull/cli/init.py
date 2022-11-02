import click
import rich
from rich.status import Status

from esgpull import Esgpull
from esgpull.utils import Root


@click.command()
def init():
    root = Root.get(mkdir=True)
    esg = Esgpull(root)
    with Status("Fetching params", spinner="earth"):
        if esg.fetch_params(update=False):
            rich.print("Params are initialised.")
        else:
            rich.print("Params already initialised.")
