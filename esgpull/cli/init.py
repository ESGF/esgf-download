import click
import rich
from rich.status import Status

from esgpull import Esgpull


@click.command()
def init():
    esg = Esgpull()
    with Status("Fetching params", spinner="earth"):
        if esg.fetch_params(update=False):
            rich.print("Params are initialised.")
        else:
            rich.print("Params already initialised.")
