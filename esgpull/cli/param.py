import click
import rich

from esgpull import Esgpull
from esgpull.types import Param


@click.group()
def param():
    ...


@param.command()
def init():
    esg = Esgpull()
    if esg.fetch_params(update=False):
        rich.print("Params are initialised.")
    else:
        rich.print("Params already initialised.")


@param.command()
def update():
    esg = Esgpull()
    esg.fetch_params(update=True)
    rich.print("Params are up to date.")


@param.command("list")
def list_cmd():
    esg = Esgpull()
    with esg.db.select(Param.name) as stmt:
        params = stmt.distinct().scalars
    rich.print(params)


@param.command()
@click.argument("name", nargs=1, type=str)
def facet(name):
    esg = Esgpull()
    with esg.db.select(Param.value) as stmt:
        params = stmt.where(Param.name == name).scalars
    rich.print(params)
