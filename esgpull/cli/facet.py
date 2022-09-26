import click
import rich

from esgpull import Esgpull
from esgpull.types import Param


@click.group()
def facet():
    ...


@facet.command()
def init():
    esg = Esgpull()
    if esg.fetch_params(update=False):
        click.echo("Params are initialised.")
    else:
        click.echo("Params already initialised.")


@facet.command()
def update():
    esg = Esgpull()
    esg.fetch_params(update=True)
    click.echo("Params are up to date.")


@facet.command("list")
def list_cmd():
    esg = Esgpull()
    with esg.db.select(Param.name) as stmt:
        params = stmt.distinct().scalars
    rich.print(params)


@facet.command()
@click.argument("name", nargs=1, type=str)
def values(name):
    esg = Esgpull()
    with esg.db.select(Param.value) as stmt:
        params = stmt.where(Param.name == name).scalars
    rich.print(params)
