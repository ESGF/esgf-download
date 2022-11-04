import click
import rich

from esgpull import Esgpull
from esgpull.cli.decorators import args
from esgpull.db.models import Param


@click.command()
@args.key
def facet(
    key: str | None,
):
    esg = Esgpull()
    if key is None:
        with esg.db.select(Param.name) as stmt:
            params = stmt.distinct().scalars
    else:
        with esg.db.select(Param.value) as stmt:
            params = stmt.where(Param.name == key).scalars
    rich.print(sorted(params))
