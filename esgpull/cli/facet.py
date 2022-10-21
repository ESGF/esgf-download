import click
import rich

from esgpull import Esgpull
from esgpull.db.models import Param


@click.command()
@click.argument("name", type=str, nargs=1, required=False, default=None)
def facet(name: str | None):
    esg = Esgpull()
    if name is None:
        with esg.db.select(Param.name) as stmt:
            params = stmt.distinct().scalars
    else:
        with esg.db.select(Param.value) as stmt:
            params = stmt.where(Param.name == name).scalars
    rich.print(sorted(params))
