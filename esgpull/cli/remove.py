from typing import Optional

import rich
import click

from esgpull import Esgpull
from esgpull.query import Query
from esgpull.cli.utils import load_facets
from esgpull.cli.decorators import args, opts


@click.command()
@opts.force
@opts.selection_file
@args.facets
def remove(
    facets: list[str],
    force: bool,
    selection_file: Optional[str],
):
    esg = Esgpull()
    query = Query()
    load_facets(query, facets, selection_file)
    files = esg.db.search(query)
    click.echo(f"Found {len(files)} files to remove.")
    if not force:
        click.confirm("Continue?", default=True, abort=True)
    removed = esg.remove(*files)
    click.echo(f"Removed {len(removed)} files.")
