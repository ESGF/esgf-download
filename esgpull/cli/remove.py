from typing import Optional

import click

from esgpull import Esgpull
from esgpull.query import Query
from esgpull.types import FileStatus
from esgpull.cli.utils import load_facets
from esgpull.cli.decorators import args, opts


@click.command()
@opts.force
@opts.selection_file
@opts.status
@args.facets
def remove(
    facets: list[str],
    force: bool,
    selection_file: Optional[str],
    status: Optional[FileStatus],
):
    esg = Esgpull()
    query = Query()
    load_facets(query, facets, selection_file)
    files = esg.db.search(query=query, status=status)
    if files:
        click.echo(f"Found {len(files)} files to remove.")
        if not force:
            click.confirm("Continue?", default=True, abort=True)
        removed = esg.remove(*files)
        click.echo(f"Removed {len(removed)} files.")
    else:
        click.echo("No matching file found.")
