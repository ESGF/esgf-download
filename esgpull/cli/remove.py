import rich
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
    selection_file: str | None,
    status: list[FileStatus] | None,
):
    esg = Esgpull()
    query = Query()
    load_facets(query, facets, selection_file)
    if not query.dump() and not status:
        raise click.UsageError("No search terms or status provided.")
    files = esg.db.search(query=query, statuses=status)
    if not files:
        rich.print("No matching file found.")
        raise click.exceptions.Exit(0)
    rich.print(f"Found {len(files)} files to remove.")
    if not force:
        click.confirm("Continue?", default=True, abort=True)
    removed = esg.remove(*files)
    rich.print(f"Removed {len(removed)} files.")
