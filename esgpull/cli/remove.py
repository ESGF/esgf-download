import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import load_facets
from esgpull.db.models import FileStatus
from esgpull.query import Query
from esgpull.tui import Verbosity


@click.command()
@args.facets
@opts.force
@opts.selection_file
@opts.status
@opts.verbosity
def remove(
    facets: list[str],
    force: bool,
    selection_file: str | None,
    status: list[FileStatus] | None,
    verbosity: Verbosity,
):
    esg = Esgpull.with_verbosity(verbosity)
    with esg.ui.logging("remove", onraise=Abort):
        query = Query()
        load_facets(query, facets, selection_file)
        if not query.dump() and not status:
            raise click.UsageError("No search terms or status provided.")
        files = esg.db.search(query=query, statuses=status)
        if not files:
            esg.ui.print("No matching file found.")
            raise Exit(0)
        esg.ui.print(f"Found {len(files)} files to remove.")
        if not force:
            click.confirm("Continue?", default=True, abort=True)
        removed = esg.remove(*files)
        esg.ui.print(f"Removed {len(removed)} files.")
