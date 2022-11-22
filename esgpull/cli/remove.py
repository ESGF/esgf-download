import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import filter_docs, load_facets, totable
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
    status: list[FileStatus],
    verbosity: Verbosity,
):
    esg = Esgpull.with_verbosity(verbosity)
    with esg.ui.logging("remove", onraise=Abort):
        query = Query()
        load_facets(query, facets, selection_file)
        if not query.dump() and not status:
            raise click.UsageError("No search terms or status provided.")
        files = esg.db.search(query=query, statuses=status)
        nb = len(files)
        if not nb:
            esg.ui.print("No matching file found.")
            raise Exit(0)
        if not force:
            docs = filter_docs([file.raw for file in files])
            esg.ui.print(totable(docs))
            s = "s" if nb > 1 else ""
            esg.ui.print(f"Found {nb} file{s} to remove.")
            click.confirm("Continue?", default=True, abort=True)
        removed = esg.remove(*files)
        esg.ui.print(f"Removed {len(removed)} files.")
        nb_remain = len(removed) - nb
        if nb_remain:
            esg.ui.print(f"{nb_remain} files could not be removed.")
        if force:
            docs = filter_docs([file.raw for file in removed])
            esg.ui.print(totable(docs))
