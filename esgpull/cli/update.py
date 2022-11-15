from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import load_facets
from esgpull.query import Query
from esgpull.tui import Verbosity
from esgpull.utils import format_size


@click.command()
@opts.distrib
@opts.dry_run
@opts.force
@opts.replica
@opts.since
@opts.selection_file
@opts.verbosity
@args.facets
def update(
    facets: list[str],
    distrib: bool,
    dry_run: bool,
    force: bool,
    replica: bool | None,
    selection_file: str | None,
    since: str | None,
    verbosity: Verbosity,
) -> None:
    esg = Esgpull.with_verbosity(verbosity)
    with esg.ui.logging("update", onraise=Abort):
        query = Query()
        load_facets(query, facets, selection_file)
        if not query.dump():
            esg.ui.print("No search terms provided, this might take a while.")
        with esg.ui.spinner("Searching for outdated files"):
            files = esg.fetch_updated_files(
                query=query,
                distrib=distrib,
                replica=replica,
                since=since,
            )
        if files is None:
            esg.ui.print("No files found.")
            raise Exit(0)
        elif len(files) == 0:
            esg.ui.print("All files are up to update.")
            raise Exit(0)
        esg.ui.print(f"Found {len(files)} files to update.")
        if dry_run:
            esg.ui.print(files)
            raise Exit(0)
        total_size = sum([file.size for file in files])
        esg.ui.print(f"Total size: {format_size(total_size)}")
        if not force:
            click.confirm("Continue?", default=True, abort=True)
        installed = esg.install(*files)
        esg.ui.print(f"Installed {len(installed)} new files.")
