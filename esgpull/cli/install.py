from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import load_facets
from esgpull.db.models import File
from esgpull.tui import Verbosity
from esgpull.utils import format_size


@click.command()
@opts.distrib
@opts.dry_run
@opts.date
@opts.force
@opts.since
@opts.latest
@opts.data_node
@opts.replica
@opts.selection_file
@opts.verbosity
@args.facets
def install(
    facets: list[str],
    date: bool,
    data_node: bool,
    distrib: bool,
    dry_run: bool,
    force: bool,
    latest: bool | None,
    replica: bool | None,
    selection_file: str | None,
    since: str | None,
    verbosity: Verbosity,
) -> None:
    esg = Esgpull.with_verbosity(verbosity)
    with esg.context() as ctx, esg.ui.logging("install", onraise=Abort):
        ctx.distrib = distrib
        ctx.latest = latest
        ctx.since = since
        ctx.replica = replica
        load_facets(ctx.query, facets, selection_file)
        if not ctx.query.dump():
            raise click.UsageError("No search terms provided.")
        hits = ctx.file_hits
        nb_files = sum(hits)
        esg.ui.print(f"Found {nb_files} files.")
        if nb_files > 500 and distrib:
            # Enable better distrib
            ctx.index_nodes = esg.fetch_index_nodes()
        if dry_run:
            queries = ctx._build_queries_search(
                hits, file=True, max_results=nb_files, offset=0
            )
            esg.ui.print(queries)
            raise Exit(0)
        if not force and nb_files > 5000:
            nb_req = nb_files // 50
            message = f"{nb_req} requests will be send to ESGF. Continue?"
            click.confirm(message, default=True, abort=True)
        results = ctx.search(
            file=True,
            max_results=None,
            offset=0,
            hits=hits,
        )
        files = [File.from_dict(result) for result in results]
        total_size = sum([file.size for file in files])
        esg.ui.print(f"Total size: {format_size(total_size)}")
        if not force:
            click.confirm("Continue?", default=True, abort=True)
        installed = esg.install(*files)
        esg.ui.print(f"Installed {len(installed)} new files.")
