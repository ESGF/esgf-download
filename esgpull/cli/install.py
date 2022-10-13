from __future__ import annotations

import rich
import click
from rich.filesize import decimal

from esgpull import Esgpull, Context
from esgpull.types import File
from esgpull.cli.utils import load_facets
from esgpull.cli.decorators import args, opts


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
) -> None:
    esg = Esgpull()
    ctx = Context(distrib=distrib, latest=latest, since=since, replica=replica)
    load_facets(ctx.query, facets, selection_file)
    if not ctx.query.dump():
        raise click.UsageError("No search terms provided.")
    hits = ctx.file_hits
    nb_files = sum(hits)
    if dry_run:
        queries = ctx._build_queries_search(
            hits, file=True, max_results=nb_files, offset=0
        )
        rich.print(queries)
    else:
        rich.print(f"Found {nb_files} files.")
        if not force and nb_files > 5000:
            nb_req = nb_files // 50
            message = f"{nb_req} requests will be send to ESGF. Continue?"
            click.confirm(message, default=True, abort=True)
        if nb_files > 500 and distrib:
            # Enable better distrib
            ctx.index_nodes = esg.fetch_index_nodes()
        results = ctx.search(file=True, max_results=None, offset=0)
        files = [File.from_dict(result) for result in results]
        total_size = sum([file.size for file in files])
        rich.print(f"Total size: {decimal(total_size)}")
        if not force:
            click.confirm("Continue?", default=True, abort=True)
        installed = esg.install(*files)
        rich.print(f"Installed {len(installed)} new files.")
