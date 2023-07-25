import asyncio
import sys

import click
import rich
from click.exceptions import Abort, Exit

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.models import File, FileStatus
from esgpull.tui import Verbosity, logger
from esgpull.utils import format_size


@click.command()
@args.query_id
@opts.tag
@opts.disable_ssl
@opts.quiet
@opts.record
@opts.verbosity
def download(
    query_id: str | None,
    tag: str | None,
    disable_ssl: bool,
    quiet: bool,
    record: bool,
    verbosity: Verbosity,
):
    """
    Asynchronously download files linked to queries
    """
    esg = init_esgpull(verbosity, record=record)
    if disable_ssl:
        esg.config.download.disable_ssl = True
    with esg.ui.logging("download", onraise=Abort):
        if not valid_name_tag(esg.graph, esg.ui, query_id, tag):
            esg.ui.raise_maybe_record(Exit(1))
        if query_id is None and tag is None:
            esg.graph.load_db()
            graph = esg.graph
        else:
            queries = get_queries(esg.graph, query_id, tag)
            graph = esg.graph.subgraph(
                *queries,
                children=True,
                parents=True,
            )
        esg.ui.print(graph)
        shas: set[str] = set()
        queue: list[File] = []
        for query in graph.queries.values():
            for file in query.files:
                if file.status == FileStatus.Queued and file.sha not in shas:
                    shas.add(file.sha)
                    queue.append(file)
        if not queue:
            rich.print("Download queue is empty.")
            esg.ui.raise_maybe_record(Exit(0))
        coro = esg.download(queue, show_progress=not quiet)
        files, errors = asyncio.run(coro)
        if files:
            size = format_size(sum(file.size for file in files))
            esg.ui.print(
                f"Downloaded {len(files)} new files for a total size of {size}"
            )
        if errors:
            logger.error(f"{len(errors)} files could not be installed.")
            exc_group = BaseExceptionGroup("Download", [e.err for e in errors])
            esg.ui.raise_maybe_record(exc_group)
        esg.ui.raise_maybe_record(Exit(0))
