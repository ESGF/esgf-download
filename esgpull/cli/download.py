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
@args.sha_or_name
@opts.tag
@opts.quiet
@opts.record
@opts.verbosity
def download(
    sha_or_name: str | None,
    tag: str | None,
    quiet: bool,
    record: bool,
    verbosity: Verbosity,
):
    esg = init_esgpull(verbosity, record=record)
    with esg.ui.logging("download", onraise=Abort):
        if not valid_name_tag(esg.graph, esg.ui, sha_or_name, tag):
            esg.ui.raise_maybe_record(Exit(1))
        if sha_or_name is None and tag is None:
            esg.graph.load_db()
            graph = esg.graph
        else:
            queries = get_queries(esg.graph, sha_or_name, tag)
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
