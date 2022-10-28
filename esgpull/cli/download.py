import asyncio

import click
import rich
from click.exceptions import Exit

from esgpull import Esgpull
from esgpull.cli.decorators import opts
from esgpull.db.models import FileStatus
from esgpull.utils import format_size


@click.command()
@opts.quiet
def download(quiet: bool):
    esg = Esgpull()
    if quiet:
        progress_level = 0
    else:
        progress_level = 1
    queue = esg.db.search(statuses=[FileStatus.Queued])
    if not queue:
        rich.print("Download queue is empty.")
        raise Exit(0)
    coro = esg.download(queue, progress_level=progress_level)
    files, errors = asyncio.run(coro)
    if files:
        size = format_size(sum(file.size for file in files))
        rich.print(
            f"Downloaded {len(files)} new files for a total size of {size}"
        )
    if errors:
        for error in errors:
            rich.print(error.err)
        rich.print(f"{len(errors)} files could not be installed.")
        raise Exit(1)
