import rich
import click
from rich.filesize import decimal

import asyncio

from esgpull import Esgpull
from esgpull.types import FileStatus

# from esgpull.cli.utils import print_errors
from esgpull.cli.decorators import opts


@click.command()
@opts.quiet
def download(quiet: bool):
    esg = Esgpull()
    if quiet:
        progress_level = 0
    else:
        progress_level = 1
    queue = esg.db.search(statuses=[FileStatus.queued])
    if not queue:
        rich.print("Download queue is empty.")
        raise click.exceptions.Exit(0)
    coro = esg.download(queue, progress_level=progress_level)
    files, errors = asyncio.run(coro)
    if files:
        size = decimal(sum(file.size for file in files))
        rich.print(
            f"Downloaded {len(files)} new files for a total size of {size}"
        )
    if errors:
        for error in errors:
            rich.print(error.err)
        rich.print(f"{len(errors)} files could not be installed.")
        raise click.exceptions.Exit(1)
