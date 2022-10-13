from collections import Counter
from typing import Collection

import click
import rich

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.types import FileStatus


@click.command()
@args.status
@opts.all
def retry(status: Collection[FileStatus], all_: bool):
    if all_:
        status = set(FileStatus) - {FileStatus.done, FileStatus.queued}
    if not status:
        status = [FileStatus.error, FileStatus.cancelled]
    esg = Esgpull()
    assert FileStatus.done not in status
    assert FileStatus.queued not in status
    files = esg.db.search(statuses=status)
    status_str = "/".join(f"[bold red]{s.name}[/]" for s in status)
    if not files:
        rich.print(f"No {status_str} files found.")
        raise click.exceptions.Exit(0)
    counts = Counter(file.status for file in files)
    for file in files:
        file.status = FileStatus.queued
    esg.db.add(*files)
    msg = "Sent back to the queue: "
    msg += ", ".join(
        f"{count} [bold red]{status}[/]" for status, count in counts.items()
    )
    rich.print(msg)
