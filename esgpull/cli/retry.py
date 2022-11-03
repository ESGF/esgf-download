from collections import Counter
from typing import Sequence

import click
import rich
from click.exceptions import Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.db.models import FileStatus


@click.command()
@args.status
@opts.all
def retry(status: Sequence[FileStatus], all_: bool):
    if all_:
        status = FileStatus.retryable()
    if not status:
        status = [FileStatus.Error, FileStatus.Cancelled]
    esg = Esgpull()
    assert FileStatus.Done not in status
    assert FileStatus.Queued not in status
    files = esg.db.search(statuses=status)
    status_str = "/".join(f"[bold red]{s.value}[/]" for s in status)
    if not files:
        rich.print(f"No {status_str} files found.")
        raise Exit(0)
    counts = Counter(file.status for file in files)
    for file in files:
        file.status = FileStatus.Queued
    esg.db.add(*files)
    msg = "Sent back to the queue: "
    msg += ", ".join(
        f"{count} [bold red]{status}[/]" for status, count in counts.items()
    )
    rich.print(msg)
