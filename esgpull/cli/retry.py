from collections import Counter
from typing import Sequence

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.db.models import FileStatus
from esgpull.tui import Verbosity


@click.command()
@args.status
@opts.all
@opts.verbosity
def retry(
    status: Sequence[FileStatus],
    all_: bool,
    verbosity: Verbosity,
):
    if all_:
        status = FileStatus.retryable()
    if not status:
        status = [FileStatus.Error, FileStatus.Cancelled]
    esg = Esgpull.with_verbosity(verbosity)
    with esg.ui.logging("retry", onraise=Abort):
        assert FileStatus.Done not in status
        assert FileStatus.Queued not in status
        files = esg.db.search(statuses=status)
        status_str = "/".join(f"[bold red]{s.value}[/]" for s in status)
        if not files:
            esg.ui.print(f"No {status_str} files found.")
            raise Exit(0)
        counts = Counter(file.status for file in files)
        for file in files:
            file.status = FileStatus.Queued
        esg.db.add(*files)
        msg = "Sent back to the queue: "
        msg += ", ".join(
            f"{count} [bold red]{status.value}[/]"
            for status, count in counts.items()
        )
        esg.ui.print(msg)
