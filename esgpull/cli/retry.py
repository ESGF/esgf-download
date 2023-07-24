from collections import Counter
from collections.abc import Sequence

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import init_esgpull
from esgpull.models import FileStatus, sql
from esgpull.tui import Verbosity


@click.command()
@args.status
@opts.verbosity
def retry(
    status: Sequence[FileStatus],
    verbosity: Verbosity,
):
    """
    Re-queue failed and cancelled downloads
    """
    if not status:
        status = FileStatus.retryable()
    esg = init_esgpull(verbosity)
    with esg.ui.logging("retry", onraise=Abort):
        assert FileStatus.Done not in status
        assert FileStatus.Queued not in status
        files = list(esg.db.scalars(sql.file.with_status(*status)))
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
