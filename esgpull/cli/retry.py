import click

from esgpull import Esgpull
from esgpull.types import FileStatus
from esgpull.cli.decorators import args


@click.command()
@args.status
def retry(status: FileStatus):
    esg = Esgpull()
    assert status not in {FileStatus.queued, FileStatus.done}
    files = esg.db.search(status=status)
    if files:
        for file in files:
            file.status = FileStatus.queued
        esg.db.add(*files)
        click.echo(
            f"{len(files)} files with [{status.name}] "
            "status have been put back to the queue."
        )
    else:
        click.echo(f"Found no files with [{status.name}] status.")
