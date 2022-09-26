import click

from esgpull import Esgpull
from esgpull.types import FileStatus


@click.command()
def retry():
    esg = Esgpull()
    files = esg.db.get_files_with_status(FileStatus.error)
    if files:
        for file in files:
            file.status = FileStatus.queued
        esg.db.add(*files)
        click.echo(f"{len(files)} files back in the queue.")
    else:
        click.echo("Found no files with error status.")
