from typing import cast

import rich
import click

import asyncio
from collections import Counter
from sqlalchemy.orm.attributes import InstrumentedAttribute

from esgpull import Esgpull
from esgpull.types import File, FileStatus
from esgpull.utils import naturalsize
from esgpull.cli.utils import print_errors


@click.group()
def download():
    ...


@download.command()
def start():
    esg = Esgpull()
    coro = esg.download_queued(use_bar=True)
    files, errors = asyncio.run(coro)
    if files:
        size = naturalsize(sum(file.size for file in files))
        click.echo(
            f"Downloaded {len(files)} new files for a total size of {size}"
        )
    if errors:
        print_errors(errors)
        click.echo(f"{len(errors)} files could not be installed.")


@download.command()
@click.option("--all", "-a", is_flag=True, default=False)
def queue(all: bool):
    esg = Esgpull()
    statuses = set(FileStatus)
    if not all:
        statuses.remove(FileStatus.done)
    status_attr = cast(InstrumentedAttribute, File.status)
    with esg.db.select(status_attr) as stmt:
        counts = Counter(stmt.where(status_attr.in_(statuses)).scalars)
    if not counts:
        click.echo("Queue is empty")
    table = rich.table.Table()
    table.add_column("status")
    table.add_column("#")
    for status, count in counts.items():
        table.add_row(status.name, str(count))
    rich.print(table)
