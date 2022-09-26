import click

import asyncio

from esgpull import Esgpull
from esgpull.utils import naturalsize


@click.group()
def download():
    ...


@download.command()
def start():
    esg = Esgpull()
    coro = esg.download_queued(use_bar=True)
    size_install, nok, nerr = asyncio.run(coro)
    size_str = naturalsize(size_install)
    if nok:
        click.echo(f"Installed {nok} new files for a total size of {size_str}")
    if nerr:
        click.echo(f"{nerr} files could not be installed.")
