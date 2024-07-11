#!/usr/bin/env python3

# https://click.palletsprojects.com/en/latest/
import click

from esgpull import __version__
from esgpull.cli.add import add
from esgpull.cli.config import config
from esgpull.cli.convert import convert
from esgpull.cli.datasets import datasets
from esgpull.cli.download import download
from esgpull.cli.login import login
from esgpull.cli.remove import remove
from esgpull.cli.retry import retry
from esgpull.cli.search import search
from esgpull.cli.self import self
from esgpull.cli.show import show
from esgpull.cli.status import status
from esgpull.cli.track import track, untrack
from esgpull.cli.update import update
from esgpull.tui import UI

# from esgpull.cli.autoremove import autoremove
# from esgpull.cli.facet import facet
# from esgpull.cli.get import get
# from esgpull.cli.install import install

# [-]TODO: stats
#   - speed per index/data node
#   - total disk usage
#   - log config for later optimisation ?

SUBCOMMANDS: list[click.Command] = [
    add,
    # autoremove,
    config,
    convert,
    datasets,
    download,
    # facet,
    # get,
    self,
    # install,
    login,
    remove,
    retry,
    search,
    show,
    track,
    untrack,
    status,
    # # stats,
    update,
]

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])

_ui = UI("/tmp")
version_msg = _ui.render(f"esgpull, version [green]{__version__}[/]")


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(None, "-V", "--version", message=version_msg)
def cli():
    """
    esgpull is a management utility for files and datasets from ESGF.
    """


for subcmd in SUBCOMMANDS:
    cli.add_command(subcmd)


def main():
    cli()
