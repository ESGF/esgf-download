#!/usr/bin/env python3

# https://click.palletsprojects.com/en/latest/
import click

from esgpull import __version__
from esgpull.cli.autoremove import autoremove
from esgpull.cli.download import download
from esgpull.cli.get import get
from esgpull.cli.login import login
from esgpull.cli.install import install
from esgpull.cli.facet import facet
from esgpull.cli.remove import remove
from esgpull.cli.retry import retry
from esgpull.cli.search import search
from esgpull.cli.settings import settings
from esgpull.cli.upgrade import upgrade

# [-]TODO: stats
#   - speed per index/data node
#   - total disk usage
#   - per-setting stats for optimisation purpose ?

SUBCOMMANDS: list[click.Command] = [
    autoremove,
    settings,
    download,
    facet,
    get,
    install,
    login,
    remove,
    retry,
    search,
    # stats,
    upgrade,
]

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(__version__, "--version", "-V")
def cli():
    """
    esgpull is a management utility for files and datasets from ESGF.
    """


for subcmd in SUBCOMMANDS:
    cli.add_command(subcmd)


def main():
    cli()


__all__ = ["cli"]
