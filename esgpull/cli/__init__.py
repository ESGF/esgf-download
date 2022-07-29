#!/usr/bin/env python3

# https://click.palletsprojects.com/en/latest/
import click

from esgpull import __version__
from esgpull.cli.autoremove import autoremove
from esgpull.cli.config import config
from esgpull.cli.download import download
from esgpull.cli.get import get
from esgpull.cli.login import login
from esgpull.cli.install import install
from esgpull.cli.param import param
from esgpull.cli.remove import remove
from esgpull.cli.retry import retry
from esgpull.cli.search import search
from esgpull.cli.upgrade import upgrade

# [-]TODO: stats
#   - speed per index/data node
#   - total disk usage
#   - per-setting stats for optimisation purpose ?

__all__ = ["cli"]

SUBCOMMANDS: list[click.Command] = [
    autoremove,
    config,
    download,
    get,
    install,
    login,
    param,
    remove,
    retry,
    search,
    # stats,
    upgrade,
]

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(__version__, "--version", "-v")
def cli():
    ...


for subcmd in SUBCOMMANDS:
    cli.add_command(subcmd)


def main():
    cli()
