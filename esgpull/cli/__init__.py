#!/usr/bin/env python3

# https://click.palletsprojects.com/en/latest/
import click

from esgpull import __version__
from esgpull.cli.autoremove import autoremove
from esgpull.cli.download import download
from esgpull.cli.get import get
from esgpull.cli.login import login
from esgpull.cli.install import install
from esgpull.cli.param import param
from esgpull.cli.remove import remove
from esgpull.cli.retry import retry
from esgpull.cli.upgrade import upgrade

# TODO: add metric

__all__ = ["cli"]

SUBCOMMANDS: list[click.Command] = [
    autoremove,
    download,
    get,
    install,
    login,
    param,
    remove,
    retry,
    upgrade,
]

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(__version__, "--version", "-v")
def cli():
    ...


for subcmd in SUBCOMMANDS:
    cli.add_command(subcmd)
