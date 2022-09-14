from __future__ import annotations
from typing import Optional

import time
import click
from esgpull.cli.utils import arg, opt


@click.command()
@opt.selection_file
@opt.distrib
@opt.dry_run
@click.option("--force", "-f", is_flag=True, default=False)
@arg.facets
def install(
    facets: list[str],
    selection_file: Optional[str],
    force: bool,
    distrib: bool,
    dry_run: bool,
) -> None:
    print(dry_run)
    if not force:
        time.sleep(1)
        click.confirm("Are you sure?", default=True, abort=True)
