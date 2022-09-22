from __future__ import annotations
from typing import Optional

import time
import click
from esgpull.cli.decorators import args, opts


@click.command()
@opts.distrib
@opts.dry_run
@opts.force
@args.facets
@opts.selection_file
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
