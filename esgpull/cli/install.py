from __future__ import annotations
from typing import Optional

import time
import click


@click.command()
@click.option("--yes", "-y", is_flag=True, default=False)
@click.option("--selection-file", "-s")
@click.option("--dry-run", "-z", is_flag=True)
def install(yes: bool, selection_file: Optional[str], dry_run: bool) -> None:
    if not yes:
        time.sleep(1)
        click.confirm("Are you sure?", default=True, abort=True)
