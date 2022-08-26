from __future__ import annotations
from typing import Optional

import time
import click


@click.command()
@click.option("--force", "-f", is_flag=True, default=False)
@click.option("--selection-file", "-s")
@click.option("--dry-run", "-z", is_flag=True)
def install(force: bool, selection_file: Optional[str], dry_run: bool) -> None:
    if not force:
        time.sleep(1)
        click.confirm("Are you sure?", default=True, abort=True)
