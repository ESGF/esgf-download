from typing import Callable, TypeAlias
from pathlib import Path

import click
from click_params import StringListParamType

from esgpull.cli.utils import SliceParam

Dec: TypeAlias = Callable[[Callable], Callable]


class args:
    facets: Dec = click.argument("facets", nargs=-1)


class opts:
    date: Dec = click.option("--date", "-D", is_flag=True, default=False)
    data_node: Dec = click.option(
        "--data-node", "-n", is_flag=True, default=False
    )
    distrib: Dec = click.option("--distrib", "-d", is_flag=True, default=False)
    dry_run: Dec = click.option("--dry-run", "-z", is_flag=True, default=False)
    file: Dec = click.option("--file", "-f", is_flag=True)
    force: Dec = click.option("--force", "-f", is_flag=True, default=False)
    latest: Dec = click.option(
        "--latest/--no-latest", "-l/-L", is_flag=True, default=None
    )
    one: Dec = click.option("--one", "-1", is_flag=True, default=False)
    options: Dec = click.option(
        "--options",
        "-o",
        type=StringListParamType(","),
        default=None,
    )
    replica: Dec = click.option(
        "--replica/--no-replica", "-r/-R", is_flag=True, default=None
    )
    selection_file: Dec = click.option(
        "--selection-file",
        "-s",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=None,
    )
    since: Dec = click.option("--since", type=str, default=None)
    slice: Dec = click.option(
        "slice_", "--slice", "-S", type=SliceParam(), default="0:20"
    )
    zero: Dec = click.option("--zero", "-0", is_flag=True, default=False)
