from pathlib import Path
from typing import Callable, TypeAlias

import click
from click_option_group import MutuallyExclusiveOptionGroup, optgroup
from click_params import StringListParamType

from esgpull.cli.utils import EnumParam, SliceParam
from esgpull.db.models import FileStatus
from esgpull.tui import Verbosity

Dec: TypeAlias = Callable[[Callable], Callable]


def compose(*decs: Dec) -> Dec:
    def deco(fn: Callable):
        for dec in reversed(decs):
            fn = dec(fn)
        return fn

    return deco


class args:
    facets: Dec = click.argument(
        "facets",
        nargs=-1,
    )
    key: Dec = click.argument(
        "key",
        type=str,
        nargs=1,
        required=False,
        default=None,
    )
    value: Dec = click.argument(
        "value",
        type=str,
        nargs=1,
        required=False,
        default=None,
    )
    status: Dec = click.argument(
        "status",
        type=EnumParam(FileStatus),
        nargs=-1,
    )


class opts:
    all: Dec = click.option(
        "all_",
        "--all",
        "-a",
        is_flag=True,
        default=False,
    )
    date: Dec = click.option(
        "--date",
        is_flag=True,
        default=False,
    )
    data_node: Dec = click.option(
        "--data-node",
        "-n",
        is_flag=True,
        default=False,
    )
    distrib: Dec = click.option(
        "--distrib",
        "-d",
        is_flag=True,
        default=False,
    )
    dry_run: Dec = click.option(
        "--dry-run",
        "-z",
        is_flag=True,
        default=False,
    )
    dump: Dec = click.option(
        "--dump",
        "-D",
        is_flag=True,
        default=False,
    )
    file: Dec = click.option(
        "--file",
        "-F",
        is_flag=True,
    )
    force: Dec = click.option(
        "--force",
        "-f",
        is_flag=True,
        default=False,
    )
    json: Dec = click.option(
        "--json",
        is_flag=True,
        default=False,
    )
    latest: Dec = click.option(
        "--latest/--no-latest",
        " /-L",
        is_flag=True,
        default=True,
        show_default=True,
    )
    options: Dec = click.option(
        "--options",
        "-o",
        type=StringListParamType(","),
        default=None,
    )
    quiet: Dec = click.option(
        "--quiet",
        "-q",
        is_flag=True,
        default=False,
    )
    replica: Dec = click.option(
        "--replica/--no-replica",
        "-r/-R",
        is_flag=True,
        default=None,
    )
    selection_file: Dec = click.option(
        "--selection-file",
        "-s",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=None,
    )
    since: Dec = click.option(
        "--since",
        type=str,
        default=None,
    )
    status: Dec = click.option(
        "--status",
        type=EnumParam(FileStatus),
        default=None,
        multiple=True,
    )
    verbosity: Dec = click.option(
        "verbosity",
        "-v",
        count=True,
        type=EnumParam(Verbosity),
    )


class groups:
    __display: Dec = optgroup.group(
        "Display options",
        cls=MutuallyExclusiveOptionGroup,
    )
    __slice: Dec = optgroup.option(
        "slice_",
        "--slice",
        "-S",
        type=SliceParam(),
        default="0:20",
    )
    __zero: Dec = optgroup.option(
        "--zero",
        "-0",
        is_flag=True,
        default=False,
    )
    __one: Dec = optgroup.option(
        "--one",
        "-1",
        is_flag=True,
        default=False,
    )
    __all: Dec = optgroup.option(
        "all_",
        "--all",
        "-a",
        is_flag=True,
        default=False,
    )
    display: Dec = compose(__display, __slice, __zero, __one, __all)
