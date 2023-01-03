from pathlib import Path
from typing import Any, Callable, TypeAlias, TypeVar

import click
from click_option_group import MutuallyExclusiveOptionGroup, optgroup
from click_params import StringListParamType

from esgpull.cli.utils import EnumParam, SliceParam
from esgpull.models import FileStatus, Option
from esgpull.tui import Verbosity

F = TypeVar("F", bound=Callable[..., Any])
Dec: TypeAlias = Callable[[F], F]


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
    # date: Dec = click.option(
    #     "--date",
    #     is_flag=True,
    #     default=False,
    # )
    data_node: Dec = click.option(
        "--data-node",
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
    hints: Dec = click.option(
        "--hints",
        "-H",
        type=StringListParamType(","),
        default=None,
    )
    json: Dec = click.option(
        "--json",
        is_flag=True,
        default=False,
    )
    selection_file: Dec = click.option(
        "--selection-file",
        "-s",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=None,
    )
    show: Dec = click.option(
        "--show",
        is_flag=True,
        default=False,
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


# Display group

_display: Dec = optgroup.group(
    "Display options",
    cls=MutuallyExclusiveOptionGroup,
)
_slice: Dec = optgroup.option(
    "slice_",
    "--slice",
    "-S",
    type=SliceParam(),
    default="0:20",
)
_zero: Dec = optgroup.option(
    "--zero",
    "-0",
    is_flag=True,
    default=False,
)
_one: Dec = optgroup.option(
    "--one",
    "-1",
    is_flag=True,
    default=False,
)
_all: Dec = optgroup.option(
    "all_",
    "--all",
    "-a",
    is_flag=True,
    default=False,
)

# Query options group

OptionChoice = click.Choice([opt.name for opt in list(Option)[:-1]])
_query: Dec = optgroup.group("Query options")
_tag: Dec = optgroup.option("tags", "--tag", "-t", multiple=True)
_require: Dec = optgroup.option("--require", "-r")
_distrib: Dec = optgroup.option(
    "--distrib",
    "-d",
    type=OptionChoice,
    # default="notset",
)
_latest: Dec = optgroup.option(
    "--latest",
    # "-l",
    type=OptionChoice,
    # default="notset",
)
_replica: Dec = optgroup.option(
    "--replica",
    type=OptionChoice,
    # default="notset",
)
_retracted: Dec = optgroup.option(
    "--retracted",
    type=OptionChoice,
    # default="notset",
)
# _since: Dec


class groups:
    display: Dec = compose(
        _display,
        _slice,
        _zero,
        _one,
        _all,
    )
    query: Dec = compose(
        _query,
        _tag,
        _require,
        _distrib,
        _latest,
        _replica,
        _retracted,
    )
