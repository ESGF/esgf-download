from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeAlias, TypeVar

import click
from click_params import StringListParamType

from esgpull.cli.utils import EnumParam
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
        type=str,
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
    path: Dec = click.argument(
        "path",
        type=click.Path(exists=False, path_type=Path),
        required=False,
        default=None,
    )
    status: Dec = click.argument(
        "status",
        type=EnumParam(FileStatus),
        nargs=-1,
    )
    query_id: Dec = click.argument(
        "query_id",
        type=str,
        nargs=1,
        required=False,
    )
    query_id_required: Dec = click.argument(
        "query_id",
        type=str,
        nargs=1,
        required=True,
    )
    query_ids: Dec = click.argument(
        "query_ids",
        type=str,
        nargs=-1,
        required=True,
    )


class opts:
    all: Dec = click.option(
        "all_",
        "--all",
        "-a",
        is_flag=True,
        default=False,
    )
    children: Dec = click.option(
        "--children",
        "-c",
        is_flag=True,
        default=False,
    )
    default: Dec = click.option(
        "--default",
        "-d",
        is_flag=True,
        default=False,
    )
    detail: Dec = click.option(
        "--detail",
        type=int,
        default=None,
    )
    disable_ssl: Dec = click.option(
        "--disable-ssl",
        is_flag=True,
        default=False,
    )
    dry_run: Dec = click.option(
        "--dry-run",
        "-z",
        is_flag=True,
        default=False,
    )
    facets_hints: Dec = click.option(
        "facets_hints",
        "--facets",
        "-F",
        is_flag=True,
        default=False,
    )
    file: Dec = click.option(
        "--file",
        "-f",
        is_flag=True,
        default=False,
    )
    files: Dec = click.option(
        "--files",
        is_flag=True,
        default=False,
    )
    force: Dec = click.option(
        "--force",
        is_flag=True,
        default=False,
    )
    generate: Dec = click.option(
        "--generate",
        is_flag=True,
        default=False,
    )
    hints: Dec = click.option(
        "--hints",
        "-H",
        type=StringListParamType(","),
        default=None,
    )
    name: Dec = click.option(
        "--name",
        "-n",
        type=str,
        default=None,
    )
    no_default_query: Dec = click.option(
        "--no-default-query",
        is_flag=True,
        default=False,
    )
    query_file: Dec = click.option(
        "--query-file",
        "-q",
        type=click.Path(exists=True, dir_okay=False, path_type=Path),
        default=None,
    )
    quiet: Dec = click.option(
        "--quiet",
        is_flag=True,
        default=False,
    )
    record: Dec = click.option(
        "--record",
        is_flag=True,
        default=False,
    )
    reset: Dec = click.option(
        "--reset",
        is_flag=True,
        default=False,
    )
    shas: Dec = click.option(
        "--shas",
        "-s",
        is_flag=True,
        default=False,
    )
    show: Dec = click.option(
        "--show",
        is_flag=True,
        default=False,
    )
    simple: Dec = click.option(
        "--simple",
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
    tag: Dec = click.option(
        "--tag",
        "-t",
        type=str,
        default=None,
    )
    track: Dec = click.option(
        "--track",
        is_flag=True,
        default=False,
    )
    yes: Dec = click.option(
        "--yes",
        "-y",
        is_flag=True,
        default=False,
    )
    verbosity: Dec = click.option(
        "verbosity",
        "-v",
        count=True,
        type=EnumParam(Verbosity),
    )


# Display group
_page: Dec = click.option(
    "--page",
    "-p",
    type=int,
    default=0,  # 1?
)
_zero: Dec = click.option(
    "--zero",
    "-0",
    is_flag=True,
    default=False,
)
_all: Dec = click.option(
    "_all",
    "--all",
    "-a",
    is_flag=True,
    default=False,
)

# Json/Yaml exclusive group
_json: Dec = click.option(
    "--json",
    is_flag=True,
    default=False,
)
_yaml: Dec = click.option(
    "--yaml",
    is_flag=True,
    default=False,
)

# Query definition group
OptionChoice = click.Choice([opt.name for opt in list(Option)[:-1]])
_tag: Dec = click.option("tags", "--tag", "-t", multiple=True)
_require: Dec = click.option(
    "--require",
    "-r",
    type=str,
    nargs=1,
    required=False,
    default=None,
)
_distrib: Dec = click.option(
    "--distrib",
    "-d",
    type=OptionChoice,
)
_latest: Dec = click.option(
    "--latest",
    type=OptionChoice,
)
_replica: Dec = click.option(
    "--replica",
    type=OptionChoice,
)
_retracted: Dec = click.option(
    "--retracted",
    type=OptionChoice,
)

# Query dates group
datetime_type = click.DateTime(["%Y-%m-%d"])
_from: Dec = click.option(
    "date_from",
    "--from",
    type=datetime_type,
    default=None,
)
_to: Dec = click.option(
    "date_to",
    "--to",
    type=datetime_type,
    default=None,
)

_children: Dec = click.option(
    "--children",
    "-c",
    is_flag=True,
    default=False,
)
_parents: Dec = click.option(
    "--parents",
    "-p",
    is_flag=True,
    default=False,
)
_expand: Dec = click.option(
    "--expand",
    "-e",
    is_flag=True,
    default=False,
)


class groups:
    display: Dec = compose(
        _page,
        _zero,
        _all,
    )
    json_yaml: Dec = compose(
        _json,
        _yaml,
    )
    query_def: Dec = compose(
        _tag,
        _require,
        _distrib,
        _latest,
        _replica,
        _retracted,
    )
    query_date: Dec = compose(
        _from,
        _to,
    )
    show: Dec = compose(
        _children,
        _parents,
        _expand,
    )
