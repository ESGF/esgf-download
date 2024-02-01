from collections import Counter
from collections.abc import MutableMapping
from pathlib import Path

import click
import pyparsing as pp
import yaml
from click.exceptions import Abort, Exit
from rich.box import MINIMAL_DOUBLE_HEAD
from rich.prompt import Confirm
from rich.table import Table
from rich.text import Text

from esgpull.cli.decorators import opts
from esgpull.cli.utils import init_esgpull
from esgpull.graph import Graph
from esgpull.models import Options, Query, Tag
from esgpull.models.selection import FacetValues, Selection
from esgpull.tui import Verbosity, logger

SKIP = {"priority", "protocol"}
options_names = Options()._names
pp.ParserElement.set_default_whitespace_chars(" \t,")  # remove newline


def line(expr: pp.ParserElement) -> pp.ParserElement:
    return pp.LineStart().suppress() + expr + pp.LineEnd().suppress()


def sqbr(expr: pp.ParserElement) -> pp.ParserElement:
    return pp.Suppress("[") + expr + pp.Suppress("]")


anything = pp.Word(pp.printables)
word = pp.Word(pp.alphanums + "-_")
eq = pp.Suppress("=")

comment_start = pp.Char("#")[1, ...].suppress()
name_comment = pp.Group(line(comment_start + anything[1])[0, 1])
comment = line(comment_start + anything[...]).suppress()
facet_name = word("name")
facet_value = word
facet_values = pp.Group(word[1, ...])
facet = pp.Group(line(facet_name + eq + facet_values("vals")))("facet")
variable_cmip5 = pp.Group(
    line(
        "variable"
        + sqbr(facet_value)("realm")
        + sqbr(facet_value)("time_frequency")
        + eq
        + facet_values("variable")
    )
)("variable_cmip5")
variable_cmip6 = pp.Group(
    line(
        "variable"
        + sqbr(pp.Opt("table_id=") + facet_value("table_id"))
        + eq
        + facet_values("variable_id")
    )
)("variable_cmip6")
variable_no_sqbr = pp.Group(line("variable" + eq + facet_values("variable")))(
    "variable_no_sqbr"
)
otherwise = line(anything[...])("otherwise")
rest = pp.Group(
    comment
    | facet
    | variable_no_sqbr
    | variable_cmip5
    | variable_cmip6
    | otherwise
)[...]
selection_file = name_comment("name") + rest("rest")


def remove_duplicates(
    selection: MutableMapping[str, FacetValues],
) -> MutableMapping[str, FacetValues]:
    result: dict[str, FacetValues] = {}
    duplicates: dict[str, list[str]] = {}
    for name, values in selection.items():
        if isinstance(values, str):
            result[name] = values
            continue
        counter = Counter(values)
        nb_dup = sum(c > 1 for c in counter.values())
        if nb_dup == 0:
            result[name] = values
            continue
        duplicates[name] = list(dict(counter.most_common(nb_dup)))
    if duplicates:
        logger.warning(f"Duplicate values {duplicates}'")
    return result


def is_CMIP6(q: Query) -> bool:
    project = set(q.selection["project"] + q.selection["mip_era"])
    return "CMIP6" in project


def isnot_CMIP6(q: Query) -> bool:
    project = set(q.selection["project"] + q.selection["mip_era"])
    return any({"CMIP5", "CORDEX"} & project)


def fix_CMIP5(q: Query) -> Query:
    qd = q.asdict()
    if q.selection["frequency"]:
        qd["selection"]["time_frequency"] = qd["selection"].pop("frequency")
    return Query(**qd)


def convert_file(path: Path) -> Graph:
    logger.info(path)
    query = Query()
    kids: list[Query] = []
    result = selection_file.parse_file(path)
    if result.name:
        # name = result.name[0].split("@", 1)[0]
        query.tags.append(Tag(name=result.name[0]))
    for line in result.rest:
        if line.facet:
            name, values = line.facet.as_list()
            if name in SKIP:
                continue
            elif name in options_names:
                if len(values) > 1:
                    raise ValueError({name: values})
                d = {name: values[0]}
                logger.debug(f"OPTION {d}")
                setattr(query.options, name, values[0])
            elif name in Selection._facet_names:
                d = {name: values}
                logger.debug(f"FACET {d}")
                query.selection[name] = list(set(values))
            else:
                raise ValueError(f"{name!r} undefined\n{path.read_text()}")
        elif line.variable_cmip5:
            selection = line.variable_cmip5.as_dict()
            logger.debug(f"SUBQUERY {selection}")
            kid = Query(selection=remove_duplicates(selection), tracked=True)
            kids.append(kid)
        elif line.variable_cmip6:
            selection = line.variable_cmip6.as_dict()
            project = query.selection["project"]
            if project and project[0] in ["CMIP5", "CORDEX"]:
                if "variable_id" in selection:
                    selection["variable"] = selection.pop("variable_id")
                if "table_id" in selection:
                    selection["time_frequency"] = selection.pop("table_id")
            logger.debug(f"SUBQUERY {selection}")
            kid = Query(selection=remove_duplicates(selection))
            kids.append(kid)
        elif line.variable_no_sqbr:
            logger.error(line.as_dict())
        elif line.as_list():
            logger.error(line.as_list())
    if len(kids) > 1:
        query.untrack()
    elif len(kids) == 1:
        query = query << kids.pop()
    else:
        query.track(query.options)
    if isnot_CMIP6(query):
        query = fix_CMIP5(query)
    query.compute_sha()
    graph = Graph(None)
    graph.add(query)
    for kid in kids:
        if isnot_CMIP6(query) or isnot_CMIP6(kid):
            kid = fix_CMIP5(kid)
        kid.require = query.sha
        kid.compute_sha()
        expanded = query << kid
        kid.track(expanded.options)
        graph.add(kid)
    return graph


CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.argument(
    "paths",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    nargs=-1,
)
@click.option(
    "--out",
    "-o",
    type=click.Path(exists=False, path_type=Path),
    default=None,
)
@click.option("print_table", "--table", is_flag=True, default=False)
@click.option("print_graph", "--graph", is_flag=True, default=False)
@opts.record
@opts.verbosity
def convert(
    paths: list[Path],
    out: Path | None,
    print_table: bool,
    print_graph: bool,
    record: bool,
    verbosity: Verbosity,
):
    """
    Convert synda selection files to esgpull queries

    The flags `--table/--graph` are used to select the console output design.

    Use `--out <path/to/output.yaml>` to generate a query file containing the converted synda
    selection files. This file can be used as an input to the `add --query-file` command.

    Note that `convert` takes any number of input paths, and produces a single output.
    Using `**/*` as the input path is a good way to convert a whole tree of selection files.

    As a an arbitrary convention, the generated queries will be tagged with the content of the first line of a synda selection file, if that line starts with `#` and has a single word (+symbols) without whitespaces in it.
    """
    esg = init_esgpull(verbosity, safe=False, record=record)
    with esg.ui.logging("convert", onraise=Abort):
        if len(paths) == 0:
            esg.ui.print("No file provided")
            raise click.exceptions.Exit(0)
        if out is not None:
            out_str = f"[yellow]{out.resolve()}[/]"
            if out.is_file():
                esg.ui.print(f"Overwriting existing file {out_str}")
                allow = Confirm.ask("Continue?", default=False)
                if not allow:
                    raise click.exceptions.Exit(0)
        if out is None and not print_table and not print_graph:
            esg.ui.print(
                "[red]Error[/]: At least one of "
                "[yellow]--out/--table/--graph[/] is required."
            )
            raise click.exceptions.Exit(1)
        table = Table(
            box=MINIMAL_DOUBLE_HEAD,
            show_edge=False,
            show_lines=True,
        )
        table.add_column(Text("path", justify="left"))
        table.add_column(Text("file", justify="center"))
        table.add_column(Text("query", justify="center"))
        full_graph = Graph(None)
        empty_query = Query()
        empty_query.options.apply_defaults(empty_query.options)
        empty_query.compute_sha()
        for path in paths:
            if path.is_file():
                try:
                    graph = convert_file(path)
                except Exception as e:
                    msg = f"Cannot convert {path.resolve()}"
                    logger.exception(e)
                    logger.error(msg)
                    esg.ui.print(f"[red]Error[/]: {msg}")
                    continue
                queries = list(graph.queries.values())
                if len(queries) == 1 and queries[0].sha == empty_query.sha:
                    esg.ui.print(
                        f"Skipping empty query produced from {path.resolve()}"
                    )
                    continue
                path_text = Text(str(path).replace("/", "/\n"), style="yellow")
                file_text = Text(path.read_text())
                table.add_row(path_text, file_text, graph)
                full_graph.add(*queries, force=True)
        if print_table:
            esg.ui.print(table)
        if print_graph:
            esg.ui.print(full_graph)
        if out is not None:
            graph_as_yaml = yaml.dump(full_graph.dump())
            try:
                with out.open("w") as f:
                    f.write(graph_as_yaml)
                esg.ui.print(f":+1: [green]Graph was written to[/] {out_str}")
            except Exception:
                out.unlink()
                raise
        esg.ui.raise_maybe_record(Exit(0))
