import click
from click.exceptions import Abort, Exit
from rich.box import MINIMAL_DOUBLE_HEAD
from rich.table import Table
from rich.text import Text

from esgpull.cli.decorators import opts
from esgpull.cli.utils import init_esgpull
from esgpull.models import sql
from esgpull.tui import Verbosity
from esgpull.utils import format_size


@click.command()
@opts.simple
@opts.all
@opts.verbosity
def status(
    simple: bool,
    all_: bool,
    verbosity: Verbosity,
):
    """
    View file queue status

    Use the `--all` flag to include already downloaded files (`done` status).
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("status", onraise=Abort):
        status_count_size = list(esg.db.rows(sql.file.status_count_size(all_)))
        table = Table(box=MINIMAL_DOUBLE_HEAD, show_edge=False)
        table.add_column("status", justify="right", style="bold blue")
        table.add_column("files", justify="center")
        table.add_column("size", justify="right", style="magenta")
        if not status_count_size:
            esg.ui.print("Queue is empty.")
            raise Exit(0)
        if simple:
            for status, count, size in status_count_size:
                table.add_row(status.value, str(count), format_size(size))
        else:
            esg.graph.load_db()
            first_line = True
            for status, count, total_size in status_count_size:
                first_row = True
                for query in esg.graph.queries.values():
                    files = [f for f in query.files if f.status == status]
                    if files:
                        if first_row:
                            if first_line:
                                first_line = False
                            else:
                                table.add_row(end_section=True)
                            table.add_row(
                                Text(status.value, justify="left"),
                                str(count),
                                format_size(total_size),
                                style="bold",
                                end_section=True,
                            )
                            first_row = False
                        table.add_row(
                            query.rich_name,
                            str(len(files)),
                            format_size(sum([f.size for f in files])),
                        )
        esg.ui.print(table)
