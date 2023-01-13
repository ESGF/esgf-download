# from collections import Counter
# from typing import cast

# import click
# from click.exceptions import Abort, Exit
# from rich.box import MINIMAL
# from rich.table import Table
# from sqlalchemy.orm.attributes import InstrumentedAttribute

# from esgpull import Esgpull
# from esgpull.cli.decorators import opts
# from esgpull.db.models import File, FileStatus
# from esgpull.tui import Verbosity
# from esgpull.utils import format_size


# @click.command()
# @opts.all
# @opts.verbosity
# def status(
#     all_: bool,
#     verbosity: Verbosity,
# ):
#     esg = Esgpull.with_verbosity(verbosity)
#     with esg.ui.logging("status", onraise=Abort):
#         statuses = set(FileStatus)
#         if not all_:
#             statuses.remove(FileStatus.Done)
#         status_attr = cast(InstrumentedAttribute, File.status)
#         with esg.db.select(File) as stmt:
#             files = stmt.where(status_attr.in_(list(statuses))).scalars
#         counts = Counter(file.status for file in files)
#         sizes = {
#             status: sum(file.size for file in files if file.status == status)
#             for status in counts.keys()
#         }
#         if not counts:
#             esg.ui.print("Queue is empty.")
#             raise Exit(0)
#         table = Table(box=MINIMAL)
#         table.add_column("status", justify="right", style="bold blue")
#         table.add_column("#")
#         table.add_column("size")
#         for status in counts.keys():
#             table.add_row(
#                 status.value, str(counts[status]), format_size(sizes[status])
#             )
#         esg.ui.print(table)
