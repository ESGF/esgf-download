# import asyncio

# import click
# import rich
# from click.exceptions import Abort, Exit
# from exceptiongroup import BaseExceptionGroup

# from esgpull import Esgpull
# from esgpull.cli.decorators import opts
# from esgpull.db.models import FileStatus
# from esgpull.tui import Verbosity, logger
# from esgpull.utils import format_size


# @click.command()
# @opts.quiet
# @opts.verbosity
# def download(
#     quiet: bool,
#     verbosity: Verbosity,
# ):
#     esg = Esgpull.with_verbosity(verbosity)
#     queue = esg.db.search(statuses=[FileStatus.Queued])
#     if not queue:
#         rich.print("Download queue is empty.")
#         raise Exit(0)
#     coro = esg.download(queue, show_progress=not quiet)
#     with esg.ui.logging("download", onraise=Abort):
#         files, errors = asyncio.run(coro)
#         if files:
#             size = format_size(sum(file.size for file in files))
#             esg.ui.print(
#                 f"Downloaded {len(files)} new files for a total size of {size}"
#             )
#         if errors:
#             logger.error(f"{len(errors)} files could not be installed.")
#             raise BaseExceptionGroup("Download", [e.err for e in errors])
