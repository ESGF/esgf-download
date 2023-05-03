# from __future__ import annotations

# import click
# from click.exceptions import Abort, Exit

# from esgpull import Esgpull
# from esgpull.cli.decorators import args, opts
# from esgpull.cli.utils import load_facets
# from esgpull.db.models import File
# from esgpull.tui import Verbosity
# from esgpull.utils import format_size


# @click.command()
# @opts.distrib
# @opts.dry_run
# @opts.force
# @opts.replica
# @opts.since
# @opts.selection_file
# @opts.verbosity
# @args.facets
# def install(
#     facets: list[str],
#     distrib: bool,
#     dry_run: bool,
#     force: bool,
#     replica: bool | None,
#     selection_file: str | None,
#     since: str | None,
#     verbosity: Verbosity,
# ) -> None:
#     esg = Esgpull(verbosity=verbosity)
#     with (
#         esg.context(
#             distrib=distrib,
#             latest=True,
#             since=since,
#             replica=replica,
#         ) as ctx,
#         esg.ui.logging("install", onraise=Abort),
#     ):
#         load_facets(ctx.query, facets, selection_file)
#         if not ctx.query.dump():
#             raise click.UsageError("No search terms provided.")
#         hits = ctx.file_hits
#         nb_files = sum(hits)
#         esg.ui.print(f"Found {nb_files} files.")
#         if nb_files > 500 and distrib:
#             # Enable better distrib
#             ctx.index_nodes = esg.fetch_index_nodes()
#         if dry_run:
#             queries = ctx._build_queries_search(
#                 hits, file=True, max_results=nb_files, offset=0
#             )
#             esg.ui.print(queries)
#             raise Exit(0)
#         if not force and nb_files > 5000:
#             nb_req = nb_files // esg.config.api.page_limit
#             message = f"{nb_req} requests will be send to ESGF. Continue?"
#             if not esg.ui.ask(message, default=True):
#                 raise Abort
#         results = ctx.search(
#             file=True,
#             max_results=None,
#             offset=0,
#             hits=hits,
#         )
#         files = [File.from_dict(result) for result in results]
#         total_size = sum([file.size for file in files])
#         esg.ui.print(f"Total size: {format_size(total_size)}")
#         if not force:
#             if not esg.ui.ask("Continue?", default=True):
#                 raise Abort
#         to_download, already_on_disk = esg.install(*files)
#         if to_download:
#             nb = len(to_download)
#             s = "s" if nb > 1 else ""
#             esg.ui.print(f"Installed {nb} new file{s} ready for download.")
#         if already_on_disk:
#             nb = len(already_on_disk)
#             s = "s" if nb > 1 else ""
#             esg.ui.print(f"Tracking {nb} file{s} already downloaded.")
#         if not to_download and not already_on_disk:
#             esg.ui.print("All files are already in the database.")
