# import click
# from click.exceptions import Abort, Exit

# from esgpull import Esgpull
# from esgpull.cli.decorators import opts
# from esgpull.cli.utils import filter_docs, totable
# from esgpull.tui import Verbosity


# @click.command()
# @opts.force
# @opts.verbosity
# def autoremove(
#     force: bool,
#     verbosity: Verbosity,
# ):
#     esg = Esgpull(verbosity=verbosity)
#     with esg.ui.logging("autoremove", onraise=Abort):
#         deprecated = esg.db.get_deprecated_files()
#         nb = len(deprecated)
#         if not nb:
#             esg.ui.print("All files are up to date.")
#             raise Exit(0)
#         if not force:
#             docs = filter_docs([file.raw for file in deprecated])
#             esg.ui.print(totable(docs))
#             s = "s" if nb > 1 else ""
#             esg.ui.print(f"Found {nb} file{s} to remove.")
#             if not esg.ui.ask("Continue?", default=True):
#                 raise Abort
#         removed = esg.remove(*deprecated)
#         esg.ui.print(f"Removed {len(removed)} files with newer version.")
#         nb_remain = len(removed) - nb
#         if nb_remain:
#             esg.ui.print(f"{nb_remain} files could not be removed.")
#         if force:
#             docs = filter_docs([file.raw for file in removed])
#             esg.ui.print(totable(docs))
