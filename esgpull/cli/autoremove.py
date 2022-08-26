import rich
import click

from esgpull import Esgpull
from esgpull.cli.utils import totable


@click.command()
@click.pass_context
@click.option("--force", "-f", is_flag=True, default=False)
def autoremove(ctx, force: bool):
    esg = Esgpull()
    deprecated = esg.db.get_deprecated_files()
    nb = len(deprecated)
    if not nb:
        rich.print("All files are up to date.")
        ctx.exit(0)
    if not force:
        rich.print(totable([file.metadata for file in deprecated]))
        s = "s" if nb > 1 else ""
        click.confirm(f"Remove {nb} file{s}?", default=True, abort=True)
    removed = esg.remove(*deprecated)
    rich.print(f"Removed {len(removed)} files with newer version.")
    nb_remain = len(removed) - nb
    if nb_remain:
        rich.print(f"{nb_remain} files could not be removed.")
    if force:
        rich.print(totable([file.metadata for file in removed]))
