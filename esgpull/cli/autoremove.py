import click
import rich
from click.exceptions import Exit

from esgpull import Esgpull
from esgpull.cli.utils import totable


@click.command()
@click.option("--force", "-f", is_flag=True, default=False)
def autoremove(force: bool):
    esg = Esgpull()
    deprecated = esg.db.get_deprecated_files()
    nb = len(deprecated)
    if not nb:
        rich.print("All files are up to date.")
        raise Exit(0)
    if not force:
        rich.print(totable([file.raw for file in deprecated]))
        s = "s" if nb > 1 else ""
        rich.print(f"Removing {nb} file{s}")
        click.confirm("Continue?", default=True, abort=True)
    removed = esg.remove(*deprecated)
    rich.print(f"Removed {len(removed)} files with newer version.")
    nb_remain = len(removed) - nb
    if nb_remain:
        rich.print(f"{nb_remain} files could not be removed.")
    if force:
        rich.print(totable([file.raw for file in removed]))
