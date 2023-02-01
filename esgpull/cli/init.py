from pathlib import Path

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import opts
from esgpull.config import RootSolver, RootSource
from esgpull.tui import TempUI, Verbosity


@click.command()
@opts.reset
@opts.root
@opts.verbosity
def init(
    reset: bool,
    root: Path | None,
    verbosity: Verbosity,
):
    with TempUI.logging():
        if reset:
            if RootSolver.from_user_config is not None:
                RootSolver.reset_user_config()
                TempUI.print(
                    "Install location is not set anymore as"
                    f" {RootSolver.from_user_config}\n"
                    "To set it back to its previous location, run:\n"
                    f"$ esgpull init --root {RootSolver.from_user_config}"
                )
                raise Exit(0)
            else:
                TempUI.print("No install found.")
                raise Exit(1)
        RootSolver.set(root)
        if RootSolver.source == RootSource.Set:
            if (
                RootSolver.user_installed
                and RootSolver.root != RootSolver.from_user_config
            ):
                TempUI.print(
                    ":stop_sign: Found existing install at"
                    f" {RootSolver.from_user_config}\n"
                    f"New install location will be {RootSolver.root}"
                )
                if not TempUI.ask("Proceed?", default=False):
                    raise Exit(1)
            do_install = True
        elif not RootSolver.installed:
            msg = " [b green]esgpull[/] installation "
            TempUI.print(f"{msg:=^80}")
            msg = "Install location"
            default = str(RootSolver.root)
            root = Path(TempUI.prompt(msg, default=default))
            RootSolver.set(root)
            do_install = True
        else:
            TempUI.print(f"Found existing install at {RootSolver.root}")
            do_install = False
        if RootSolver.from_user_config != RootSolver.root:
            TempUI.print(
                f"Setting install location to {RootSolver.root}\n"
                f"Install location written to {RootSolver.user_config_path}"
            )
            RootSolver.create_user_config(override=True)
            RootSolver.reload()
        if do_install:
            RootSolver.mkdir()
        esg = Esgpull(verbosity=verbosity, install=do_install)
    with esg.ui.logging("init", onraise=Abort):
        with esg.ui.spinner("Fetching facets"):
            if esg.fetch_facets(update=False):
                esg.ui.print(":+1: Facets are initialised.")
            else:
                esg.ui.print(":+1: Facets were already initialised.")
