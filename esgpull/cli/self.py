import os
from configparser import ConfigParser
from pathlib import Path

import click
from click.exceptions import Abort, Exit
from rich.table import Table

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import init_esgpull
from esgpull.config import Config
from esgpull.exceptions import (
    InvalidInstallPath,
    UnknownInstallName,
    UnregisteredInstallPath,
)
from esgpull.install_config import InstallConfig
from esgpull.tui import TempUI, Verbosity


def get_synda_db_path(sdt_home: str | None = None) -> Path | None:
    if sdt_home is None:
        sdt_home = os.getenv("SDT_HOME")
    if sdt_home is not None:
        sdt_path = Path(sdt_home).expanduser().resolve()
        conf_path = sdt_path / "conf" / "sdt.conf"
        conf = ConfigParser()
        conf.read(conf_path)
        path = Path(conf["core"]["db_path"]) / "sdt.db"
        if path.is_file():
            db_path = path
        else:
            db_path = None
    else:
        db_path = None
    return db_path


@click.group()
def self():
    """
    Manage esgpull installations / import synda database
    """
    ...


@self.command()
@args.path
@opts.name
@opts.verbosity
def install(
    path: Path | None,
    name: str | None,
    verbosity: Verbosity,
):
    with TempUI.logging():
        TempUI.rule("[b green]esgpull[/] installation")
        if path is None:
            default = str(InstallConfig.default)
            path = Path(TempUI.prompt("Install location", default=default))
            idx = InstallConfig.index(path=path)
            if idx > -1:
                TempUI.print(
                    (
                        f"\n:stop_sign: {path} is already installed:\n"
                        f"  {InstallConfig.installs[idx]}\n\n"
                        f"{InstallConfig.activate_msg(idx)}"
                    ),
                    err=True,
                )
                raise Exit(1)
            if name is None:
                name = TempUI.prompt("Name (optional)") or None
        idx = InstallConfig.add(path, name)
        InstallConfig.choose(idx=idx)
        path = InstallConfig.installs[idx].path
        if path.is_dir():
            TempUI.print(f"Using existing install at {path}")
        else:
            TempUI.print(f"Creating install directory and files at {path}")
        InstallConfig.write()
        TempUI.print(f"Install config added to {InstallConfig.path}")
        esg = Esgpull(verbosity=verbosity, install=True)
    with esg.ui.logging("init", onraise=Abort):
        # with esg.ui.spinner("Fetching facets"):
        #     if esg.fetch_facets(update=False):
        #         esg.ui.print(":+1: Facets are initialised.")
        #     else:
        #         esg.ui.print(":+1: Facets were already initialised.")
        sdt_home = os.getenv("SDT_HOME")
        if sdt_home is not None:
            db_name = get_synda_db_path(sdt_home) or ""
            msg = (
                f"Found existing synda installation at {sdt_home}\n"
                "You can import its database by running:\n"
                f"$ esgpull self import_synda {db_name}"
            )
            esg.ui.print(msg)


@self.command()
@args.path
@opts.name
def activate(
    path: Path | None,
    name: str | None,
):
    with TempUI.logging():
        idx = InstallConfig.index(path=path, name=name)
        if idx < 0:
            if path is not None and InstallConfig.index(name=path.name) > -1:
                raise ValueError(
                    f"{InstallConfig.fullpath(path)} is not installed, "
                    "did you mean this?\n\n"
                    f"$ esgpull self activate --name {path}"
                )
            raise InvalidInstallPath(path=path)
        TempUI.print(InstallConfig.activate_needs_eval(idx))


@self.command()
@args.path
@opts.name
def choose(
    path: Path | None,
    name: str | None,
):
    with TempUI.logging():
        if path is None and name is None:
            table = Table(
                title="Install locations",
                title_style="bold",
                title_justify="left",
                show_header=False,
                show_footer=False,
                show_edge=False,
                box=None,
                padding=(0, 1),
            )
            table.add_column(style="b yellow")
            table.add_column(style="magenta")
            table.add_column(style="b green")
            for i, inst in enumerate(InstallConfig.installs):
                table.add_row(
                    "*" if i == InstallConfig.current_idx else "",
                    str(inst.path),
                    inst.name or "",
                )
            TempUI.print(table)
            raise Exit(0)
        InstallConfig.choose(path=path, name=name)
        if InstallConfig.current is None:
            if name is not None:
                raise UnknownInstallName(name)
            elif path is not None:
                raise UnregisteredInstallPath(path)
        else:
            InstallConfig.write()


@self.command()
def reset():
    with TempUI.logging():
        if InstallConfig.current is not None:
            idx = InstallConfig.current_idx
            path = InstallConfig.current.path
            InstallConfig.choose()
            InstallConfig.write()
            TempUI.print(
                f"Install location is not set anymore as {path}\n"
                # "To set it back to its previous location, run:\n"
                # f"$ esgpull self choose {name}"
            )
            TempUI.print(InstallConfig.activate_msg(idx))
            raise Exit(0)
        else:
            TempUI.print(":stop_sign: No install found.")
            TempUI.print("To install esgpull, run:\n")
            TempUI.print("$ esgpull self install")
            raise Exit(1)


@self.command()
def delete():
    with TempUI.logging():
        if InstallConfig.current is None:
            msg = (
                "None\n"
                "Please choose an existing install before trying to delete it."
                "\n\n$ esgpull self choose ..."
            )
            raise UnregisteredInstallPath(msg)
        else:
            path = InstallConfig.current.path
            TempUI.print(f"You are going to delete: {path}")
            choice = TempUI.prompt(f"Please enter {path.name!r} to continue")
            if choice == path.name:
                TempUI.print(f"Deleting {path} from config...")
                TempUI.print("To remove all files from this install, run:\n")
                config = Config.load(path=path)
                for p in config.paths:
                    if not p.is_relative_to(path):
                        TempUI.print(f"$ rm -rf {p}")
                TempUI.print(f"$ rm -rf {path}")
                InstallConfig.remove_current()
                InstallConfig.write()
            else:
                raise Abort


@self.command()
@args.path
@opts.verbosity
def import_synda(
    path: Path | None,
    verbosity: Verbosity,
):
    esg = init_esgpull(verbosity)
    with esg.ui.logging("import_synda", onraise=Abort):
        if path is None:
            sdt_home = os.getenv("SDT_HOME")
            prompt_title = "Enter synda database location"
            if sdt_home is not None:
                esg.ui.print(
                    "Found existing synda installation at"
                    f" SDT_HOME={sdt_home}"
                )
                default = str(get_synda_db_path(sdt_home))
                path = Path(esg.ui.prompt(prompt_title, default=str(default)))
            else:
                path = Path(esg.ui.prompt(prompt_title))
        else:
            path = path.expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        nb_imported = esg.import_synda(url=path, track=True, ask=True)
        esg.ui.print(f"Imported {nb_imported} new files from {path}")
