from __future__ import annotations

import atexit
import logging
from contextlib import contextmanager
from datetime import datetime
from enum import IntEnum
from json import dumps as json_dumps
from pathlib import Path
from typing import Any, Mapping

import click.exceptions
from attrs import define, field
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.logging import RichHandler
from rich.progress import Progress, ProgressColumn
from rich.prompt import Confirm, Prompt
from rich.status import Status
from rich.syntax import Syntax
from rich.text import Text
from tomlkit import dumps as tomlkit_dumps
from yaml import dump as yaml_dump

from esgpull.config import Config

logger = logging.getLogger("esgpull")
logging.root.setLevel(logging.DEBUG)
logging.root.addHandler(logging.NullHandler())

# sqlalchemy is very verbose
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

_console = Console(highlight=True)
_err_console = Console(stderr=True)


class Verbosity(IntEnum):
    Normal = 0
    Errors = 1
    Detail = 2
    Debug = 3

    def get_level(self) -> int:
        return [logging.WARNING, logging.WARNING, logging.INFO, logging.DEBUG][
            self
        ]

    def render(self) -> Text:
        return Text(self.name.upper(), style=f"logging.level.{self.name}")


class DummyLive:
    def __enter__(self):
        ...

    def __exit__(self, *args):
        ...


def yaml_syntax(data: Mapping[str, Any]) -> Syntax:
    return Syntax(yaml_dump(data, sort_keys=False), "yaml", theme="ansi_dark")


def toml_syntax(data: Mapping[str, Any]) -> Syntax:
    return Syntax(tomlkit_dumps(data), "toml", theme="ansi_dark")


@define
class UI:
    path: Path = field(converter=Path)
    verbosity: Verbosity = Verbosity.Normal
    # max_size: int = 1 << 30

    @staticmethod
    def from_config(
        config: Config, verbosity: Verbosity = Verbosity.Normal
    ) -> UI:
        return UI(config.paths.log, verbosity)

    @contextmanager
    def logging(
        self,
        modulename: str,
        onraise: type[Exception] | None = None,
    ):
        handler: logging.Handler
        temp_path: Path | None = None
        fmt = "[%(asctime)s]  %(levelname)-10s%(name)s\n%(message)s\n"
        datefmt = "%Y-%m-%d %H:%M:%S"
        file_datefmt = "%Y-%m-%d_%H-%M-%S"
        if self.verbosity >= Verbosity.Errors:
            if _err_console.is_terminal or _err_console.is_jupyter:
                handler = RichHandler(
                    console=_err_console,
                    show_path=False,
                    markup=True,
                )
                fmt = "[yellow]· %(name)s ·[/]\n%(message)s"
                datefmt = "[%X]"
            else:
                handler = logging.StreamHandler()
            handler.setLevel(self.verbosity.get_level())
        else:
            date = datetime.utcnow().strftime(file_datefmt)
            filename = "-".join(["esgpull", modulename, date]) + ".log"
            temp_path = self.path / filename
            handler = logging.FileHandler(temp_path)
            handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
        logging.root.addHandler(handler)
        try:
            yield
        except (click.exceptions.Exit, click.exceptions.Abort):
            if temp_path is not None:
                atexit.register(temp_path.unlink)
            raise
        except click.exceptions.ClickException:
            raise
        except BaseException as exc:
            tb = exc.__traceback__
            while True:
                if tb is None:
                    break
                elif tb.tb_next is None:
                    break
                tb = tb.tb_next
            if tb is None:
                f_locals = {}
            else:
                f_locals = tb.tb_frame.f_locals
            locals_text = self.render(f_locals, highlight=False)
            logging.root.debug(f"Locals:\n{locals_text}")
            logging.root.exception("")
            if self.verbosity < Verbosity.Errors:
                self.print(f"[red]{type(exc).__name__}[/]: {exc}", err=True)
                self.print(
                    f"See [yellow]{temp_path}[/] for error log.",
                    err=True,
                )
            if onraise is not None:
                raise onraise
            else:
                raise
        else:
            if temp_path is not None:
                atexit.register(temp_path.unlink)
        finally:
            logging.root.removeHandler(handler)

    def print(
        self,
        msg: Any,
        err: bool = False,
        json: bool = False,
        yaml: bool = False,
        toml: bool = False,
        verbosity: Verbosity = Verbosity.Normal,
        **kwargs: Any,
    ) -> None:
        if self.verbosity >= verbosity:
            console = _err_console if err else _console
            if json:
                console.print_json(json_dumps(msg), **kwargs)
            elif yaml:
                console.print(yaml_syntax(msg), **kwargs)
            elif toml:
                console.print(toml_syntax(msg), **kwargs)
            else:
                if not console.is_interactive:
                    kwargs.setdefault("crop", False)
                    kwargs.setdefault("overflow", "ignore")
                console.print(msg, **kwargs)

    def render(
        self,
        msg: Any,
        json: bool = False,
        yaml: bool = False,
        toml: bool = False,
        **kwargs: Any,
    ) -> str:
        with _console.capture() as capture:
            if json:
                _console.print_json(json_dumps(msg), **kwargs)
            elif yaml:
                _console.print(yaml_syntax(msg), **kwargs)
            elif toml:
                _console.print(toml_syntax(msg), **kwargs)
            else:
                _console.print(msg, **kwargs)
        return capture.get()

    def live(
        self,
        first: RenderableType,
        *rest: RenderableType,
        disable: bool = False,
    ) -> Live | DummyLive:
        if disable:
            return DummyLive()
        if not rest:
            renderables = first
        else:
            renderables = Group(first, *rest)
        return Live(renderables, console=_console)

    def make_progress(
        self, *columns: str | ProgressColumn, **kwargs: Any
    ) -> Progress:
        return Progress(
            *columns,
            console=_console,
            **kwargs,
        )

    def spinner(self, msg: str) -> Status:
        return _console.status(msg, spinner="earth")

    def ask(self, msg: str, default: bool = False) -> bool:
        return Confirm.ask(msg, default=default)

    def choice(
        self,
        msg: str,
        choices: list[str],
        default: str | None = None,
    ) -> str:
        if default is not None:
            return Prompt.ask(msg, choices=choices, default=default)
        else:
            return Prompt.ask(msg, choices=choices)
