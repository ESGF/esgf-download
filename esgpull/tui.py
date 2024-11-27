from __future__ import annotations

import atexit
import logging
import sys
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from datetime import datetime
from enum import IntEnum
from json import dumps as json_dumps
from pathlib import Path
from typing import Any, TypeVar

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup

import click.exceptions
from attrs import define, field
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.logging import RichHandler
from rich.progress import Progress, ProgressColumn, track
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
_record_console = Console(highlight=True, record=True)

T = TypeVar("T")


class Verbosity(IntEnum):
    Normal = 0
    Errors = 1
    Detail = 2
    Debug = 3

    def get_level(self) -> int:
        levels = [
            logging.WARNING,
            logging.WARNING,
            logging.INFO,
            logging.DEBUG,
        ]
        return levels[self]

    def render(self) -> Text:
        return Text(self.name.upper(), style=f"logging.level.{self.name}")


class DummyConsole:
    def print(self, msg: str) -> None:
        pass


class DummyLive:
    def __enter__(self) -> DummyLive:
        return self

    def __exit__(self, *args): ...

    @property
    def console(self) -> DummyConsole:
        return DummyConsole()


def yaml_syntax(data: Mapping[str, Any]) -> Syntax:
    return Syntax(yaml_dump(data, sort_keys=False), "yaml", theme="ansi_dark")


def toml_syntax(data: Mapping[str, Any]) -> Syntax:
    return Syntax(tomlkit_dumps(data), "toml", theme="ansi_dark")


LOG_FORMAT = "[%(asctime)s]  %(levelname)-10s%(name)s\n%(message)s\n"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
FILE_DATE_FORMAT = "%Y-%m-%d_%H-%M-%S"


@define
class UI:
    path: Path = field(converter=Path)
    verbosity: Verbosity = Verbosity.Normal
    logfile: bool = True
    record: bool = False
    default_onraise: type[Exception] | Exception | None = None
    # max_size: int = 1 << 30

    @property
    def console(self) -> Console:
        if self.record:
            return _record_console
        else:
            return _console

    @property
    def err_console(self) -> Console:
        if self.record:
            return _record_console
        else:
            return _err_console

    @staticmethod
    def from_config(
        config: Config,
        verbosity: Verbosity = Verbosity.Normal,
        record: bool = False,
    ) -> UI:
        return UI(config.paths.log, verbosity=verbosity, record=record)

    @contextmanager
    def logging(
        self,
        modulename: str = "",
        onraise: type[Exception] | Exception | None = None,
        record: bool | None = None,
    ):
        if self.verbosity > Verbosity.Normal:
            logger.setLevel(logging.INFO)
        if record is not None:
            self.record = record
        handler: logging.Handler
        temp_path: Path | None = None
        fmt = LOG_FORMAT
        datefmt = LOG_DATE_FORMAT
        if self.verbosity >= Verbosity.Errors:
            if self.err_console.is_terminal or self.err_console.is_jupyter:
                handler = RichHandler(
                    console=self.err_console,
                    show_path=False,
                    markup=True,
                )
                fmt = "[yellow]· %(name)s ·[/]\n%(message)s"
                datefmt = "[%X]"
            else:
                handler = logging.StreamHandler()
            handler.setLevel(self.verbosity.get_level())
        elif self.logfile:
            date = datetime.utcnow().strftime(FILE_DATE_FORMAT)
            filename = "-".join(["esgpull", modulename, date]) + ".log"
            temp_path = self.path / filename
            handler = logging.FileHandler(temp_path)
            handler.setLevel(logging.DEBUG)
        else:
            handler = logging.NullHandler()
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
            self.print(f"[red]{type(exc).__name__}[/]: {exc}", err=True)
            if self.verbosity < Verbosity.Errors and self.logfile:
                self.print(
                    f"See [yellow]{temp_path}[/] for error log.",
                    err=True,
                )
            if onraise is not None:
                raise onraise
            elif self.default_onraise is not None:
                raise self.default_onraise
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
            console = self.err_console if err else self.console
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
        with self.console.capture() as capture:
            if json:
                self.console.print_json(json_dumps(msg), **kwargs)
            elif yaml:
                self.console.print(yaml_syntax(msg), **kwargs)
            elif toml:
                self.console.print(toml_syntax(msg), **kwargs)
            else:
                self.console.print(msg, **kwargs)
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
        # use _console to avoid recording the progress bar
        return Live(renderables, console=_console)

    def track(self, iterable: Iterable[T], **kwargs) -> Iterable[T]:
        # use _console to avoid recording the progress bar
        return track(iterable, console=_console, **kwargs)

    def make_progress(
        self,
        *columns: str | ProgressColumn,
        **kwargs: Any,
    ) -> Progress:
        # use _console to avoid recording the progress bar
        return Progress(*columns, console=_console, **kwargs)

    def spinner(self, msg: str) -> Status:
        # use _console to avoid recording the spinner
        return _console.status(msg, spinner="earth")

    def ask(self, msg: str, default: bool | None = None) -> bool:
        if default is not None:
            return Confirm.ask(msg, default=default, console=self.console)
        else:
            return Confirm.ask(msg, console=self.console)

    def choice(
        self,
        msg: str,
        choices: list[str],
        default: str | None = None,
        show_choices: bool = True,
    ) -> str:
        if default is not None:
            return Prompt.ask(
                msg,
                choices=choices,
                default=default,
                show_choices=show_choices,
                console=self.console,
            )
        else:
            return Prompt.ask(
                msg,
                choices=choices,
                show_choices=show_choices,
                console=self.console,
            )

    def prompt(
        self, msg: str, default: str | None = None, password: bool = False
    ) -> str:
        if default is not None:
            return Prompt.ask(
                msg,
                default=default,
                password=password,
                console=self.console,
            )
        else:
            return Prompt.ask(msg, password=password, console=self.console)

    def rule(self, msg: str):
        self.console.rule(msg)

    def export_svg(self) -> Path:
        date = datetime.utcnow().strftime(FILE_DATE_FORMAT)
        filename = "-".join(["record", date]) + ".svg"
        output_path = self.path / filename
        with output_path.open("w") as f:
            f.write(_record_console.export_svg())
        return output_path

    def raise_maybe_record(
        self,
        exc: type[Exception] | Exception | BaseExceptionGroup,
    ) -> None:
        if self.record:
            output_path = self.export_svg()
            self.print(f":+1: Console output exported to {output_path}")
        raise exc


TempUI = UI(
    "/tmp",
    Verbosity.Errors,
    logfile=False,
    default_onraise=click.exceptions.Exit(1),
)
