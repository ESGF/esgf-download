from rich.console import Console, ConsoleOptions
from rich.measure import Measurement, measure_renderables


def short_sha(sha: str) -> str:
    return f"<{sha[:6]}>"


def rich_measure_impl(
    self,
    console: Console,
    options: ConsoleOptions,
) -> Measurement:
    renderables = list(self.__rich_console__(console, options))
    return measure_renderables(console, options, renderables)


def find_str(container: list | str) -> str:
    if isinstance(container, list):
        return find_str(container[0])
    elif isinstance(container, str):
        return container
    else:
        raise ValueError(container)


def find_int(container: list | int) -> int:
    if isinstance(container, list):
        return find_int(container[0])
    elif isinstance(container, int):
        return container
    else:
        raise ValueError(container)
