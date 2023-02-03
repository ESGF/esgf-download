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


def get_local_path(source: dict, version: str) -> str:
    flat_raw = {}
    for k, v in source.items():
        if isinstance(v, list) and len(v) == 1:
            flat_raw[k] = v[0]
        else:
            flat_raw[k] = v
    template = find_str(flat_raw["directory_format_template_"])
    # format: "%(a)/%(b)/%(c)/..."
    template = template.removeprefix("%(root)s/")
    template = template.replace("%(", "{")
    template = template.replace(")s", "}")
    flat_raw.pop("version", None)
    if "rcm_name" in flat_raw:  # cordex special case
        institute = flat_raw["institute"]
        rcm_name = flat_raw["rcm_name"]
        rcm_model = institute + "-" + rcm_name
        flat_raw["rcm_model"] = rcm_model
    return template.format(version=version, **flat_raw)
