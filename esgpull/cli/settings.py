import rich
import yaml
import click

from esgpull import Esgpull

SKIP_PARAMS = ["help", "version", "yes"]


def get_top_ctx(ctx):
    parent = ctx.parent
    if parent is not None:
        return get_top_ctx(parent)
    else:
        return ctx


def get_info_dict(ctx):
    return get_top_ctx(ctx).to_info_dict()["command"]


def get_defaults(d: dict) -> dict:
    result = {}
    for param in d.get("params", []):
        name = param["name"]
        is_option = param["param_type_name"] == "option"
        default = param.get("default")
        if name not in SKIP_PARAMS and is_option and default is not None:
            result[name] = default
    for command, info in d.get("commands", {}).items():
        params = get_defaults(info)
        if params:
            result[command] = params
    return result


def print_yaml(d: dict) -> None:
    if d:
        yml = yaml.dump(d)
        syntax = rich.syntax.Syntax(yml, "yaml")
        rich.print(syntax)
    else:
        click.echo("Nothing to configure.")


@click.command()
@click.option("--global", "_global", is_flag=True, default=True)
@click.option("--cli", is_flag=True, default=False)
@click.argument("command", type=str, nargs=1, default="")
@click.pass_context
def settings(ctx, _global: bool, cli: bool, command: str):
    if cli:
        info = get_info_dict(ctx)
        keys = command.split(".")
        for key in keys:
            if len(key) > 0:
                info = info["commands"][key]
        print_yaml(get_defaults(info))
    elif _global:
        esg = Esgpull()
        print_yaml(esg.settings.dict())
