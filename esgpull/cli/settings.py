import rich
import yaml
import click
from click_default_group import DefaultGroup

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


@click.group(cls=DefaultGroup, default="global", default_if_no_args=True)
def settings():
    ...


@settings.command("global")
def _global():
    esg = Esgpull()
    print_yaml(esg.settings.dict())


@settings.group(cls=DefaultGroup, default="show", default_if_no_args=True)
@click.pass_context
def cli(ctx):
    ...


@cli.command()
@click.pass_context
def show(ctx):
    info = get_info_dict(ctx)
    print_yaml(get_defaults(info))


@cli.command()
@click.pass_context
@click.argument("command", nargs=1, type=str)
def get(ctx, command):
    info = get_info_dict(ctx)
    for key in command.split("."):
        info = info["commands"][key]
    print_yaml(get_defaults(info))