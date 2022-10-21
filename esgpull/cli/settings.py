import click

from esgpull import Esgpull
from esgpull.cli.utils import print_toml


def extract_command(info: dict, command: str | None) -> dict:
    if command is None:
        return info
    keys = command.split(".")
    assert all(len(key) > 0 for key in keys)
    for key in keys:
        info = info[key]
    for key in keys[::-1]:
        info = {key: info}
    return info


@click.command()
@click.argument("key", type=str, nargs=1, required=False, default=None)
@click.argument("value", type=str, nargs=1, required=False, default=None)
# @click.argument("value", type=str, nargs=1, required=False, default=None)
def settings(key: str | None, value: str | None):
    # [?]TODO: load Filesystem for SettingsPath
    # [?]TODO: load Esgpull for auto SettingsPath
    esg = Esgpull()
    info = extract_command(esg.settings.dict(), key)
    print_toml(info)
