import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import init_esgpull
from esgpull.tui import Verbosity


def extract_command(doc: dict, key: str | None) -> dict:
    if key is None:
        return doc
    for part in key.split("."):
        if not part:
            raise KeyError(key)
        elif part in doc:
            doc = doc[part]
        else:
            raise KeyError(part)
    for part in key.split(".")[::-1]:
        doc = {part: doc}
    return doc


@click.command()
@args.key
@args.value
@opts.generate
@opts.verbosity
def config(
    key: str | None,
    value: str | None,
    generate: bool,
    verbosity: Verbosity,
):
    """
    View/modify config

    The full config is shown when no arguments are provided. It includes all items overwritten
    in the `config.toml` file, and default values otherwise.
    Note that the `config.toml` file does not exist by default, an empty file will be created
    on the first modification of any item. Otherwise one can generate a config file containing
    every default value using the `--generate` flag.

    To view a specific config section/item, the dot-separated path to that section/item must
    be provided as the only argument.

    To modify a config item, the dot-separated path to that item must be provided as the first
    argument, along with the new value that item should get as the second argument.

    Only config items can be modified.
    """
    esg = init_esgpull(verbosity=verbosity, load_db=False)
    with esg.ui.logging("config", onraise=Abort):
        if key is not None and value is not None:
            old_value = esg.config.update_item(key, value, empty_ok=True)
            info = extract_command(esg.config.dump(), key)
            esg.config.write()
            esg.ui.print(info, toml=True)
            esg.ui.print(f"Previous value: {old_value}")
        elif key is not None:
            info = extract_command(esg.config.dump(), key)
            esg.ui.print(info, toml=True)
        else:
            esg.ui.rule(str(esg.config._config_file))
            esg.ui.print(esg.config.dump(), toml=True)
            if generate:
                esg.config.generate()
                msg = f":+1: Config generated at {esg.config._config_file}"
                esg.ui.print(msg)
                raise Exit(0)
