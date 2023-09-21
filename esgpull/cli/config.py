from textwrap import dedent

import click
from click.exceptions import Abort, BadOptionUsage, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import init_esgpull
from esgpull.config import ConfigKind
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
@opts.default
@opts.generate
@opts.record
@opts.verbosity
def config(
    key: str | None,
    value: str | None,
    default: bool,
    generate: bool,
    record: bool,
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
    esg = init_esgpull(verbosity=verbosity, load_db=False, record=record)
    with esg.ui.logging("config", onraise=Abort):
        if key is not None and value is not None:
            if default:
                raise BadOptionUsage(
                    "default",
                    dedent(
                        f"""
                        --default/-d is invalid with a value.
                        Instead use:

                        $ esgpull config {key} -d
                        """
                    ),
                )
            kind = esg.config.kind
            old_value = esg.config.update_item(key, value, empty_ok=True)
            info = extract_command(esg.config.dump(), key)
            esg.config.write()
            esg.ui.print(info, toml=True)
            if kind == ConfigKind.NoFile:
                esg.ui.print(
                    ":+1: New config file created at "
                    f"{esg.config._config_file}."
                )
            else:
                esg.ui.print(f"Previous value: {old_value}")
        elif key is not None:
            if default:
                old_value = esg.config.set_default(key)
                info = extract_command(esg.config.dump(), key)
                esg.config.write()
                esg.ui.print(info, toml=True)
                esg.ui.print(f"Previous value: {old_value}")
            else:
                info = extract_command(esg.config.dump(), key)
                esg.ui.print(info, toml=True)
        elif generate:
            overwrite = False
            if esg.config.kind == ConfigKind.Complete:
                esg.ui.print(
                    f"{esg.config._config_file}\n"
                    ":+1: Your config file is already complete."
                )
                esg.ui.raise_maybe_record(Exit(0))
            elif esg.config.kind == ConfigKind.Partial and esg.ui.ask(
                "A config file already exists,"
                " fill it with missing defaults?",
                default=False,
            ):
                overwrite = True
            esg.config.generate(overwrite=overwrite)
            msg = f":+1: Config generated at {esg.config._config_file}"
            esg.ui.print(msg)
        else:
            esg.ui.rule(str(esg.config._config_file))
            esg.ui.print(esg.config.dump(), toml=True)
        esg.ui.raise_maybe_record(Exit(0))
