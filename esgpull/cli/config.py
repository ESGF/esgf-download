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
