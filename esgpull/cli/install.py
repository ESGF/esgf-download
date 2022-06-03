import click
import time


@click.command()
@click.option("--yes", "-y", is_flag=True, default=False)
def install(yes):
    if not yes:
        time.sleep(1)
        click.confirm(
            "Are you sure?", default=True, abort=True
        )
    raise NotImplementedError
