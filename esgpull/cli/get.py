import click
import rich
from click_params import PUBLIC_URL

from esgpull.context import Context


@click.command()
@click.option("--metadata", "-m", is_flag=True, default=False)
@click.argument("url", nargs=1, type=PUBLIC_URL)
def get(
    url: str,
    metadata: bool,  # display file metadata, no download
):
    """
    Steps:
        1. validate url (version+filename)
        2. create Context
        3. fetch File from Context
        4. search database (if exist -> [Y/N])
        5. check file exists in output directory (if exist -> [Y/N]) -> rename
        6. download
    """
    rich.print(url)
    ctx = Context()
    print(ctx)

    raise NotImplementedError()
