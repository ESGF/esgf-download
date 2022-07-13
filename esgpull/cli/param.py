import click
import rich

from esgpull import Context, Storage

DB_PATH = "/home/srodriguez/ipsl/data/synda/db/sdt_new.db"
SKIP_FACETS = [
    "cf_standard_name",
    "variable_long_name",
    "creation_date",
    "datetime_end",
]

storage = Storage(path=DB_PATH)


def fetch_esgf_params():
    with storage.select(storage.Param) as ctx:
        params = ctx.scalars
    for param in params:
        storage.session.delete(param)
    storage.session.commit()
    ctx = Context(distrib=True)
    ctx.query.facets = "index_node"
    index_nodes = list(ctx.facet_counts[0]["index_node"])
    ctx = Context(distrib=False)
    for index_node in index_nodes:
        with ctx.query:
            ctx.query.index_node = index_node
    index_facets = ctx.facet_counts
    facet_counts = {}
    for facets in index_facets:
        for name, values in facets.items():
            if name in SKIP_FACETS or len(values) == 0:
                continue
            facet_values = set()
            for value, count in values.items():
                if count and len(value) <= 255:
                    facet_values.add(value)
            if facet_values:
                facet_counts.setdefault(name, set())
                facet_counts[name] |= facet_values
    for name, values in facet_counts.items():
        for value in values:
            param = storage.Param(name=name, value=value)
            storage.session.add(param)
    storage.session.commit()


@click.group()
def param():
    ...


@param.command()
def init():
    fetch_esgf_params()


@param.command()
def update():
    fetch_esgf_params()
    rich.print("Params are up to date.")


@param.command("list")
def list_cmd():
    with storage.select(storage.Param.name) as ctx:
        params = ctx.distinct().scalars
    rich.print(params)


@param.command()
@click.argument("name", nargs=1, type=str)
def facet(name):
    with storage.select(storage.Param.value) as ctx:
        params = ctx.where(storage.Param.name == name).scalars
    rich.print(params)
