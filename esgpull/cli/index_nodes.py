from __future__ import annotations

from collections import OrderedDict

import click
from click.exceptions import Abort, Exit
from httpx import HTTPError

from esgpull.cli.decorators import groups, opts
from esgpull.cli.utils import init_esgpull, totable
from esgpull.context.solr import SolrContext
from esgpull.models import Query
from esgpull.tui import Verbosity, logger


def check_node(c: SolrContext, node: str) -> bool:
    result = True
    try:
        c.probe(index_node=node)
    except* HTTPError:
        result = False
    return result


def find_nodes(c: SolrContext, node: str) -> list[str]:
    try:
        hints = c.hints(
            Query(), file=False, index_node=node, facets=["index_node"]
        )
        return list(hints[0]["index_node"])
    except:
        return []


@click.command()
@groups.json_yaml
@opts.record
@opts.verbosity
def index_nodes(
    ## json_yaml
    json: bool,
    yaml: bool,
    record: bool,
    verbosity: Verbosity,
) -> None:
    """
    Test index nodes for their current status
    """
    esg = init_esgpull(
        verbosity,
        safe=False,
        record=record,
    )
    with esg.ui.logging("scan", onraise=Abort):
        node_status: dict[str, bool] = {}
        nodes = [
            "esgf-node.ipsl.upmc.fr",
            "esgf-data.dkrz.de",
            "esgf.ceda.ac.uk",
            "esgf-node.ornl.gov/esgf-1-5-bridge",
        ]

        esg.config.api.http_timeout = 3
        with esg.ui.spinner("Fetching index nodes status"):
            while True:
                if not nodes:
                    break
                node = nodes.pop()
                if node in node_status or node == "us-index":
                    continue
                logger.info(f"check: {node}")
                node_status[node] = check_node(esg.context._solr, node)
                logger.info(f"{node}\tok: {node_status[node]}")
                for node in find_nodes(esg.context._solr, node):
                    if node not in node_status:
                        nodes.append(node)
                        logger.info(f"found index_node: {node}")
        if json:
            esg.ui.print(node_status, json=True)
        elif yaml:
            esg.ui.print(node_status, yaml=True)
        else:
            table = [
                OrderedDict(
                    [
                        ("node", node),
                        (
                            "status",
                            "[green]OK" if status else "[red]no response",
                        ),
                    ]
                )
                for node, status in node_status.items()
            ]
            esg.ui.print(totable(table))
        esg.ui.raise_maybe_record(Exit(0))
