from collections import defaultdict
from dataclasses import dataclass

import click
from click.exceptions import Abort, Exit
from rich.box import MINIMAL_DOUBLE_HEAD
from rich.table import Table

from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import init_esgpull, valid_name_tag
from esgpull.models import FileStatus
from esgpull.tui import Verbosity


@dataclass
class DatasetCounter:
    done: int = 0
    total: int = 0

    def is_complete(self) -> int:
        return self.done == self.total

    def asdict(self) -> dict:
        return {
            "done": self.done,
            "total": self.total,
            "complete": self.is_complete(),
        }


@click.command()
@args.query_id_required
@groups.json_yaml
@opts.verbosity
def datasets(
    query_id: str,
    json: bool,
    yaml: bool,
    verbosity: Verbosity,
):
    """
    View datasets completeness per query.
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("datasets", onraise=Abort):
        if not valid_name_tag(esg.graph, esg.ui, query_id, None):
            raise Exit(1)
        query = esg.graph.get(query_id)
        datasets: defaultdict[str, DatasetCounter] = defaultdict(
            DatasetCounter
        )
        for file in query.files:
            datasets[file.dataset_id].total += 1
            if file.status == FileStatus.Done:
                datasets[file.dataset_id].done += 1
        if json or yaml:
            datasets_dict = {
                dataset_id: counts.asdict()
                for dataset_id, counts in datasets.items()
            }
            if json:
                esg.ui.print(datasets_dict, json=True)
            elif yaml:
                esg.ui.print(datasets_dict, yaml=True)
        else:
            table = Table(box=MINIMAL_DOUBLE_HEAD, show_edge=False)
            table.add_column("dataset_id", justify="right", style="bold blue")
            table.add_column("done", justify="center")
            table.add_column("total", justify="center")
            table.add_column("complete", justify="center")
            for dataset_id, counts in datasets.items():
                table.add_row(
                    dataset_id,
                    str(counts.done),
                    str(counts.total),
                    str(counts.is_complete()),
                )
            esg.ui.print(table)
