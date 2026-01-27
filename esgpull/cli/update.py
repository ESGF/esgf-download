from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.context import HintsDict
from esgpull.exceptions import UnsetOptionsError
from esgpull.graph import Graph
from esgpull.models import Dataset, File, FileStatus, Query
from esgpull.tui import UI, Verbosity
from esgpull.utils import format_size


@dataclass
class QueryFiles:
    query: Query
    expanded: Query
    skip: bool = False
    datasets: list[Dataset] = field(default_factory=list)
    files: list[File] = field(default_factory=list)
    dataset_hits: int = field(init=False)
    hits: int = field(init=False)
    hints: HintsDict | None = field(init=False)
    # results: list[ResultSearch] | list[PreparedRequest] = field(init=False)
    # dataset_results: list[ResultSearch] | list[PreparedRequest] = field(
    #     init=False
    # )


def parse_queries(
    graph: Graph,
    ui: UI,
    query_id: str | None,
    tag: str | None,
    children: bool,
) -> Iterator[Query]:
    """Select which queries to update + setup."""
    if query_id is None and tag is None:
        graph.load_db()
        yield from graph.queries.values()
    elif not valid_name_tag(graph, ui, query_id, tag):
        ui.raise_maybe_record(Exit(1))
    else:
        for q in get_queries(
            graph,
            query_id,
            tag,
            children=children,
        ):
            yield q


def keep_tracked(qs: Iterator[Query], graph: Graph) -> Iterator[QueryFiles]:
    for q in qs:
        expanded = graph.expand(q.sha)
        if not expanded.tracked:
            continue
        if q.sha == "LEGACY":
            continue
        if not expanded.trackable():
            raise UnsetOptionsError(q.name)
        yield QueryFiles(q, expanded)


@click.command()
@args.query_id
@opts.tag
@opts.children
@opts.yes
@opts.record
@opts.verbosity
def update(
    query_id: str | None,
    tag: str | None,
    children: bool,
    yes: bool,
    record: bool,
    verbosity: Verbosity,
) -> None:
    """
    Fetch files, link files <-> queries, send files to download queue
    """
    esg = init_esgpull(verbosity, record=record)
    with esg.ui.logging("update", onraise=Abort):
        queries: Iterator[Query] = parse_queries(
            esg.graph,
            esg.ui,
            query_id,
            tag,
            children,
        )
        qfs = list(keep_tracked(queries, esg.graph))
        if not qfs:
            esg.ui.print(":stop_sign: Trying to update untracked queries.")
            esg.ui.raise_maybe_record(Exit(0))
        if any(qf.query.backend == ApiBackend.solr for qf in qfs):
            esg.context._solr.probe()
        hints = [None for _ in qfs]
        hits = esg.context.hits(
            *[qf.expanded for qf in qfs],
            file=True,
        )
        dataset_hits = esg.context.hits(
            *[qf.expanded for qf in qfs],
            file=False,
        )
        for qf, qf_hits, qf_hints, qf_dataset_hits in zip(
            qfs, hits, hints, dataset_hits
        ):
            qf.hits = qf_hits
            qf.hints = qf_hints
            qf.dataset_hits = qf_dataset_hits
            if not qf.hits:
                qf.skip = True
                continue
            s = "s" if qf.hits > 1 else ""
            esg.ui.print(
                f"{qf.query.rich_name} -> {qf.hits} file{s} (before replica de-duplication)."
            )
        total_hits = sum([qf.hits for qf in qfs])
        if total_hits == 0:
            esg.ui.print("No files found.")
            esg.ui.raise_maybe_record(Exit(0))
        elif len(qfs) > 1:
            esg.ui.print(f"{total_hits} files found.")
        qfs = [qf for qf in qfs if not qf.skip]
        # Prepare optimally distributed requests to ESGF
        # [?] TODO: fetch FastFile first to determine what to fetch in detail later
        #   It might be interesting for the special case where all files already
        #   exist in db, then the detailed fetch could be skipped.
        nb_files_requests = esg.context.number_of_requests(
            *[qf.expanded for qf in qfs],
            file=True,
            max_hits=None,
        )
        nb_datasets_requests = esg.context.number_of_requests(
            *[qf.expanded for qf in qfs],
            file=False,
            max_hits=None,
        )
        for qf, nb_files_req, nb_datasets_req in zip(
            qfs,
            nb_files_requests,
            nb_datasets_requests,
        ):
            nb_req = nb_datasets_req + nb_files_req
            if nb_req > 50:
                msg = (
                    f"{nb_req} requests will be sent to ESGF to"
                    f" update {qf.query.rich_name}. Send anyway?"
                )
                choice1: Literal["y", "n", "u"] = esg.ui.choice(
                    msg, ["y", "n", "u"], default="n"
                )
                match choice1:
                    case "u":
                        esg.ui.print(f"{qf.query.rich_name} is now untracked.")
                        qf.query.tracked = False
                        esg.db.add(qf.query)
                        qf.skip = True
                    case "n":
                        qf.skip = True
                    case "y":
                        ...
        qfs = [qf for qf in qfs if not qf.skip]
        # Fetch files and update db
        # [?] TODO: dry_run to print urls here
        with esg.ui.spinner("Fetching datasets"):
            for qf in qfs:
                records = esg.context.search(
                    qf.expanded,
                    file=False,
                    max_hits=None,
                    keep_duplicates=False,
                )
                qf.datasets = [
                    Dataset(
                        dataset_id=record.dataset_id,
                        total_files=record.number_of_files,
                    )
                    for record in records
                ]
        with esg.ui.spinner("Fetching files"):
            for qf in qfs:
                files = esg.context.search(
                    qf.expanded,
                    file=True,
                    max_hits=None,
                )
                qf.files = list(files)
        for qf in qfs:
            with esg.db.commit_context():
                unregistered_datasets = [
                    f for f in qf.datasets if f not in esg.db
                ]
                if len(unregistered_datasets) > 0:
                    esg.ui.print(
                        f"Adding {len(unregistered_datasets)} new datasets to database."
                    )
                    esg.db.session.add_all(unregistered_datasets)
                files_from_db = [
                    esg.db.get(File, f.sha) for f in qf.files if f in esg.db
                ]
                registered_files = [f for f in files_from_db if f is not None]
                unregistered_files = [f for f in qf.files if f not in esg.db]
                if len(unregistered_files) > 0:
                    esg.ui.print(
                        f"Adding {len(unregistered_files)} new files to database."
                    )
                    esg.db.session.add_all(unregistered_files)
                files = registered_files + unregistered_files
            not_done_files = [
                file for file in files if file.status != FileStatus.Done
            ]
            download_size = sum(file.size for file in not_done_files)
            msg = (
                f"\n{qf.query.rich_name}: {len(not_done_files)} "
                f" files ({format_size(download_size)}) to download."
                f"\nLink to query and send to download queue?"
            )
            choice2: Literal["y", "n", "show"]
            if yes:
                choice2 = "y"
            else:
                while (
                    choice2 := esg.ui.choice(
                        msg,
                        choices=["y", "n", "show"],
                        show_choices=True,
                    )
                ) and choice2 == "show":
                    esg.ui.print(esg.graph.subgraph(qf.query, parents=True))
            if choice2 == "y":
                legacy = esg.legacy_query
                has_legacy = legacy.state.persistent
                applied_changes = False
                with esg.db.commit_context():
                    for file in esg.ui.track(
                        files,
                        description=f"{qf.query.rich_name}",
                    ):
                        if file.status != FileStatus.Done:
                            file.status = FileStatus.Queued
                        if has_legacy and legacy in file.queries:
                            _ = esg.db.unlink(query=legacy, file=file)
                        changed = esg.db.link(query=qf.query, file=file)
                        applied_changes = applied_changes or changed
                    if applied_changes:
                        qf.query.updated_at = datetime.now(timezone.utc)
                    esg.db.session.add(qf.query)
        esg.ui.raise_maybe_record(Exit(0))
