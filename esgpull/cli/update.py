from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.context import HintsDict, ResultSearch
from esgpull.context.solr import SolrContext
from esgpull.context.stac import PreparedRequest, StacContext
from esgpull.exceptions import UnsetOptionsError
from esgpull.models import Dataset, File, FileStatus, Query
from esgpull.models.query import ApiBackend
from esgpull.tui import Verbosity
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
    results: list[ResultSearch] | list[PreparedRequest] = field(init=False)
    dataset_results: list[ResultSearch] | list[PreparedRequest] = field(
        init=False
    )
    ctx: SolrContext | StacContext = field(init=False)


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
        # Select which queries to update + setup
        if query_id is None and tag is None:
            esg.graph.load_db()
            queries = list(esg.graph.queries.values())
        else:
            if not valid_name_tag(esg.graph, esg.ui, query_id, tag):
                esg.ui.raise_maybe_record(Exit(1))
            queries = get_queries(
                esg.graph,
                query_id,
                tag,
                children=children,
            )
        qfs: list[QueryFiles] = []
        for query in queries:
            expanded = esg.graph.expand(query.sha)
            if query.tracked and not expanded.trackable():
                esg.ui.print(query)
                raise UnsetOptionsError(query.name)
            elif query.tracked:
                qfs.append(QueryFiles(query, expanded))
        queries = [
            query
            for query in queries
            if query.tracked and query.sha != "LEGACY"
        ]
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
            if not qf_hits:
                qf.skip = True
            match qf.query.backend or ApiBackend.default():
                case ApiBackend.solr:
                    qf.ctx = esg.context._solr
                case ApiBackend.stac:
                    qf.ctx = esg.context._stac
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
        for qf in qfs:
            qf_dataset_results = qf.ctx.prepare_search(
                qf.expanded,
                file=False,
                hits=[qf.dataset_hits],
                max_hits=None,
            )
            qf_results = qf.ctx.prepare_search(
                qf.expanded,
                file=True,
                hits=[qf.hits],
                max_hits=None,
            )
            match qf.query.backend or ApiBackend.default():
                case ApiBackend.solr:
                    nb_req = len(qf_dataset_results) + len(qf_results)
                case ApiBackend.stac:
                    nb_datasets_req = (
                        qf.dataset_hits // qf_dataset_results[0].limit
                    )
                    nb_files_req = qf.hits // qf_results[0].limit
                    nb_req = nb_datasets_req + nb_files_req
            if nb_req > 50:
                msg = (
                    f"{nb_req} requests will be sent to ESGF to"
                    f" update {qf.query.rich_name}. Send anyway?"
                )
                match esg.ui.choice(msg, ["y", "n", "u"], default="n"):
                    case "u":
                        esg.ui.print(f"{qf.query.rich_name} is now untracked.")
                        qf.query.tracked = False
                        qf_results = []
                        qf_dataset_results = []
                    case "n":
                        qf_results = []
                        qf_dataset_results = []
                    case _:
                        ...
            qf.results = qf_results
            qf.dataset_results = qf_dataset_results
        # Fetch files and update db
        # [?] TODO: dry_run to print urls here
        with esg.ui.spinner("Fetching datasets"):
            coros = []
            for qf in qfs:
                coro = qf.ctx._datasets(
                    *qf.dataset_results, keep_duplicates=False
                )
                coros.append(coro)
            datasets = qf.ctx.sync_gather(*coros)
            for qf, qf_datasets in zip(qfs, datasets):
                qf.datasets = [
                    Dataset(
                        dataset_id=record.dataset_id,
                        total_files=record.number_of_files,
                    )
                    for record in qf_datasets
                ]
        with esg.ui.spinner("Fetching files"):
            coros = []
            for qf in qfs:
                coro = qf.ctx._files(*qf.results, keep_duplicates=False)
                coros.append(coro)
            files = qf.ctx.sync_gather(*coros)
            for qf, qf_files in zip(qfs, files):
                qf.files = qf_files
        for qf in qfs:
            if not qf.query.tracked:
                esg.db.add(qf.query)
                continue
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
            if yes:
                choice = "y"
            else:
                while (
                    choice := esg.ui.choice(
                        msg,
                        choices=["y", "n", "show"],
                        show_choices=True,
                    )
                ) and choice == "show":
                    esg.ui.print(esg.graph.subgraph(qf.query, parents=True))
            if choice == "y":
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
