from __future__ import annotations

from dataclasses import dataclass, field

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.context import HintsDict, ResultSearch
from esgpull.models import File, FileStatus, Query
from esgpull.tui import Verbosity
from esgpull.utils import format_size


@dataclass
class QueryFiles:
    query: Query
    expanded: Query
    files: list[File] = field(default_factory=list)
    hints: HintsDict = field(init=False)
    results: list[ResultSearch] = field(init=False)


@click.command()
@args.sha_or_name
@opts.tag
@opts.children
@opts.yes
@opts.record
@opts.verbosity
def update(
    sha_or_name: str | None,
    tag: str | None,
    children: bool,
    yes: bool,
    record: bool,
    verbosity: Verbosity,
) -> None:
    esg = init_esgpull(verbosity, record=record)
    with esg.ui.logging("update", onraise=Abort):
        # Select which queries to update + setup
        if sha_or_name is None and tag is None:
            esg.graph.load_db()
            queries = list(esg.graph.queries.values())
        else:
            if not valid_name_tag(esg.graph, esg.ui, sha_or_name, tag):
                esg.ui.raise_maybe_record(Exit(1))
            queries = get_queries(
                esg.graph,
                sha_or_name,
                tag,
                children=children,
            )
        qfs: list[QueryFiles] = []
        for query in queries:
            if query.tracked:
                qfs.append(QueryFiles(query, esg.graph.expand(query.sha)))
        queries = [
            query
            for query in queries
            if query.tracked and query.sha != "LEGACY"
        ]
        if not qfs:
            esg.ui.print(":stop_sign: Trying to update untracked queries.")
            esg.ui.raise_maybe_record(Exit(0))
        hints = esg.context.hints(
            *[qf.expanded for qf in qfs],
            file=True,
            facets=["index_node"],
        )
        for qf, qf_hints in zip(qfs, hints):
            qf.hints = qf_hints
        nb = sum(esg.context.hits_from_hints(*hints))
        if not nb:
            esg.ui.print("No files found.")
            esg.ui.raise_maybe_record(Exit(0))
        else:
            esg.ui.print(f"Found {nb} files.")
        # Prepare optimally distributed requests to ESGF
        # [?] TODO: fetch FastFile first to determine what to fetch in detail later
        #   It might be interesting for the special case where all files already
        #   exist in db, then the detailed fetch could be skipped.
        for qf in qfs:
            qf_results = esg.context.prepare_search_distributed(
                qf.expanded,
                file=True,
                hints=[qf.hints],
                max_hits=None,
            )
            nb_req = len(qf_results)
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
                    case "n":
                        qf_results = []
                    case _:
                        ...
            qf.results = qf_results
        # Fetch files and update db
        # [?] TODO: dry_run to print urls here
        with esg.ui.spinner("Fetching files"):
            coros = []
            for qf in qfs:
                coro = esg.context._files(*qf.results, keep_duplicates=False)
                coros.append(coro)
            files = esg.context.sync_gather(*coros)
            for qf, qf_files in zip(qfs, files):
                qf.files = qf_files
        for qf in qfs:
            shas = set([f.sha for f in qf.query.files])
            new_files: list[File] = []
            for file in qf.files:
                if file.sha not in shas:
                    new_files.append(file)
            nb_files = len(new_files)
            if not qf.query.tracked:
                esg.db.add(qf.query)
                continue
            elif nb_files == 0:
                esg.ui.print(f"{query.rich_name} is already up-to-date.")
                continue
            esg.ui.print(esg.graph.subgraph(qf.query, parents=True))
            size = sum([file.size for file in new_files])
            esg.ui.print(f"{nb_files} new files ({format_size(size)}).")
            if esg.ui.ask("Send to download queue?", default=True):
                legacy = esg.legacy_query
                has_legacy = legacy.state.persistent
                for file in new_files:
                    file_db = esg.db.get(File, file.sha)
                    if file_db is None:
                        file.status = FileStatus.Queued
                        file_db = esg.db.merge(file)
                    elif has_legacy and legacy in file_db.queries:
                        file_db.queries.remove(legacy)
                    file_db.queries.append(qf.query)
                    esg.db.add(file_db)
        esg.ui.raise_maybe_record(Exit(0))
