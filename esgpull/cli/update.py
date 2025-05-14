from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.context import HintsDict, ResultSearch
from esgpull.exceptions import UnsetOptionsError
from esgpull.models import File, FileStatus, Query
from esgpull.tui import Verbosity, logger
from esgpull.utils import format_size


@dataclass
class QueryFiles:
    query: Query
    expanded: Query
    skip: bool = False
    files: list[File] = field(default_factory=list)
    hits: int = field(init=False)
    hints: HintsDict = field(init=False)
    results: list[ResultSearch] = field(init=False)


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
        hints = esg.context.hints(
            *[qf.expanded for qf in qfs],
            file=True,
            facets=["index_node"],
        )
        for qf, qf_hints in zip(qfs, hints):
            qf.hits = sum(esg.context.hits_from_hints(qf_hints))
            if qf_hints:
                qf.hints = qf_hints
            else:
                qf.skip = True
        for qf in qfs:
            s = "s" if qf.hits > 1 else ""
            esg.ui.print(f"{qf.query.rich_name} -> {qf.hits} file{s}.")
        total_hits = sum([qf.hits for qf in qfs])
        if total_hits == 0:
            esg.ui.print("No files found.")
            esg.ui.raise_maybe_record(Exit(0))
        else:
            esg.ui.print(f"{total_hits} files found.")
        qfs = [qf for qf in qfs if not qf.skip]
        # Prepare optimally distributed requests to ESGF
        # [?] TODO: fetch FastFile first to determine what to fetch in detail later
        #   It might be interesting for the special case where all files already
        #   exist in db, then the detailed fetch could be skipped.
        for qf in qfs:
            if esg.config.api.use_custom_distribution_algorithm:
                qf_results = esg.context.prepare_search_distributed(
                    qf.expanded,
                    file=True,
                    hints=[qf.hints],
                    max_hits=None,
                )
            else:
                qf_results = esg.context.prepare_search(
                    qf.expanded,
                    file=True,
                    hits=[qf.hits],
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
            shas = {f.sha for f in qf.query.files}
            new_files: list[File] = []
            for file in qf.files:
                if file.sha not in shas:
                    new_files.append(file)
            nb_files = len(new_files)
            if not qf.query.tracked:
                esg.db.add(qf.query)
                continue
            elif nb_files == 0:
                esg.ui.print(f"{qf.query.rich_name} is already up-to-date.")
                continue
            size = sum([file.size for file in new_files])
            msg = (
                f"\nUpdating {qf.query.rich_name} with {nb_files}"
                f" new files ({format_size(size)})."
                "\nSend to download queue?"
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
                with esg.db.commit_context():
                    for file in esg.ui.track(
                        new_files,
                        description=qf.query.rich_name,
                    ):
                        file_db = esg.db.get(File, file.sha)
                        if file_db is None:
                            if esg.db.has_file_id(file):
                                logger.error(
                                    "File id already exists in database, "
                                    "there might be an error with its checksum"
                                    f"\n{file}"
                                )
                                continue
                            file.status = FileStatus.Queued
                            esg.db.session.add(file)
                        elif has_legacy and legacy in file_db.queries:
                            esg.db.unlink(query=legacy, file=file_db)
                        esg.db.link(query=qf.query, file=file)
                    qf.query.updated_at = datetime.now(timezone.utc)
                    esg.db.session.add(qf.query)
        esg.ui.raise_maybe_record(Exit(0))
