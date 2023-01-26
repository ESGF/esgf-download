from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, valid_name_tag
from esgpull.models import File, FileStatus
from esgpull.tui import Verbosity
from esgpull.utils import format_size


@click.command()
@args.sha_or_name
@opts.tag
@opts.children
@opts.yes
@opts.verbosity
def update(
    sha_or_name: str | None,
    tag: str | None,
    children: bool,
    yes: bool,
    verbosity: Verbosity,
) -> None:
    esg = Esgpull.with_verbosity(verbosity)
    with esg.ui.logging("update", onraise=Abort):
        if sha_or_name is None and tag is None:
            esg.graph.load_db()
            queries = list(esg.graph.queries.values())
        else:
            if not valid_name_tag(esg.graph, esg.ui, sha_or_name, tag):
                raise Exit(1)
            queries = get_queries(
                esg.graph,
                sha_or_name,
                tag,
                children=children,
            )
        queries = [query for query in queries if query.tracked]
        expanded = [esg.graph.expand(query.sha) for query in queries]
        if not queries:
            esg.ui.print(":stop_sign: Trying to update untracked queries.")
            raise Exit(0)
        hints = esg.context.hints(*expanded, file=True, facets=["index_node"])
        hits: list[int] = []
        for query_hints in hints:
            nodes = query_hints["index_node"]
            query_hits = sum([nodes[node] for node in nodes])
            hits.append(query_hits)
        nb = sum(hits)
        if not nb:
            esg.ui.print("No files found.")
            raise Exit(0)
        else:
            esg.ui.print(f"Found {nb} files.")
        results = []
        for query_hits, query_hints, query in zip(hits, hints, expanded):
            file_results = esg.context.prepare_search_distributed(
                query,
                file=True,
                hints=[query_hints],
                max_hits=None,
                fields_param=["*"],
            )
            nb_req = len(file_results)
            if nb_req > 50:
                query_name = f"[b green]{query.name}[/]"
                msg = (
                    f"{nb_req} requests will be sent to ESGF to"
                    f" update {query_name}. Send anyway?"
                )
                if not esg.ui.ask(msg, default=False):
                    esg.ui.print(f"{query_name} is no longer tracked.")
                    query.tracked = False
                    file_results = []
            results.append(file_results)
        # TODO: dry_run to print urls here?
        with esg.ui.spinner("Fetching files"):
            coros = []
            for query_results in results:
                coro = esg.context._files(*query_results, keep_duplicates=True)
                coros.append(coro)
            files: list[list[File]] = esg.context.sync_gather(*coros)
        new_files: list[list[File]] = []
        for query, query_files in zip(queries, files):
            shas = [f.sha for f in query.files]
            query_new_files: list[File] = []
            for file in query_files:
                if file.sha not in shas:
                    file.status = FileStatus.Queued
                    query_new_files.append(file)
            new_files.append(query_new_files)
        for query, query_new_files in zip(queries, new_files):
            nb_files = len(query_new_files)
            if not query.tracked:
                esg.db.add(query)
                continue
            elif nb_files == 0:
                continue
            esg.ui.print(esg.graph.subgraph(query, parents=True))
            size = sum([file.size for file in query_new_files])
            esg.ui.print(f"{nb_files} new files ({format_size(size)}).")
            if esg.ui.ask("Download new files?", default=True):
                query.files.extend(query_new_files)
                esg.db.add(query)
