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
        queries = [query for query in queries if not query.transient]
        expanded = [esg.graph.expand(query.sha) for query in queries]
        if not queries:
            esg.ui.print(":stop_sign: Trying to update untracked queries.")
            raise Exit(0)
        hits = esg.context.hits(*expanded, file=True)
        nb = sum(hits)
        if not nb:
            esg.ui.print("No files found.")
            raise Exit(0)
        else:
            esg.ui.print(f"Found {nb} files.")
        files_coros = []
        for query_hits, query in zip(hits, expanded):
            if query_hits > 5000:
                nb_req = query_hits // esg.config.search.page_limit
                query_name = f"[b green]{query.name}[/]"
                msg = (
                    f"{nb_req} requests must be sent to ESGF "
                    f"to update {query_name}. Continue?"
                )
                if not esg.ui.ask(msg, default=False):
                    esg.ui.print(f"{query_name} is no longer tracked.")
                    query.transient = True
                    query_hits = 0
            files_coro = esg.context._search_files(
                query,
                hits=[query_hits],
                max_hits=query_hits,
            )
            files_coros.append(files_coro)
        with esg.ui.spinner("Fetching files"):
            queries_files = esg.context.sync_gather(*files_coros)
        new_files: list[list[File]] = []
        for i, files in enumerate(queries_files):
            query = queries[i]
            shas = [f.sha for f in query.files]
            query_new_files: list[File] = []
            for file in files:
                file.compute_sha()
                if file.sha not in shas:
                    file.status = FileStatus.Queued
                    query_new_files.append(file)
            new_files.append(query_new_files)
        for query, query_new_files in zip(queries, new_files):
            nb_files = len(query_new_files)
            if query.transient:
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
