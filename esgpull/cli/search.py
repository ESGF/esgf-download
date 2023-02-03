from __future__ import annotations

from datetime import datetime

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import filter_keys, get_command, parse_query, totable
from esgpull.exceptions import PageIndexError
from esgpull.models import Query
from esgpull.tui import TempUI, Verbosity


@click.command()
@args.facets
@groups.query_def
@groups.query_date
@groups.display
# @opts.date
# @opts.data_node
@opts.dry_run
@opts.dump
@opts.file
@opts.facets_hints
@opts.hints
@opts.json
@opts.show
@opts.yes
@opts.record
@opts.verbosity
# @opts.selection_file
def search(
    facets: list[str],
    ## query_def
    tags: list[str],
    require: str | None,
    distrib: str | None,
    latest: str | None,
    replica: str | None,
    retracted: str | None,
    date_from: datetime | None,
    date_to: datetime | None,
    ## display
    _all: bool,
    zero: bool,
    page: int,
    ## ungrouped
    # date: bool,
    # data_node: bool,
    dry_run: bool,
    dump: bool,
    file: bool,
    facets_hints: bool,
    hints: list[str] | None,
    json: bool,
    show: bool,
    yes: bool,
    # selection_file: str | None,
    record: bool,
    verbosity: Verbosity,
) -> None:
    """
    Search datasets and files on ESGF

    More info
    """
    with TempUI.logging(record=record):
        if record:
            TempUI.print(get_command())
        esg = Esgpull(verbosity=verbosity, record=record)
    # -> numeric ids are not consistent due to sort by instance_id
    with esg.ui.logging("search", onraise=Abort):
        query = parse_query(
            facets=facets,
            tags=tags,
            require=require,
            distrib=distrib,
            latest=latest,
            replica=replica,
            retracted=retracted,
        )
        query.compute_sha()
        esg.graph.resolve_require(query)
        esg.graph.add(query, force=True)
        if not dump and not show:
            query = esg.graph.expand(query.sha)
            hits = esg.context.hits(
                query,
                file=file,
                date_from=date_from,
                date_to=date_to,
            )
            nb = sum(hits)
            page_size = esg.config.cli.page_size
            nb_pages = (nb // page_size) or 1
            offset = page * page_size
            max_hits = min(page_size, nb - offset)
            if page > nb_pages:
                raise PageIndexError(page, nb_pages)
            elif zero:
                max_hits = 0
            elif _all:
                offset = 0
                max_hits = nb
        if dry_run:
            search_results = esg.context.prepare_search(
                query,
                file=file,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
                date_from=date_from,
                date_to=date_to,
            )
            for result in search_results:
                esg.ui.print(result.request.url)
            esg.ui.raise_maybe_record(Exit(0))
        if facets_hints:
            not_distrib_query = query << Query(options=dict(distrib=False))
            facet_counts = esg.context.hints(
                not_distrib_query,
                file=file,
                facets=["*"],
                date_from=date_from,
                date_to=date_to,
            )
            esg.ui.print(list(facet_counts[0]), json=True)
            esg.ui.raise_maybe_record(Exit(0))
        if hints is not None:
            facet_counts = esg.context.hints(
                query,
                file=file,
                facets=hints,
                date_from=date_from,
                date_to=date_to,
            )
            esg.ui.print(facet_counts, json=True)
            esg.ui.raise_maybe_record(Exit(0))
        if dump:
            if json:
                esg.ui.print(query.asdict(), json=True)
            else:
                esg.ui.print(query.asdict(), yaml=True)
            esg.ui.raise_maybe_record(Exit(0))
        if show:
            esg.ui.print(query)
            esg.ui.raise_maybe_record(Exit(0))
        if max_hits > 200 and not yes:
            nb_req = max_hits // esg.config.search.page_limit
            message = f"{nb_req} requests will be sent to ESGF. Send anyway?"
            if not esg.ui.ask(message, default=True):
                esg.ui.raise_maybe_record(Abort)
        results = esg.context.search(
            query,
            file=file,
            hits=hits,
            offset=offset,
            max_hits=max_hits,
            keep_duplicates=True,
            date_from=date_from,
            date_to=date_to,
        )
        if json:
            esg.ui.print([f.asdict() for f in results], json=True)
            esg.ui.raise_maybe_record(Exit(0))
        f_or_d = "file" if file else "dataset"
        s = "s" if nb != 1 else ""
        esg.ui.print(f"Found {nb} {f_or_d}{s}.")
        if results:
            unique_ids = {r.master_id for r in results}
            unique_nodes = {(r.master_id, r.data_node) for r in results}
            needs_data_node = len(unique_nodes) > len(unique_ids)
            docs = filter_keys(results, data_node=needs_data_node)
            esg.ui.print(totable(docs))
        esg.ui.raise_maybe_record(Exit(0))
