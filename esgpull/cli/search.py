from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull import Esgpull
from esgpull.cli.decorators import args, groups, opts
from esgpull.cli.utils import filter_keys, parse_query, totable, yaml_syntax
from esgpull.exceptions import SliceIndexError
from esgpull.models import Query
from esgpull.tui import Verbosity


@click.command()
@args.facets
@groups.query_def
@groups.display
# @opts.date
# @opts.data_node
@opts.dry_run
@opts.dump
@opts.file
@opts.hints
@opts.json
@opts.show
@opts.yes
@opts.verbosity
# @opts.selection_file
def search(
    facets: list[str],
    # query options
    tags: list[str],
    require: str | None,
    distrib: str | None,
    latest: str | None,
    replica: str | None,
    retracted: str | None,
    # since: str | None,
    # display
    all_: bool,
    zero: bool,
    one: bool,
    slice_: slice,
    # ungrouped
    # date: bool,
    # data_node: bool,
    dry_run: bool,
    dump: bool,
    file: bool,
    hints: list[str] | None,
    json: bool,
    show: bool,
    yes: bool,
    # selection_file: str | None,
    verbosity: Verbosity,
) -> None:
    """
    Search datasets and files on ESGF

    More info
    """
    esg = Esgpull.with_verbosity(verbosity)
    # TODO: bug with slice_:
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
        esg.graph.add(query, force=True)
        query = esg.graph.expand(query.sha)
        hits = esg.context.hits(query, file=file)
        nb = sum(hits)
        if zero:
            slice_ = slice(0, 0)
        elif one:
            slice_ = slice(0, 1)
        elif all_:
            slice_ = slice(0, nb)
        offset = slice_.start
        max_hits = slice_.stop - slice_.start
        if nb > 0 and slice_.start >= nb:
            raise SliceIndexError(slice_, nb)
        if dry_run:
            search_results = esg.context._init_search(
                query,
                file=file,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
            )
            for result in search_results:
                esg.ui.print(result.request.url)
            raise Exit(0)
        if hints is not None:
            if hints[0] in "/?.":
                not_distrib_query = query << Query(options=dict(distrib=False))
                facet_counts = esg.context.hints(
                    not_distrib_query,
                    file=file,
                    facets=["*"],
                )
                esg.ui.print(list(facet_counts[0]), json=True)
                raise Exit(0)
            facet_counts = esg.context.hints(query, file=file, facets=hints)
            esg.ui.print(facet_counts, json=True)
            raise Exit(0)
        if dump:
            if json:
                esg.ui.print(query.asdict(), json=True)
            else:
                esg.ui.print(yaml_syntax(query.asdict()))
            raise Exit(0)
        if show:
            esg.ui.print(query)
            raise Exit(0)
        if max_hits > 200 and not yes:
            nb_req = max_hits // esg.config.search.page_limit
            message = f"{nb_req} requests will be send to ESGF. Continue?"
            if not esg.ui.ask(message, default=True):
                raise Abort
        results = esg.context.search(
            query,
            file=file,
            hits=hits,
            offset=offset,
            max_hits=max_hits,
        )
        if json:
            esg.ui.print([f.asdict() for f in results], json=True)
            raise Exit(0)
        f_or_d = "file" if file else "dataset"
        s = "s" if nb != 1 else ""
        esg.ui.print(f"Found {nb} {f_or_d}{s}.")
        if results:
            docs = filter_keys(
                results,
                # data_node=data_node,
                # date=date,
                offset=offset,
            )
            esg.ui.print(totable(docs))
