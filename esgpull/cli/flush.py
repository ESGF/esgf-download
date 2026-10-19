from __future__ import annotations

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.models import File, FileStatus, sql
from esgpull.tui import Verbosity
from esgpull.utils import format_size


@click.command()
@args.query_id
@opts.tag
@opts.children
@opts.all
@opts.orphans
@opts.yes
@opts.verbosity
def flush(
    query_id: str | None,
    tag: str | None,
    children: bool,
    all_: bool,
    orphans: bool,
    yes: bool,
    verbosity: Verbosity,
) -> None:
    """
    Remove files from the database (without touching the filesystem)

    Files flushed from the `file` table are re-fetched and replaced on the
    next `update`, for instance to fix an outdated `local_path` or a dead
    `data_node`.

    Select which files to flush with exactly one target:

      esgpull flush <query_id>    files linked to a query
      esgpull flush --tag <tag>   files linked to queries with a tag
      esgpull flush --all         every file in the database
      esgpull flush --orphans     files not linked to any query

    Flushing never deletes files from disk.
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("flush", onraise=Abort):
        targeted = query_id is not None or tag is not None
        if sum([targeted, all_, orphans]) != 1:
            raise click.UsageError(
                "Select exactly one of: <query_id>/--tag, --all, --orphans."
            )
        if targeted:
            if not valid_name_tag(esg.graph, esg.ui, query_id, tag):
                raise Exit(1)
            queries = get_queries(
                esg.graph,
                query_id,
                tag,
                children=children,
            )
            shas: set[str] = set()
            for query in queries:
                shas.update(
                    esg.db.scalars(sql.file.shas_from_query(query.sha))
                )
            files = [
                file
                for file in (esg.db.get(File, sha) for sha in shas)
                if file is not None
            ]
        elif orphans:
            files = list(esg.db.scalars(sql.file.orphans()))
        else:  # all_
            files = list(esg.db.scalars(sql.file.all()))
        if not files:
            esg.ui.print("Nothing to flush.")
            raise Exit(0)
        nb = len(files)
        s = "s" if nb > 1 else ""
        esg.ui.print(f"Flush {nb} file{s} from the database?")
        done = [file for file in files if file.status == FileStatus.Done]
        if done:
            size_done = sum(file.size for file in done)
            esg.ui.print(
                f":warning: {len(done)} downloaded file{s}"
                f" ({format_size(size_done)}) stay on disk and"
                " will be re-fetched on next update."
            )
        if not yes and not esg.ui.ask("Continue?", default=True):
            raise Abort
        esg.db.delete(*files)
        esg.ui.print(":+1:")
