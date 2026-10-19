from __future__ import annotations

from datetime import datetime

import click
from click.exceptions import Abort, Exit

from esgpull.cli.decorators import args, opts
from esgpull.cli.utils import get_queries, init_esgpull, valid_name_tag
from esgpull.fs import Filesystem
from esgpull.models import File, FileStatus, Query, sql
from esgpull.tui import FILE_DATE_FORMAT, Verbosity
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

    The list of flushed files still present on disk is written automatically
    to a file in the log directory (the path is shown in the console), since
    it cannot be recovered from the database after deletion.

    Flushing never deletes files from disk.
    """
    esg = init_esgpull(verbosity)
    with esg.ui.logging("flush", onraise=Abort):
        targeted = query_id is not None or tag is not None
        if sum([targeted, all_, orphans]) != 1:
            raise click.UsageError(
                "Select exactly one of: <query_id>/--tag, --all, --orphans."
            )
        selected_shas: set[str] = set()
        if targeted:
            if not valid_name_tag(esg.graph, esg.ui, query_id, tag):
                raise Exit(1)
            queries = get_queries(
                esg.graph,
                query_id,
                tag,
                children=children,
            )
            selected_shas = {query.sha for query in queries}
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
        impacted_shas = set(
            esg.db.scalars(
                sql.query.shas_from_files({file.sha for file in files})
            )
        )
        other_shas = impacted_shas - selected_shas
        if other_shas:
            other_queries = [
                query
                for query in (
                    esg.db.get(Query, sha) for sha in other_shas
                )
                if query is not None
            ]
            if len(other_queries) == 1:
                esg.ui.print(
                    f":warning: also flushed files linked to"
                    f" [b cyan]{other_queries[0].name}[/]"
                )
            else:
                names = ", ".join(
                    f"[b cyan]{query.name}[/]" for query in other_queries
                )
                esg.ui.print(
                    f":warning: also flushed files linked to {names}"
                )
        fs = Filesystem.from_config(esg.config, install=False)
        on_disk = [
            path
            for file in files
            if (path := fs[file].drs).is_file()
        ]
        if not yes and not esg.ui.ask("Continue?", default=True):
            raise Abort
        esg.db.delete(*files)
        esg.ui.print(":+1:")
        if on_disk:
            date = datetime.utcnow().strftime(FILE_DATE_FORMAT)
            out = esg.config.paths.log / ("-".join(["flush", date]) + ".txt")
            out.parent.mkdir(parents=True, exist_ok=True)
            with out.open("w") as f:
                f.write("\n".join(str(path) for path in on_disk) + "\n")
            esg.ui.print(
                f":+1: {len(on_disk)} flushed file{s} stay on disk,"
                f" list written to {out}"
            )
        else:
            esg.ui.print("No flushed files remain on disk.")
