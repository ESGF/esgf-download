import sqlalchemy as sa

from esgpull.models import Table
from esgpull.models.facet import Facet
from esgpull.models.file import FileStatus
from esgpull.models.query import File, Query, query_file_proxy, query_tag_proxy
from esgpull.models.selection import Selection, selection_facet_proxy
from esgpull.models.synda_file import SyndaFile
from esgpull.models.tag import Tag

# from esgpull.models.options import Options


def count(item: Table) -> sa.Select[tuple[int]]:
    table = item.__class__
    return (
        sa.select(sa.func.count("*"))
        .select_from(table)
        .filter_by(sha=item.sha)
    )


def count_table(table: type[Table]) -> sa.Select[tuple[int]]:
    return sa.select(sa.func.count("*")).select_from(table)


class facet:
    all: sa.Select[tuple[Facet]] = sa.select(Facet)
    shas: sa.Select[tuple[str]] = sa.select(Facet.sha)
    name_count: sa.Select[tuple[str, int]] = sa.select(
        Facet.name, sa.func.count("*")
    ).group_by(Facet.name)
    usage: sa.Select[tuple[Facet, int]] = (
        sa.select(Facet, sa.func.count("*"))
        .join(selection_facet_proxy)
        .group_by(Facet.sha)
    )

    @staticmethod
    def known_shas(shas: list[str]) -> sa.Select[tuple[str]]:
        return sa.select(Facet.sha).where(Facet.sha.in_(shas))

    names: sa.Select[tuple[str]] = sa.select(Facet.name).distinct()

    @staticmethod
    def values(name: str) -> sa.Select[tuple[str]]:
        return sa.select(Facet.value).where(Facet.name == name)


class file:
    all: sa.Select[tuple[File]] = sa.select(File)
    shas: sa.Select[tuple[str]] = sa.select(File.sha)
    orphans: sa.Select[tuple[File]] = (
        sa.select(File).outerjoin(query_file_proxy).filter_by(file_sha=None)
    )
    __dups_cte = (
        sa.select(File.master_id)
        .group_by(File.master_id)
        .having(sa.func.count("*") > 1)
        .cte()
    )
    duplicates: sa.Select[tuple[File]] = sa.select(File).join(
        __dups_cte, File.master_id == __dups_cte.c.master_id
    )

    @staticmethod
    def with_status(*status: FileStatus) -> sa.Select[tuple[File]]:
        return sa.select(File).where(File.status.in_(status))

    status_count_size: sa.Select[tuple[FileStatus, int, int]] = (
        sa.select(
            File.status,
            sa.func.count("*"),
            sa.func.sum(File.size),
        )
        .group_by(File.status)
        .where(File.status != FileStatus.Done)
    )


class query:
    all: sa.Select[tuple[Query]] = sa.select(Query)
    shas: sa.Select[tuple[str]] = sa.select(Query.sha)
    __tag_query_cte = (
        sa.select(Tag.name, Query.sha)
        .join_from(query_tag_proxy, Tag)
        .join_from(query_tag_proxy, Query)
        .cte("tag_query_cte")
    )
    __name_cte = (
        sa.select(__tag_query_cte)
        .group_by(__tag_query_cte.c.name)
        .having(sa.func.count("*") == 1)
        .cte("name_cte")
    )
    __sha_cte = (
        sa.select(__tag_query_cte)
        .group_by(__tag_query_cte.c.sha)
        .having(sa.func.count("*") == 1)
        .cte("sha_cte")
    )
    name_sha: sa.Select[tuple[str, str]] = sa.select(__name_cte).join(
        __sha_cte, __name_cte.c.sha == __sha_cte.c.sha
    )

    @staticmethod
    def with_tag(tag: str) -> sa.Select[tuple[Query]]:
        return (
            sa.select(Query)
            .join_from(query_tag_proxy, Tag)
            .join_from(query_tag_proxy, Query)
            .where(Tag.name == tag)
        )


class selection:
    all: sa.Select[tuple[Selection]] = sa.select(Selection)
    orphans: sa.Select[tuple[Selection]] = (
        sa.select(Selection).outerjoin(Query).where(Query.sha == None)  # noqa
    )


class tag:
    all: sa.Select[tuple[Tag]] = sa.select(Tag)
    shas: sa.Select[tuple[str]] = sa.select(Tag.sha)
    orphans: sa.Select[tuple[Tag]] = (
        sa.select(Tag).outerjoin(query_tag_proxy).filter_by(tag_sha=None)
    )


class synda_file:
    all: sa.Select[tuple[SyndaFile]] = sa.select(SyndaFile)
    ids: sa.Select[tuple[int]] = sa.select(SyndaFile.file_id)
