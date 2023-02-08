import functools

import sqlalchemy as sa

from esgpull.models import Table
from esgpull.models.facet import Facet
from esgpull.models.file import FileStatus
from esgpull.models.query import File, Query, query_file_proxy, query_tag_proxy
from esgpull.models.selection import Selection, selection_facet_proxy
from esgpull.models.synda_file import SyndaFile
from esgpull.models.tag import Tag


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
    @staticmethod
    @functools.cache
    def all() -> sa.Select[tuple[Facet]]:
        return sa.select(Facet)

    @staticmethod
    @functools.cache
    def shas() -> sa.Select[tuple[str]]:
        return sa.select(Facet.sha)

    @staticmethod
    @functools.cache
    def name_count() -> sa.Select[tuple[str, int]]:
        return sa.select(Facet.name, sa.func.count("*")).group_by(Facet.name)

    @staticmethod
    @functools.cache
    def usage() -> sa.Select[tuple[Facet, int]]:
        return (
            sa.select(Facet, sa.func.count("*"))
            .join(selection_facet_proxy)
            .group_by(Facet.sha)
        )

    @staticmethod
    def known_shas(shas: list[str]) -> sa.Select[tuple[str]]:
        return sa.select(Facet.sha).where(Facet.sha.in_(shas))

    @staticmethod
    @functools.cache
    def names() -> sa.Select[tuple[str]]:
        return sa.select(Facet.name).distinct()

    @staticmethod
    def values(name: str) -> sa.Select[tuple[str]]:
        return sa.select(Facet.value).where(Facet.name == name)


class file:
    @staticmethod
    @functools.cache
    def all() -> sa.Select[tuple[File]]:
        return sa.select(File)

    @staticmethod
    @functools.cache
    def shas() -> sa.Select[tuple[str]]:
        return sa.select(File.sha)

    @staticmethod
    @functools.cache
    def orphans() -> sa.Select[tuple[File]]:
        return (
            sa.select(File)
            .outerjoin(query_file_proxy)
            .filter_by(file_sha=None)
        )

    __dups_cte: sa.CTE = (
        sa.select(File.master_id)
        .group_by(File.master_id)
        .having(sa.func.count("*") > 1)
        .cte()
    )

    @staticmethod
    @functools.cache
    def duplicates() -> sa.Select[tuple[File]]:
        return sa.select(File).join(
            file.__dups_cte,
            File.master_id == file.__dups_cte.c.master_id,
        )

    @staticmethod
    def with_status(*status: FileStatus) -> sa.Select[tuple[File]]:
        return sa.select(File).where(File.status.in_(status))

    @staticmethod
    @functools.cache
    def status_count_size() -> sa.Select[tuple[FileStatus, int, int]]:
        return (
            sa.select(
                File.status,
                sa.func.count("*"),
                sa.func.sum(File.size),
            )
            .group_by(File.status)
            .where(File.status != FileStatus.Done)
        )


class query:
    @staticmethod
    @functools.cache
    def all() -> sa.Select[tuple[Query]]:
        return sa.select(Query)

    @staticmethod
    @functools.cache
    def shas() -> sa.Select[tuple[str]]:
        return sa.select(Query.sha)

    __tag_query_cte: sa.CTE = (
        sa.select(Tag.name, query_tag_proxy.c.query_sha)
        .join_from(query_tag_proxy, Tag)
        .cte("tag_query_cte")
    )

    __name_cte: sa.CTE = (
        sa.select(__tag_query_cte)
        .group_by(__tag_query_cte.c.name)
        .having(sa.func.count("*") == 1)
        .cte("name_cte")
    )

    __sha_cte: sa.CTE = (
        sa.select(__tag_query_cte)
        .group_by(__tag_query_cte.c.query_sha)
        .having(sa.func.count("*") == 1)
        .cte("sha_cte")
    )

    @staticmethod
    @functools.cache
    def name_sha() -> sa.Select[tuple[str, str]]:
        return sa.select(query.__name_cte).join(
            query.__sha_cte,
            query.__name_cte.c.query_sha == query.__sha_cte.c.query_sha,
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
    @staticmethod
    @functools.cache
    def all() -> sa.Select[tuple[Selection]]:
        return sa.select(Selection)

    @staticmethod
    @functools.cache
    def orphans() -> sa.Select[tuple[Selection]]:
        return (
            sa.select(Selection)
            .outerjoin(Query)
            .where(Query.sha == None)  # noqa
        )


class tag:
    @staticmethod
    @functools.cache
    def all() -> sa.Select[tuple[Tag]]:
        return sa.select(Tag)

    @staticmethod
    @functools.cache
    def shas() -> sa.Select[tuple[str]]:
        return sa.select(Tag.sha)

    @staticmethod
    @functools.cache
    def orphans() -> sa.Select[tuple[Tag]]:
        return (
            sa.select(Tag).outerjoin(query_tag_proxy).filter_by(tag_sha=None)
        )


class synda_file:
    @staticmethod
    @functools.cache
    def all() -> sa.Select[tuple[SyndaFile]]:
        return sa.select(SyndaFile)

    @staticmethod
    @functools.cache
    def ids() -> sa.Select[tuple[int]]:
        return sa.select(SyndaFile.file_id)
