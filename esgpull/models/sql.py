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

    @staticmethod
    @functools.cache
    def linked() -> sa.Select[tuple[File]]:
        return sa.select(query_file_proxy.c.file_sha).distinct()

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
    def shas_from_query(query_sha: str) -> sa.Select[tuple[str]]:
        return sa.select(query_file_proxy.c.file_sha).filter_by(
            query_sha=query_sha
        )

    @staticmethod
    def with_status(*status: FileStatus) -> sa.Select[tuple[File]]:
        return sa.select(File).where(File.status.in_(status))

    @staticmethod
    def with_file_id(file_id: str) -> sa.Select[tuple[str]]:
        return sa.select(File.sha).where(File.file_id == file_id).limit(1)

    @staticmethod
    def total_size_with_status(
        *status: FileStatus,
        query_sha: str | None = None,
    ) -> sa.Select[tuple[int]]:
        """
        This is re-implemented in Query.files_count_size because
        of cyclic import between query.py and the current file.
        """
        stmt = sa.select(sa.func.sum(File.size).where(File.status.in_(status)))
        if query_sha is not None:
            stmt = stmt.join_from(query_file_proxy, File).where(
                query_file_proxy.c.query_sha == query_sha
            )
        return stmt

    @staticmethod
    @functools.cache
    def status_count_size(
        all_: bool = False,
    ) -> sa.Select[tuple[FileStatus, int, int]]:
        stmt = sa.select(
            File.status,
            sa.func.count("*"),
            sa.func.sum(File.size),
        ).group_by(File.status)
        if not all_:
            stmt = stmt.where(File.status != FileStatus.Done)
        return stmt


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
    def with_shas(*shas: str) -> sa.Select[tuple[Query]]:
        if not shas:
            raise ValueError(shas)
        return sa.select(Query).where(Query.sha.in_(shas))

    @staticmethod
    def with_tag(tag: str) -> sa.Select[tuple[Query]]:
        return (
            sa.select(Query)
            .join_from(query_tag_proxy, Tag)
            .join_from(query_tag_proxy, Query)
            .where(Tag.name == tag)
        )

    @staticmethod
    def children(sha: str) -> sa.Select[tuple[Query]]:
        return sa.select(Query).where(Query.require == sha)


class selection:
    @staticmethod
    @functools.cache
    def all() -> sa.Select[tuple[Selection]]:
        return sa.select(Selection)

    @staticmethod
    @functools.cache
    def orphans() -> sa.Select[tuple[Selection]]:
        return (
            sa.select(Selection).outerjoin(Query).where(Query.sha == None)  # noqa
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

    @staticmethod
    def with_ids(*ids: int) -> sa.Select[tuple[SyndaFile]]:
        return sa.select(SyndaFile).where(SyndaFile.file_id.in_(ids))


class query_file:
    @staticmethod
    def link(query: Query, file: File) -> sa.Insert:
        return sa.insert(query_file_proxy).values(
            query_sha=query.sha, file_sha=file.sha
        )

    @staticmethod
    def unlink(query: Query, file: File) -> sa.Delete:
        return (
            sa.delete(query_file_proxy)
            .where(query_file_proxy.c.query_sha == query.sha)
            .where(query_file_proxy.c.file_sha == file.sha)
        )
