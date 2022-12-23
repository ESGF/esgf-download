import sqlalchemy as sa

from esgpull.models.facet import Facet
from esgpull.models.query import Query, query_tag_proxy
from esgpull.models.selection import selection_facet_proxy
from esgpull.models.tag import Tag

# from esgpull.models.base import Base
# from esgpull.models.query import query_file_proxy
# from esgpull.models.selection import Selection
# from esgpull.models.options import Options
# from esgpull.models.file import File


class tag:
    orphans: sa.Select[tuple[Tag]] = (
        sa.select(Tag).outerjoin(query_tag_proxy).filter_by(tag_sha=None)
    )


class facet:
    all: sa.Select[tuple[Facet]] = sa.select(Facet)
    shas: sa.Select[tuple[str]] = sa.select(Query.sha)
    count: sa.Select[tuple[str, int]] = sa.select(
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
