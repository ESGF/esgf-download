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

queries = sa.select(Query)

# orphan_tags = session.scalars(...).all()
orphan_tags = sa.select(Tag).outerjoin(query_tag_proxy).filter_by(tag_sha=None)

# facet_count = dict(session.execute(...).all())
facet_count = sa.select(Facet.name, sa.func.count("*")).group_by(Facet.name)

# facet_usage = {}
# for facet, count in session.execute(...).all():
#     facet_usage.setdefault(facet.name, {})
#     facet_usage[facet.name][facet.value] = count
facet_usage = (
    sa.select(Facet, sa.func.count("*"))
    .join(selection_facet_proxy)
    .group_by(Facet.sha)
)

# shas = session.scalars(...).all()
shas = sa.select(Query.sha)

# name_sha = dict(session.execute(...).all())
name_sha = (
    sa.select(Tag.name, Query.sha)
    .join_from(query_tag_proxy, Query)
    .join_from(query_tag_proxy, Tag)
    .group_by(Query.sha)
    .having(sa.func.count("*") == 1)
)
