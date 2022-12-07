import sqlalchemy as sa

from esgpull.models.query import query_tag_proxy
from esgpull.models.tag import Tag

# from esgpull.models.base import Base
# from esgpull.models.query import query_file_proxy, Query
# from esgpull.models._select import Select, select_facet_proxy
# from esgpull.models.options import Options
# from esgpull.models.facet import Facet
# from esgpull.models.file import File


orphan_tags = (
    sa.select(Tag)
    .join(
        query_tag_proxy,
        isouter=True,
    )
    .filter_by(tag_sha=None)
)
