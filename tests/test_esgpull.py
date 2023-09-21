import pytest

from esgpull import Esgpull
from esgpull.models import Query


def test_insert_default_query(root):
    esg = Esgpull(root, install=True)
    default_query = Query(selection=dict(project="IPSL"))
    default_query.compute_sha()
    esg.graph.add(default_query)
    esg.config.api.default_query_id = default_query.sha
    queries = [
        Query(selection=dict(variable="tas")),
        Query(selection=dict(institution_id="toto")),
        Query(selection=dict(time_frequency="day")),
        Query(selection=dict(variable="pr")),
    ]
    for q in queries:
        q.compute_sha()
    queries.append(
        Query(require=queries[-1].sha, selection=dict(member_id="r1i1p1f1"))
    )
    queries[-1].compute_sha()
    queries.append(
        Query(require=queries[-1].sha, selection=dict(table_id="Amon"))
    )
    queries[-1].compute_sha()
    new_queries = esg.insert_default_query(*queries)
    esg.graph.add(*new_queries)
    assert len(esg.graph.queries) == 7
    for query in queries:
        with pytest.raises(KeyError):
            esg.graph.get(query.sha)
    for i, query in enumerate(new_queries):
        assert esg.graph.get(query.sha) == query
        if i in range(4):
            assert query.require == default_query.sha
        elif i == 4:
            assert query.require == new_queries[3].sha
        elif i == 5:
            assert query.require == new_queries[4].sha
