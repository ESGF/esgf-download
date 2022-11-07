from time import time

import pytest

from esgpull.context import Context
from esgpull.esgpull import Esgpull


@pytest.fixture
def base(root):
    return Context()


@pytest.fixture
def cmip6_ipsl(root):
    result = Context()
    result.query.mip_era = "CMIP6"
    result.query.institution_id = "IPSL"
    return result


@pytest.fixture(params=["base", "cmip6_ipsl"])
def context(request):
    return request.getfixturevalue(request.param)


def test_multi_index(base):
    index_nodes = ["esgf-node.ipsl.upmc.fr", "esgf-data.dkrz.de"]
    for index_node in index_nodes:
        query = base.query.add()
        query.index_node = index_node
    queries = base._build_queries(file=False)
    assert len(queries) == 2
    for query, expected in zip(queries, index_nodes):
        assert "url" in query and expected in query["url"]


def test_adjust_hits(base):
    variable_ids = ["tas", "tasmin"]
    for variable_id in variable_ids:
        query = base.query.add()
        query.variable_id = variable_id
    hits = base.hits
    batchsize = 5
    first_30 = base._build_queries_search(
        hits=hits,
        file=False,
        batchsize=batchsize,
        max_results=len(variable_ids) * 10,
    )
    assert len(first_30) == len(variable_ids) * 2
    for i, variable_id in enumerate(variable_ids):
        for j in range(2):
            expected = dict(
                offset=j * batchsize,
                limit=batchsize,
                query=f"variable_id:{variable_id}",
            )
            assert expected.items() < first_30[i * 2 + j].items()
    offset_100 = base._build_queries_search(
        hits=hits,
        file=False,
        batchsize=batchsize,
        max_results=len(variable_ids) * 10,
        offset=100,
    )
    assert len(offset_100) == len(variable_ids) * 2
    for i, variable_id in enumerate(variable_ids):
        for j in range(2):
            expected = dict(
                offset=50 + j * batchsize,
                limit=batchsize,
                query=f"variable_id:{variable_id}",
            )
            assert expected.items() < offset_100[i * 2 + j].items()


def test_adjust_hits_produces_1_result_per_query(base):
    variable_ids = ["c2h2", "c2h6", "c3h6"]
    for variable_id in variable_ids:
        query = base.query.add()
        query.variable_id = variable_id
    results = base.search(max_results=len(variable_ids))
    assert len(results) == len(variable_ids)
    for result, expected in zip(results, variable_ids):
        assert result["variable_id"][0] == expected


@pytest.mark.slow
def test_better_distrib(base):
    base.config.search.http_timeout = 60
    base.distrib = True
    # configure terms to target ~1000 files across different index nodes
    base.query.mip_era = "CMIP6"
    base.query.table_id = "Amon"
    base.query.variable_id = "tas"
    base.query.member_id = "r20i1p1f1"
    start = time()
    regular_results = base.search(file=True, max_results=None)
    regular_elapsed = time() - start
    esg = Esgpull()
    base.index_nodes = esg.fetch_index_nodes()
    base.semaphores = {}  # semaphores were bound to the previous event loop
    start = time()
    better_results = base.search(file=True, max_results=None)
    better_elapsed = time() - start
    regular_checksums = set(doc["checksum"][0] for doc in regular_results)
    better_checksums = set(doc["checksum"][0] for doc in better_results)
    assert regular_checksums == better_checksums
    assert regular_elapsed > better_elapsed


def test_ipsl_hits_between_1_and_2_million(cmip6_ipsl):
    assert 1_000_000 < cmip6_ipsl.hits[0] < 2_000_000


def test_more_files_than_datasets(context):
    assert sum(context.hits) < sum(context.file_hits)


@pytest.mark.slow
def test_options_dont_include_already_set_facets(cmip6_ipsl):
    options = cmip6_ipsl.options()[0]
    for facet in cmip6_ipsl.query:
        assert facet.name not in options.keys()
