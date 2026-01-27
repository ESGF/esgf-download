from time import perf_counter

import pytest

from esgpull.config import Config
from esgpull.context import Context
from esgpull.models import ApiBackend, Query
from tests.utils import CEDA_NODE, parametrized_index

base_project = Query(selection={"project": "CMIP6"})
empty = Query() << base_project
cmip6_ipsl = (
    Query(
        options={"distrib": False},
        selection={"mip_era": "CMIP6", "institution_id": "IPSL"},
    )
    << base_project
)


@pytest.fixture
def ctx(config: Config):
    return Context(config=config)


def test_multi_index(ctx, empty):
    index_nodes = [CEDA_NODE, DRKZ_NODE]
    results = []
    for index_node in index_nodes:
        query_results = ctx.prepare_hits(
            empty,
            file=False,
            index_node=index_node,
        )
        results.extend(query_results)
    assert len(results) == 2
    for result, index_node in zip(results, index_nodes, strict=False):
        assert index_node in str(result.request.url)
        assert index_node == result.request.headers["host"]


def test_adjust_hits(ctx):
    variable_ids = ["tas", "tasmin"]
    queries = []
    for variable_id in variable_ids:
        query = Query(selection=dict(variable_id=variable_id))
        queries.append(query)
    hits = ctx.hits(*queries, file=False)
    page_limit = 5
    first_20 = ctx.prepare_search(
        *queries,
        file=False,
        hits=hits,
        page_limit=page_limit,
        max_hits=len(variable_ids) * page_limit * 2,
    )
    assert len(first_20) >= len(variable_ids) * 2
    variable_offsets = dict.fromkeys(variable_ids, 0)
    for result in first_20:
        variable_id = result.query.selection.variable_id[0]
        params = dict(result.request.url.params.items())

        assert int(params["offset"]) == variable_offsets[variable_id]
        assert int(params["limit"]) <= page_limit
        assert params["query"] == f"variable_id:{variable_id}"

        variable_offsets[variable_id] += page_limit

    offset_100 = ctx.prepare_search(
        *queries,
        file=False,
        hits=hits,
        offset=100,
        page_limit=page_limit,
        max_hits=len(variable_ids) * page_limit * 2,
    )
    assert len(offset_100) >= len(variable_ids) * 2

    # ensure offsets follow the proportional splitter: sum matches the limit and each bucket stays within 1 of its ideal share
    ideal = {
        variable_id: hits[i] / sum(hits) * 100
        for i, variable_id in enumerate(variable_ids)
    }
    distributed = _distribute_hits_impl(hits, 100)
    variable_offsets = {
        variable_id: distributed[i]
        for i, variable_id in enumerate(variable_ids)
    }
    for result in offset_100:
        variable_id = result.query.selection.variable_id[0]
        params = dict(result.request.url.params.items())
        offset = int(params["offset"])

        assert offset == variable_offsets[variable_id]
        # invariant: within 1 of ideal
        assert abs(offset - ideal[variable_id]) < 1
        assert int(params["limit"]) <= page_limit
        assert params["query"] == f"variable_id:{variable_id}"

        variable_offsets[variable_id] += page_limit
        ideal[variable_id] += page_limit


class Timer:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, *excs):
        self.duration = perf_counter() - self.start


@parametrized_index
@pytest.mark.parametrize("query", [cmip6_ipsl])
@pytest.mark.parametrize("backend", [ApiBackend.solr, ApiBackend.stac])
def test_ipsl_hits_exist(
    ctx: Context, index: str, query: Query, backend: ApiBackend
):
    query.backend = backend
    hits = ctx.hits(query, file=False, index_node=index)
    assert hits[0] > 0


@parametrized_index
@pytest.mark.parametrize("query", [empty, cmip6_ipsl])
@pytest.mark.parametrize("backend", [ApiBackend.solr, ApiBackend.stac])
def test_more_files_than_datasets(
    ctx: Context,
    index: str,
    query: Query,
    backend: ApiBackend,
):
    query.backend = backend
    assert sum(ctx.hits(query, file=False)) <= sum(ctx.hits(query, file=True))


@parametrized_index
@pytest.mark.slow
@pytest.mark.parametrize("query", [cmip6_ipsl])
@pytest.mark.parametrize("backend", [ApiBackend.solr, ApiBackend.stac])
def test_hints(ctx: Context, index: str, query: Query, backend: ApiBackend):
    query.backend = backend
    facets = ["institution_id", "variable_id"]
    hints = ctx.hints(query, file=False, facets=facets)[0]
    assert list(hints["institution_id"]) == query.selection["institution_id"]
    assert len(hints["variable_id"]) > 1


@parametrized_index
@pytest.mark.parametrize(
    "query_all",
    [
        Query() << base_project,
        Query(selection={"variable_id": "tas"}) << base_project,
        Query(selection={"experiment_id": "ssp*", "variable_id": "tas"})
        << base_project,
    ],
)
@pytest.mark.parametrize("backend", [ApiBackend.solr, ApiBackend.stac])
@pytest.mark.parametrize("institution_id", ["IPSL", "IP*L"])
def test_ignore_facet_hits(
    ctx: Context,
    index: str,
    query_all: Query,
    backend: ApiBackend,
    institution_id: str,
):
    query_all.backend = backend
    query_with = (
        Query(backend=backend, selection={"institution_id": institution_id})
        << query_all
    )
    query_without = (
        Query(backend=backend, selection={"!institution_id": institution_id})
        << query_all
    )
    hits_all = ctx.hits(query_all, file=False)[0]
    hits_with = ctx.hits(query_with, file=False, index_node=index)[0]
    hits_without = ctx.hits(query_without, file=False, index_node=index)[0]
    assert all(hits > 0 for hits in [hits_all, hits_with, hits_without])
    assert hits_all == hits_with + hits_without
