from time import perf_counter

import pytest

from esgpull.config import Config
from esgpull.context.solr import SolrContext, _distribute_hits_impl
from esgpull.models import Query
from tests.utils import CEDA_NODE, DRKZ_NODE

empty = Query()
cmip6_ipsl = Query(
    options={"distrib": False},
    selection={"mip_era": "CMIP6", "institution_id": "IPSL"},
)


@pytest.fixture
def ctx(config: Config):
    return SolrContext(config=config)


@pytest.mark.parametrize("query", [empty])
def test_multi_index(ctx: SolrContext, query: Query):
    index_nodes = [CEDA_NODE, DRKZ_NODE]
    results = []
    for index_node in index_nodes:
        query_results = ctx.prepare_hits(
            query,
            file=False,
            index_node=index_node,
        )
        results.extend(query_results)
    assert len(results) == 2
    for result, index_node in zip(results, index_nodes, strict=False):
        assert index_node in str(result.request.url)
        assert index_node == result.request.headers["host"]


def test_adjust_hits(ctx: SolrContext):
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


# @pytest.mark.slow
# @pytest.mark.xfail(
#     raises=ValueError,
#     reason="ESGF bridge API gives index_node values that are not valid URLs",
# )
# def test_search_distributed(ctx: SolrContext):
#     query = Query()
#     # ctx.config.api.http_timeout = 60
#     query.options.distrib = True
#     # configure terms to target ~1000 files across different index nodes
#     query.selection.mip_era = "CMIP6"
#     query.selection.table_id = "Amon"
#     query.selection.variable_id = "tas"
#     query.selection.member_id = "r20i1p1f1"
#     with Timer() as t_regular:
#         datasets_regular = ctx.datasets(
#             query,
#             max_hits=None,
#             keep_duplicates=True,
#         )
#     with Timer() as t_distributed:
#         hints = ctx.hints(query, file=False, facets=["index_node"])
#         results = ctx.prepare_search_distributed(
#             query,
#             file=False,
#             hints=hints,
#             max_hits=None,
#         )
#         coro = ctx._datasets(*results, keep_duplicates=True)
#         datasets_distributed = ctx._sync(coro)
#     dataset_ids_regular = {d.dataset_id for d in datasets_regular}
#     dataset_ids_distributed = {d.dataset_id for d in datasets_distributed}
#     assert dataset_ids_regular == dataset_ids_distributed
#     # assert t_regular.duration >= t_distributed.duration
#     logging.info(f"{t_regular.duration}")
#     logging.info(f"{t_distributed.duration}")


@pytest.mark.parametrize("query", [cmip6_ipsl])
def test_ipsl_hits_exist(ctx: SolrContext, query: Query):
    hits = ctx.hits(query, file=False, index_node=CEDA_NODE)
    assert hits[0] > 1_000


@pytest.mark.parametrize("query", [empty, cmip6_ipsl])
def test_more_files_than_datasets(ctx: SolrContext, query: Query):
    assert sum(ctx.hits(query, file=False)) <= sum(ctx.hits(query, file=True))


@pytest.mark.slow
@pytest.mark.parametrize("query", [cmip6_ipsl])
def test_hints(ctx: SolrContext, query: Query):
    facets = ["institution_id", "variable_id"]
    hints = ctx.hints(query, file=False, facets=facets)[0]
    assert list(hints["institution_id"]) == query.selection.institution_id
    assert len(hints["variable_id"]) > 1


def test_hits_from_hints(ctx: SolrContext):
    hints = {"facet_name": {"value_a": 1, "value_b": 2, "value_c": 3}}
    hits = ctx.hits_from_hints(hints)
    assert hits == [6]


@pytest.mark.parametrize(
    "query_all",
    [
        Query(),
        Query(selection={"variable_id": "tas"}),
        Query(selection={"experiment_id": "ssp*", "variable_id": "tas"}),
    ],
)
def test_ignore_facet_hits(ctx: SolrContext, query_all: Query):
    query_ipsl = Query(selection={"institution_id": "IPSL"}) << query_all
    query_not_ipsl = Query(selection={"!institution_id": "IPSL"}) << query_all
    hits_all = ctx.hits(query_all, file=False)[0]
    hits_ipsl = ctx.hits(query_ipsl, file=False)[0]
    hits_not_ipsl = ctx.hits(query_not_ipsl, file=False)[0]
    assert all(hits > 0 for hits in [hits_all, hits_ipsl, hits_not_ipsl])
    assert hits_all == hits_ipsl + hits_not_ipsl
