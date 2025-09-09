from time import perf_counter

import pytest

from esgpull.context.stac import StacContext
from esgpull.models import Query


@pytest.fixture
def ctx(config):
    return StacContext(config=config)


@pytest.fixture
def empty():
    return Query()


@pytest.fixture
def cmip6_ipsl():
    query = Query()
    query.options.distrib = False
    query.selection.mip_era = "CMIP6"
    query.selection.institution_id = "IPSL"
    return query


@pytest.fixture(params=["empty", "cmip6_ipsl"])
def query(request):
    return request.getfixturevalue(request.param)


class Timer:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, *excs):
        self.duration = perf_counter() - self.start


def test_ipsl_hits_exist(ctx, cmip6_ipsl):
    hits = ctx.hits(
        cmip6_ipsl,
        file=False,
    )
    assert hits[0] > 1


def test_more_files_than_datasets(ctx, query):
    assert sum(ctx.hits(query, file=False)) <= sum(ctx.hits(query, file=True))


@pytest.mark.slow
def test_hints(ctx, cmip6_ipsl):
    facets = ["institution_id", "variable_id"]
    hints = ctx.hints(cmip6_ipsl, file=False, facets=facets)[0]
    assert list(hints["institution_id"]) == cmip6_ipsl.selection.institution_id
    assert len(hints["variable_id"]) > 1


def test_hits_from_hints(ctx):
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
def test_ignore_facet_hits(ctx, query_all: Query):
    query_ipsl = Query(selection={"institution_id": "IPSL"}) << query_all
    query_not_ipsl = Query(selection={"!institution_id": "IPSL"}) << query_all
    hits_all = ctx.hits(query_all, file=False)[0]
    hits_ipsl = ctx.hits(query_ipsl, file=False)[0]
    hits_not_ipsl = ctx.hits(query_not_ipsl, file=False)[0]
    assert all(hits > 0 for hits in [hits_all, hits_ipsl, hits_not_ipsl])
    assert hits_all == hits_ipsl + hits_not_ipsl
