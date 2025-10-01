from time import perf_counter

import pytest

from esgpull.config import Config
from esgpull.context import Context
from esgpull.models import ApiBackend, Query
from tests.utils import CEDA_NODE

empty = Query()
cmip6_ipsl = Query(
    options={"distrib": False},
    selection={"mip_era": "CMIP6", "institution_id": "IPSL"},
)


@pytest.fixture
def ctx(config: Config):
    return Context(config=config)


class Timer:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, *excs):
        self.duration = perf_counter() - self.start


@pytest.mark.parametrize("query", [cmip6_ipsl])
@pytest.mark.parametrize("backend", [ApiBackend.solr, ApiBackend.stac])
def test_ipsl_hits_exist(ctx: Context, query: Query, backend: ApiBackend):
    query.backend = backend
    hits = ctx.hits(query, file=False, index_node=CEDA_NODE)
    assert hits[0] > 0


@pytest.mark.parametrize("query", [empty, cmip6_ipsl])
@pytest.mark.parametrize("backend", [ApiBackend.solr, ApiBackend.stac])
def test_more_files_than_datasets(
    ctx: Context,
    query: Query,
    backend: ApiBackend,
):
    query.backend = backend
    assert sum(ctx.hits(query, file=False)) <= sum(ctx.hits(query, file=True))


@pytest.mark.slow
@pytest.mark.parametrize("query", [cmip6_ipsl])
@pytest.mark.parametrize("backend", [ApiBackend.solr, ApiBackend.stac])
def test_hints(ctx: Context, query: Query, backend: ApiBackend):
    query.backend = backend
    facets = ["institution_id", "variable_id"]
    hints = ctx.hints(query, file=False, facets=facets)[0]
    assert list(hints["institution_id"]) == query.selection.institution_id
    assert len(hints["variable_id"]) > 1


@pytest.mark.parametrize(
    "query_all",
    [
        Query(),
        Query(selection={"variable_id": "tas"}),
        Query(selection={"experiment_id": "ssp*", "variable_id": "tas"}),
    ],
)
@pytest.mark.parametrize("backend", [ApiBackend.solr, ApiBackend.stac])
def test_ignore_facet_hits(
    ctx: Context,
    query_all: Query,
    backend: ApiBackend,
):
    query_all.backend = backend
    query_ipsl = (
        Query(backend=backend, selection={"institution_id": "IPSL"})
        << query_all
    )
    query_not_ipsl = (
        Query(backend=backend, selection={"!institution_id": "IPSL"})
        << query_all
    )
    hits_all = ctx.hits(query_all, file=False)[0]
    hits_ipsl = ctx.hits(query_ipsl, file=False)[0]
    hits_not_ipsl = ctx.hits(query_not_ipsl, file=False)[0]
    assert all(hits > 0 for hits in [hits_all, hits_ipsl, hits_not_ipsl])
    assert hits_all == hits_ipsl + hits_not_ipsl
