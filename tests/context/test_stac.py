from contextlib import nullcontext as does_not_raise
from time import perf_counter

import pytest

from esgpull.config import Config
from esgpull.context.stac import (
    StacContext,
    format_query_to_stac_filter,
    get_projects,
)
from esgpull.models import Query

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
    return StacContext(config=config)


class Timer:
    def __enter__(self):
        self.start = perf_counter()
        return self

    def __exit__(self, *excs):
        self.duration = perf_counter() - self.start


@pytest.mark.parametrize("query", [cmip6_ipsl])
def test_ipsl_hits_exist(ctx, query: Query):
    hits = ctx.hits(query, file=False)
    assert hits[0] > 1


@pytest.mark.parametrize("query", [empty, cmip6_ipsl])
def test_more_files_than_datasets(ctx: StacContext, query: Query):
    assert sum(ctx.hits(query, file=False)) <= sum(ctx.hits(query, file=True))


@pytest.mark.slow
@pytest.mark.parametrize("query", [cmip6_ipsl])
def test_hints(ctx: StacContext, query: Query):
    facets = ["institution_id", "variable_id"]
    hints = ctx.hints(query, file=False, facets=facets)[0]
    assert list(hints["institution_id"]) == query.selection.institution_id
    assert len(hints["variable_id"]) > 1


@pytest.mark.parametrize(
    "query_all",
    [
        Query() << base_project,
        Query(selection={"variable_id": "tas"}) << base_project,
        Query(selection={"experiment_id": "ssp*", "variable_id": "tas"})
        << base_project,
    ],
)
def test_ignore_facet_hits(ctx: StacContext, query_all: Query):
    query_ipsl = Query(selection={"institution_id": "IPSL"}) << query_all
    query_not_ipsl = Query(selection={"!institution_id": "IPSL"}) << query_all
    hits_all = ctx.hits(query_all, file=False)[0]
    hits_ipsl = ctx.hits(query_ipsl, file=False)[0]
    hits_not_ipsl = ctx.hits(query_not_ipsl, file=False)[0]
    assert all(hits > 0 for hits in [hits_all, hits_ipsl, hits_not_ipsl])
    assert hits_all == hits_ipsl + hits_not_ipsl


@pytest.mark.parametrize(
    ("selection", "expected", "exc"),
    [
        ({}, [], pytest.raises(ValueError)),
        ({"variable_id": "tas"}, [], pytest.raises(ValueError)),
        ({"project": "CMIP6"}, ["CMIP6"], does_not_raise()),
        (
            {"project": "CMIP6", "variable_id": "tas"},
            ["CMIP6"],
            does_not_raise(),
        ),
        (
            {"project": ["CMIP6", "CMIP7"]},
            ["CMIP6", "CMIP7"],
            does_not_raise(),
        ),
        (
            {"project": ["CMIP6", "CMIP7"], "variable_id": "tas"},
            ["CMIP6", "CMIP7"],
            does_not_raise(),
        ),
    ],
)
def test_get_projects(selection: dict, expected: list[str], exc):
    query = Query(selection=selection)
    with exc:
        projects = get_projects(query)
        assert projects == expected


@pytest.mark.parametrize(
    ("selection", "expected", "exc"),
    [
        ({}, {}, pytest.raises(ValueError)),
        ({"variable_id": "tas"}, {}, pytest.raises(ValueError)),
        ({"project": "CMIP6"}, {}, does_not_raise()),
        (
            {"project": "CMIP6", "variable_id": "tas"},
            {
                "args": [
                    {
                        "property": "properties.cmip6:variable_id",
                    },
                    "tas",
                ],
                "op": "=",
            },
            does_not_raise(),
        ),
        (
            {"project": ["CMIP6", "CMIP7"]},
            {},
            does_not_raise(),
        ),
        (
            {"project": ["CMIP6", "CMIP7"], "variable_id": "tas"},
            {
                "args": [
                    {
                        "args": [
                            {
                                "property": "properties.cmip6:variable_id",
                            },
                            "tas",
                        ],
                        "op": "=",
                    },
                    {
                        "args": [
                            {
                                "property": "properties.cmip7:variable_id",
                            },
                            "tas",
                        ],
                        "op": "=",
                    },
                ],
                "op": "or",
            },
            does_not_raise(),
        ),
    ],
)
def test_format_query_to_stac_filter(selection: dict, expected: dict, exc):
    query = Query(selection=selection)
    with exc:
        stac_filter = format_query_to_stac_filter(query)
        assert stac_filter == expected
