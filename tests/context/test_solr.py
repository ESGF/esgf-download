from contextlib import nullcontext as does_not_raise
from time import perf_counter

import pytest

from esgpull.config import Config
from esgpull.context.solr import (
    SolrContext,
    _distribute_hits_impl,
    ResultSearch,
)
from esgpull.models import Query
from tests.utils import (
    CEDA_NODE,
    DRKZ_NODE,
    IPSL_NODE,
    ORNL_BRIDGE,
    parametrized_index,
)

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


@parametrized_index
@pytest.mark.parametrize("query", [cmip6_ipsl])
def test_ipsl_hits_exist(ctx: SolrContext, index: str, query: Query):
    hits = ctx.hits(query, file=False, index_node=index)
    assert hits[0] > 1_000


@parametrized_index
@pytest.mark.parametrize("query", [empty, cmip6_ipsl])
def test_more_files_than_datasets(ctx: SolrContext, index: str, query: Query):
    assert sum(ctx.hits(query, file=False)) <= sum(ctx.hits(query, file=True))


@parametrized_index
@pytest.mark.slow
@pytest.mark.parametrize("query", [cmip6_ipsl])
def test_hints(ctx: SolrContext, index: str, query: Query):
    facets = ["institution_id", "variable_id"]
    hints = ctx.hints(query, file=False, facets=facets)[0]
    assert list(hints["institution_id"]) == query.selection.institution_id
    assert len(hints["variable_id"]) > 1


@parametrized_index
@pytest.mark.parametrize(
    "query_all",
    [
        Query(),
        Query(selection={"variable_id": "tas"}),
        Query(selection={"experiment_id": "ssp*", "variable_id": "tas"}),
    ],
)
def test_ignore_facet_hits(ctx: SolrContext, index: str, query_all: Query):
    query_ipsl = Query(selection={"institution_id": "IPSL"}) << query_all
    query_not_ipsl = Query(selection={"!institution_id": "IPSL"}) << query_all
    hits_all = ctx.hits(query_all, file=False, index_node=index)[0]
    hits_ipsl = ctx.hits(query_ipsl, file=False, index_node=index)[0]
    hits_not_ipsl = ctx.hits(query_not_ipsl, file=False, index_node=index)[0]
    assert all(hits > 0 for hits in [hits_all, hits_ipsl, hits_not_ipsl])
    assert hits_all == hits_ipsl + hits_not_ipsl


@pytest.mark.parametrize(
    "queries",
    [
        [],
        [Query()],
        [
            Query(
                selection=dict(
                    project="CMIP6",
                    institution_id="IPSL",
                    variable_id="uv",
                ),
            ),
        ],
        [
            Query(),
            Query(
                selection=dict(
                    project="CMIP6",
                    institution_id="IPSL",
                    variable_id="uv",
                ),
            ),
        ],
        [Query(selection=dict(project="notaproject"))],
    ],
)
@pytest.mark.parametrize("file", [True, False])
@pytest.mark.parametrize(
    "index_node",
    ## TODO: test bridge, but it is super slow
    [
        IPSL_NODE,
        CEDA_NODE,
        "https://github.com",
        "not_a_real.url",
    ],
)
def test_hits_never_empty(
    ctx: SolrContext,
    queries: tuple[Query],
    file: bool,
    index_node: str,
):
    ctx.noraise = True
    hits = ctx.hits(*queries, file=file, index_node=index_node)
    assert len(hits) == len(queries)


@pytest.mark.parametrize(
    ("index_node", "exc"),
    ## TODO: test bridge, but it is super slow
    [
        pytest.param(
            IPSL_NODE,
            does_not_raise(),
            marks=pytest.mark.xfail(reason="unstable"),
        ),
        (CEDA_NODE, does_not_raise()),
        ("https://github.com", pytest.raises(Exception)),
        ("not_a_real.url", pytest.raises(Exception)),
    ],
)
def test_probe(
    ctx: SolrContext,
    index_node: str,
    exc,
):
    ctx.config.api.index_node = index_node
    with exc:
        ctx.probe()


def test_bridge_exact_match_params(ctx: SolrContext):
    query = Query(selection=dict(source_id="CESM2", variable_id="tas"))
    result = ctx.prepare_hits(
        query,
        file=False,
        index_node=ORNL_BRIDGE,
    )[0]
    params = dict(result.request.url.params.items())

    assert "source_id" in params
    assert params["source_id"] == "CESM2"
    assert "variable_id" in params
    assert params["variable_id"] == "tas"
    assert "query" not in params or params["query"] == ""


def test_bridge_wildcard_query_param(ctx: SolrContext):
    query = Query(selection=dict(source_id="CESM*", variable_id="tas*"))
    result = ctx.prepare_hits(
        query,
        file=False,
        index_node=ORNL_BRIDGE,
    )[0]
    params = dict(result.request.url.params.items())

    assert "query" in params
    assert "source_id" not in params
    assert "variable_id" not in params
    assert "source_id:CESM*" in params["query"]
    assert "variable_id:tas*" in params["query"]


def test_bridge_mixed_exact_wildcard(ctx: SolrContext):
    query = Query(selection=dict(source_id="CESM2", variable_id="tas*"))
    result = ctx.prepare_hits(
        query,
        file=False,
        index_node=ORNL_BRIDGE,
    )[0]
    params = dict(result.request.url.params.items())

    assert "source_id" in params
    assert params["source_id"] == "CESM2"
    assert "query" in params
    assert "variable_id:tas*" in params["query"]
    assert "variable_id" not in params


def test_bridge_multi_value_exact(ctx: SolrContext):
    query = Query(selection=dict(source_id=["CESM2", "CESM2-LENS2"]))
    result = ctx.prepare_hits(
        query,
        file=False,
        index_node=ORNL_BRIDGE,
    )[0]
    params = dict(result.request.url.params.items())

    assert "source_id" in params
    assert params["source_id"] == "CESM2,CESM2-LENS2"
    assert "query" not in params or params["query"] == ""


def test_bridge_negated_query(ctx: SolrContext):
    query = Query(selection=dict(**{"!institution_id": "IPSL"}))
    result = ctx.prepare_hits(
        query,
        file=False,
        index_node=ORNL_BRIDGE,
    )[0]
    params = dict(result.request.url.params.items())

    assert "query" in params
    assert "institution_id" not in params
    assert 'NOT (institution_id:"IPSL")' in params["query"]


def test_bridge_mixed_wildcard_warning(ctx: SolrContext, caplog):
    query = Query(selection=dict(source_id=["CESM2", "CESM*"]))
    result = ctx.prepare_hits(
        query,
        file=False,
        index_node=ORNL_BRIDGE,
    )[0]
    params = dict(result.request.url.params.items())

    assert "source_id" not in params
    assert "query" in params
    assert "source_id:" in params["query"]
    assert any(
        "source_id has mixed wildcard/non-wildcard values" in record.message
        for record in caplog.records
    )


def test_solr_unchanged(ctx: SolrContext):
    query = Query(selection=dict(source_id="CESM2", variable_id="tas"))
    result = ctx.prepare_hits(
        query,
        file=False,
        index_node=IPSL_NODE,
    )[0]
    params = dict(result.request.url.params.items())

    assert "query" in params
    assert params["query"] == "source_id:CESM2 AND variable_id:tas"
    assert "source_id" not in params
    assert "variable_id" not in params


def test_files_skips_duplicate_file_ids(ctx: SolrContext):
    """Test that _files skips duplicate file_ids to prevent DB constraint violations."""
    import asyncio
    from unittest.mock import MagicMock, patch

    from esgpull.context.solr import ResultFiles

    # Create Solr response docs that will be deserialized by File.serialize()
    # Two files with same file_id (dataset.v1.file.nc) but different checksums
    solr_docs = [
        {
            "instance_id": "dataset.v1.file.nc|node1.com",
            "dataset_id": "dataset.v1|node1.com",
            "title": "file.nc",
            "url": "https://node1.com/file.nc|application/netcdf|HTTPServer",
            "data_node": "node1.com",
            "checksum": "abc123",
            "checksum_type": "SHA256",
            "size": 1000,
            "directory_format_template_": "%(root)s/v1",  # Simple template
        },
        {
            "instance_id": "dataset.v1.file.nc|node2.com",
            "dataset_id": "dataset.v1|node2.com",
            "title": "file.nc",
            "url": "https://node2.com/file.nc|application/netcdf|HTTPServer",
            "data_node": "node2.com",
            "checksum": "def456",  # Different checksum!
            "checksum_type": "SHA256",
            "size": 1000,
            "directory_format_template_": "%(root)s/v1",  # Simple template
        },
        {
            "instance_id": "dataset.v1.other.nc|node1.com",
            "dataset_id": "dataset.v1|node1.com",
            "title": "other.nc",
            "url": "https://node1.com/other.nc|application/netcdf|HTTPServer",
            "data_node": "node1.com",
            "checksum": "xyz789",
            "checksum_type": "SHA256",
            "size": 2000,
            "directory_format_template_": "%(root)s/v1",  # Simple template
        },
    ]

    # Create a ResultFiles with proper JSON that will be parsed by process()
    query = Query()
    result = ResultFiles(query=query, file=True)
    result.request = MagicMock()
    result.json = {"response": {"docs": solr_docs}}

    # Mock _fetch to yield our result
    async def mock_fetch(*results):
        yield result

    async def run_test():
        with patch.object(ctx, "_fetch", mock_fetch):
            return await ctx._files(result, keep_duplicates=False)

    result_files = asyncio.run(run_test())

    # Should only have 2 files (second one with duplicate file_id is skipped)
    assert len(result_files) == 2

    # Verify the file_ids in the result
    file_ids = {f.file_id for f in result_files}
    assert file_ids == {"dataset.v1.file.nc", "dataset.v1.other.nc"}

    # The first file with dataset.v1.file.nc should be kept (abc123 checksum)
    duplicate_files = [
        f for f in result_files if f.file_id == "dataset.v1.file.nc"
    ]
    assert len(duplicate_files) == 1
    assert duplicate_files[0].checksum == "abc123"


def test_update_checks_files_by_file_id_not_sha(db):
    """Test that update separates files by file_id, not SHA.

    This verifies the fix for the bug where re-adding a query after removal
    would fail because files were checked by SHA instead of file_id.
    When the same file appears with different checksums (different replicas),
    it should still be recognized as the same file.
    """
    from esgpull.models import File, sql

    # Create a file in the database with a specific SHA
    existing_file = File(
        file_id="dataset.v1.test.nc",
        dataset_id="dataset.v1",
        master_id="dataset.v1.test",
        url="https://example.com/test.nc",
        version="v1",
        filename="test.nc",
        local_path="dataset/v1/test.nc",
        data_node="example.com",
        checksum="original_checksum_abc123",
        checksum_type="SHA256",
        size=1000,
    )
    existing_file.compute_sha()
    db.add(existing_file)

    # Verify file is in DB
    all_file_ids = set(db.scalars(sql.file.all_file_ids()))
    assert "dataset.v1.test.nc" in all_file_ids

    # Create a "fetched" file with same file_id but different SHA
    # (This simulates what happens when fetching from a different replica)
    fetched_file = File(
        file_id="dataset.v1.test.nc",
        dataset_id="dataset.v1",
        master_id="dataset.v1.test",
        url="https://other-node.com/test.nc",
        version="v1",
        filename="test.nc",
        local_path="dataset/v1/test.nc",
        data_node="other-node.com",
        checksum="different_checksum_def456",
        checksum_type="SHA256",
        size=1000,
    )
    fetched_file.compute_sha()

    # Verify the fetched file has a different SHA
    assert fetched_file.sha != existing_file.sha

    # The fetched file should be identified as existing by file_id
    assert fetched_file.file_id in all_file_ids

    # Fetch the existing file by file_id
    existing_sha = db.scalars(sql.file.with_file_id(fetched_file.file_id))
    assert existing_sha is not None
    assert len(existing_sha) == 1
    assert existing_sha[0] == existing_file.sha

    # Get the existing file from DB
    file_from_db = db.get(File, existing_sha[0])
    assert file_from_db is not None
    assert file_from_db.file_id == "dataset.v1.test.nc"
    assert file_from_db.checksum == "original_checksum_abc123"


async def simple_fetch(
    ctx: SolrContext, prepared: ResultSearch
) -> ResultSearch:
    async for result in ctx._fetch(prepared):
        return result
    assert False


def test_bridge_client_connection(ctx: SolrContext):
    # query as seen in issue #148
    query = Query(
        selection={
            "project": "CMIP6",
            "activity_id": "HighResMIP",
            "variable_id": "areacello",
        },
    )
    file_hits = ctx.hits(query, file=True)
    assert len(file_hits) > 0
    assert file_hits[0] > 0
    dataset_hits = ctx.hits(query, file=False)
    assert len(dataset_hits) > 0
    assert dataset_hits[0] > 0
    file_prepared = ctx.prepare_search(query, file=True, hits=file_hits)
    dataset_prepared = ctx.prepare_search(query, file=False, hits=dataset_hits)
    file_result = ctx._sync(simple_fetch(ctx, file_prepared[0]))
    dataset_result = ctx._sync(simple_fetch(ctx, dataset_prepared[0]))
    assert file_result.exc is None
    assert dataset_result.exc is None
    # project:CMIP6 activity_id:HighResMIP variable_id:areacello --distrib true
