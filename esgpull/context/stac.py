from __future__ import annotations

import asyncio
from collections.abc import Coroutine, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, TypeVar

import esgvoc.api
import httpx
from esgvoc.apps import DrsGenerator
from pydantic import BaseModel, Field, PrivateAttr
from pystac_client import Client
from pystac_client.item_search import FilterLike, ItemSearch

from esgpull.config import Config
from esgpull.context.types import HintsDict, IndexNode
from esgpull.context.utils import hits_from_hints
from esgpull.models import ApiBackend, DatasetRecord, File, Query
from esgpull.tui import logger
from esgpull.utils import sync

T = TypeVar("T")


def to_property(name: str, prefix: str) -> dict[str, str]:
    return {"property": f"properties.{prefix}:{name}"}


def to_frequency(name: str, prefix: str) -> str:
    return f"{prefix}_{name}_frequency"


def from_frequency(freq: str, prefix: str) -> str:
    return freq.removeprefix(f"{prefix}_").removesuffix("_frequency")


def name_value_to_eq_or_like(
    name: str, value: str, prefix: str
) -> dict[str, Any]:
    if name.startswith("!"):
        name = name.removeprefix("!")
        negate_filter = True
    else:
        negate_filter = False
    if "*" in value:
        filt = {
            "op": "like",
            "args": [
                to_property(name, prefix),
                value.replace("*", "%"),
            ],
        }
    else:
        filt = {
            "op": "=",
            "args": [to_property(name, prefix), value],
        }
    if negate_filter:
        filt = {"op": "not", "args": [filt]}
    return filt


def get_projects(query: Query) -> list[str]:
    sel = query.selection.asdict()
    if "project" in sel:
        projects = list(query.selection.project)
    else:
        projects = []
    if len(projects) == 0:
        raise ValueError(
            f"{query}: `project` facet is required with stac backend"
        )
    return projects


def deduce_query_prefixes(query: Query) -> list[str]:
    return [p.lower() for p in get_projects(query)]


def merge_with_op(
    filters: list[dict[str, Any]], op: Literal["or", "and"]
) -> dict[str, Any]:
    match filters:
        case []:
            return {}
        case [single_filter]:
            return single_filter
        case [*multiple_filters]:
            return {"op": op, "args": multiple_filters}


def format_query_to_stac_filter(query: Query) -> FilterLike:
    prefixes = deduce_query_prefixes(query)
    filters: list[dict[str, Any]] = []
    for prefix in prefixes:
        project_filter: list[dict[str, Any]] = []
        for name, values in query.selection.items():
            if name == "project":
                continue
            values_filter = [
                name_value_to_eq_or_like(name, value, prefix)
                for value in values
            ]
            values_merged = merge_with_op(values_filter, op="or")
            if values_merged:
                project_filter.append(values_merged)
        project_merged = merge_with_op(project_filter, op="and")
        if project_merged:
            filters.append(project_merged)
    merged = merge_with_op(filters, op="or")
    return merged


class PreparedRequest(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    query: Query
    file: bool
    stac_filter: FilterLike
    item_search: ItemSearch
    limit: int
    max_items: int | None
    facets: list[str] | None


class ProcessedHits(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    query: Query
    file: bool
    data: int
    exc: BaseException | None = None

    @property
    def success(self) -> bool:
        return self.exc is None


class ProcessedHints(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    query: Query
    file: bool
    data: HintsDict
    exc: BaseException | None = None

    @property
    def success(self) -> bool:
        return self.exc is None


class ProcessedDatasets(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    query: Query
    file: bool
    data: Sequence[DatasetRecord]
    exc: BaseException | None = None

    @property
    def success(self) -> bool:
        return self.exc is None


class ProcessedFiles(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    query: Query
    file: bool
    data: Sequence[File]
    exc: BaseException | None = None

    @property
    def success(self) -> bool:
        return self.exc is None


# Helper functions for state transitions
def prepare_request(
    query: Query,
    file: bool,
    client: Client,
    offset: int = 0,
    page_limit: int = 50,
    facets_param: list[str] | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    max_items: int | None = None,
) -> PreparedRequest:
    projects = get_projects(query)
    stac_filter = format_query_to_stac_filter(query)
    logger.info(f"{projects=}")
    logger.info(f"{stac_filter=}")
    item_search = client.search(
        filter=stac_filter,
        limit=page_limit,
        max_items=max_items,
        collections=projects,
    )

    return PreparedRequest(
        query=query,
        file=file,
        stac_filter=stac_filter,
        item_search=item_search,
        limit=page_limit,
        max_items=max_items,
        facets=facets_param,
    )


def process_hits(request: PreparedRequest) -> ProcessedHits:
    try:
        count: int | None = request.item_search.matched()
        if count is None:
            try:
                page = next(request.item_search.pages_as_dicts())
                count = page.get("numMatched", 0)
            except StopIteration:
                count = 0
        if count is None:
            raise NotImplementedError("API did not give back a count")
        return ProcessedHits(
            query=request.query,
            file=request.file,
            data=count,
        )
    except Exception as exc:
        raise
        return ProcessedHits(
            query=request.query,
            file=request.file,
            data=0,
            exc=exc,
        )


def process_hints(request: PreparedRequest) -> ProcessedHints:
    try:
        if request.facets is None:
            raise ValueError(request.facets)
        if request.item_search.client is None:
            raise ValueError(request.item_search.client)
        client = request.item_search.client
        cmip6 = client.get_collection("CMIP6")
        aggregations_link = cmip6.get_links("aggregations")[0]
        aggregate_link = cmip6.get_links("aggregate")[0]

        # Get available aggregations
        cmip6_aggregations = httpx.get(aggregations_link.href)
        aggregation_names = [
            x["name"] for x in cmip6_aggregations.json()["aggregations"]
        ]

        # Map facets to aggregation field names
        facets_to_aggregate: list[str] = []
        for facet in request.facets:
            for prefix in deduce_query_prefixes(request.query):
                frequency_field = to_frequency(facet, prefix)
                if frequency_field not in aggregation_names:
                    raise ValueError(
                        f"Missing aggregation field: {frequency_field}"
                    )
                facets_to_aggregate.append(frequency_field)

        # Make aggregation request
        payload = {
            "filter-lang": "cql2-json",
            "filter": request.stac_filter,
            "aggregations": facets_to_aggregate,
        }
        resp = httpx.post(aggregate_link.href, json=payload).raise_for_status()

        # Process the aggregation response
        data = {
            from_frequency(x["name"], prefix): {
                y["key"]: y["frequency"] for y in x["buckets"]
            }
            for x in resp.json()["aggregations"]
            for prefix in deduce_query_prefixes(request.query)
        }

        return ProcessedHints(
            query=request.query,
            file=request.file,
            data=data,
        )
    except Exception as exc:
        raise
        logger.exception(f"Failed to process STAC aggregations: {exc}")
        return ProcessedHints(
            query=request.query,
            file=request.file,
            data={},
            exc=exc,
        )


def process_datasets(request: PreparedRequest) -> ProcessedDatasets:
    try:
        datasets = []
        ## need `items_as_dicts` because some may use `alternate` assets
        for item in request.item_search.items_as_dicts():
            if (
                request.max_items is not None
                and len(datasets) >= request.max_items
            ):
                break
            dataset = stac_item_to_dataset_record(item)
            if dataset:
                datasets.append(dataset)

        return ProcessedDatasets(
            query=request.query,
            file=request.file,
            data=datasets,
        )
    except Exception as exc:
        raise
        return ProcessedDatasets(
            query=request.query,
            file=request.file,
            data=[],
            exc=exc,
        )


def process_files(request: PreparedRequest) -> ProcessedFiles:
    try:
        files: list[File] = []
        for item in request.item_search.items_as_dicts():
            if (
                request.max_items is not None
                and len(files) >= request.max_items
            ):
                break
            properties = item["properties"]
            dataset_id = properties.get("cmip6:dataset_id", item["id"])
            dataset_master_id, version = (
                dataset_id.rsplit(".", 1)
                if "." in dataset_id
                else (dataset_id, "1")
            )
            stac_collection = item["collection"]
            projects = esgvoc.api.get_all_projects()
            projects_lower = [p.lower() for p in projects]
            esgvoc_collection = projects[
                projects_lower.index(stac_collection.lower())
            ]
            drs_gen = DrsGenerator(esgvoc_collection)
            collection_properties = {
                name.split(":")[-1]: value
                for name, value in properties.items()
            } | {"version": version}
            collection_properties = fix_collection_name_properties(
                collection_properties
            )

            for name, asset in item["assets"].items():
                file: File | None = None
                asset = stac_asset_or_any_alternate(asset)
                if asset["type"] != "application/netcdf":
                    continue
                if not (
                    asset["href"].startswith("http")
                    or asset["href"].startswith("https")
                ):
                    continue

                drs = drs_gen.generate_directory_from_mapping(
                    collection_properties
                )
                if drs.errors:
                    raise ValueError(drs.errors)

                local_path = drs.generated_drs_expression
                data_node: str = asset["alternate:name"]
                size: int = asset["file:size"]
                checksum: str = asset["file:checksum"]
                url: str = asset["href"]
                filename = Path(url).name
                file_id = f"{dataset_id}.{filename}"
                master_id = f"{dataset_master_id}.{filename}"

                file = File.fromdict(
                    {
                        "file_id": file_id,
                        "dataset_id": dataset_id,
                        "master_id": master_id,
                        "url": url,
                        "version": version,
                        "filename": filename,
                        "local_path": local_path,
                        "data_node": data_node,
                        "checksum": checksum,
                        "checksum_type": "MULTIHASH",
                        "size": size,
                    }
                )
                file.compute_sha()
                files.append(file)

        return ProcessedFiles(
            query=request.query,
            file=request.file,
            data=files,
        )
    except Exception as exc:
        logger.exception(f"Failed to process STAC files: {exc}")
        return ProcessedFiles(
            query=request.query,
            file=request.file,
            data=[],
            exc=exc,
        )


def stac_asset_or_any_alternate(asset: dict[str, Any]) -> dict[str, Any]:
    # TODO: actually handle alternates. it needs db tables to store it.
    if "alternate" in asset:
        return list(asset["alternate"].values())[0]
    else:
        return asset


def fix_collection_name_properties(props: dict[str, Any]) -> dict[str, Any]:
    fixers = [
        lambda name: name if name != "variable" else "variable_id",
    ]

    def run_fixers(x: str) -> str:
        for fixer in fixers:
            x = fixer(x)
        return x

    return {run_fixers(name): value for name, value in props.items()}


def stac_item_to_dataset_record(
    stac_item: dict[str, Any],
) -> DatasetRecord | None:
    """Convert a STAC item to a DatasetRecord."""
    try:
        properties = stac_item["properties"]
        dataset_id = properties.get("cmip6:dataset_id", stac_item["id"])
        if not dataset_id:
            return None

        master_id, version = (
            dataset_id.rsplit(".", 1)
            if "." in dataset_id
            else (dataset_id, "1")
        )
        ## TODO: useless ? since it's bound to assets (`alternate:name`)
        data_node = properties.get("cmip6:data_node", "unknown")
        size = 0
        number_of_files = 0
        for i, asset in enumerate(stac_item["assets"].values()):
            asset = stac_asset_or_any_alternate(asset)
            if asset["type"] != "application/netcdf":
                continue
            asset_size = asset.get("file:size")
            if asset_size is None:
                logger.warning(
                    f"`file:size` missing from asset #{i} of {dataset_id}"
                )
            else:
                size += asset_size
            number_of_files += 1

        return DatasetRecord(
            dataset_id=dataset_id,
            master_id=master_id,
            version=version,
            data_node=data_node,
            size=size,
            number_of_files=number_of_files,
        )
    except Exception as exc:
        raise
        logger.exception(
            f"Failed to convert STAC item to DatasetRecord: {exc}"
        )
        return None


class StacContext(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    config: Config = Field(default_factory=Config.default)
    noraise: bool = False
    _client: Client = PrivateAttr()
    _semaphores: dict[str, asyncio.Semaphore] = PrivateAttr(
        default_factory=dict,
    )

    def model_post_init(self, context: Any):
        index = IndexNode(
            backend=ApiBackend.stac,
            value=self.config.api.stac_url,
        )
        self._client = Client.open(
            url=index.url,
            timeout=self.config.api.http_timeout,
        )

    def prepare_hits(
        self,
        *queries: Query,
        file: bool,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[PreparedRequest]:
        results = []
        for query in queries:
            prepared = prepare_request(
                query=query,
                file=file,
                client=self._client,
                page_limit=1,  # For count queries, we only need the count metadata
                date_from=date_from,
                date_to=date_to,
            )
            results.append(prepared)
        return results

    def prepare_hints(
        self,
        *queries: Query,
        file: bool,
        facets: list[str],
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[PreparedRequest]:
        results = []
        for query in queries:
            prepared = prepare_request(
                query=query,
                file=file,
                client=self._client,
                page_limit=1,
                facets_param=facets,
                date_from=date_from,
                date_to=date_to,
            )
            results.append(prepared)
        return results

    def prepare_search(
        self,
        *queries: Query,
        file: bool,
        hits: list[int],
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        # fields_param: list[str] | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[PreparedRequest]:
        if page_limit is None:
            page_limit = self.config.api.page_limit

        results = []
        for query in queries:
            prepared = prepare_request(
                query=query,
                file=file,
                client=self._client,
                offset=offset,
                page_limit=page_limit,
                max_items=max_hits,
                date_from=date_from,
                date_to=date_to,
            )
            results.append(prepared)
        return results

    def prepare_search_distributed(
        self,
        *queries: Query,
        file: bool,
        hints: list[HintsDict],
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        # fields_param: list[str] | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[PreparedRequest]:
        # For STAC, distributed search is simplified since we're dealing with a single endpoint
        hits = hits_from_hints(*hints)
        return self.prepare_search(
            *queries,
            file=file,
            hits=hits,
            offset=offset,
            max_hits=max_hits,
            page_limit=page_limit,
            # fields_param=fields_param,
            date_from=date_from,
            date_to=date_to,
        )

    async def _hits(self, *prepared_requests: PreparedRequest) -> list[int]:
        hits = []
        for request in prepared_requests:
            processed = process_hits(request)
            hits.append(processed.data)
        return hits

    async def _hints(
        self, *prepared_requests: PreparedRequest
    ) -> list[HintsDict]:
        hints: list[HintsDict] = []
        for request in prepared_requests:
            processed = process_hints(request)
            hints.append(processed.data)
        return hints

    async def _datasets(
        self,
        *prepared_requests: PreparedRequest,
        keep_duplicates: bool,
    ) -> list[DatasetRecord]:
        datasets: list[DatasetRecord] = []
        ids: set[str] = set()
        for request in prepared_requests:
            processed = process_datasets(request)
            for d in processed.data:
                if not keep_duplicates and d.dataset_id in ids:
                    logger.debug(f"Duplicate dataset {d.dataset_id}")
                else:
                    datasets.append(d)
                    ids.add(d.dataset_id)
        return datasets

    async def _files(
        self,
        *prepared_requests: PreparedRequest,
        keep_duplicates: bool,
    ) -> list[File]:
        files: list[File] = []
        shas: set[str] = set()
        for request in prepared_requests:
            processed = process_files(request)
            for f in processed.data:
                if not keep_duplicates and f.sha in shas:
                    logger.debug(f"Duplicate file {f.file_id}")
                else:
                    files.append(f)
                    shas.add(f.sha)
        return files

    async def _search_as_queries(
        self,
        *prepared_requests: PreparedRequest,
        keep_duplicates: bool,
    ) -> list[Query]:
        # For now, return empty list since this is not the priority
        return []

    def free_semaphores(self) -> None:
        self._semaphores = {}

    def _sync(self, coro: Coroutine[None, None, T]) -> T:
        """
        Reset semaphore to ensure none is bound to an expired event loop.
        """
        self.free_semaphores()
        return sync(coro)

    def hits(
        self,
        *queries: Query,
        file: bool,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[int]:
        ## TODO: implement actual file counting, either:
        ## * iterate pages (slow with large queries)
        ## * ask server team to add asset count aggregation
        ## * estimate based on 1st page number of assets
        results = self.prepare_hits(
            *queries,
            file=file,
            date_from=date_from,
            date_to=date_to,
        )
        return self._sync(self._hits(*results))

    def hints(
        self,
        *queries: Query,
        file: bool,
        facets: list[str],
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[HintsDict]:
        results = self.prepare_hints(
            *queries,
            file=file,
            facets=facets,
            date_from=date_from,
            date_to=date_to,
        )
        return self._sync(self._hints(*results))

    def datasets(
        self,
        *queries: Query,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        keep_duplicates: bool = True,
    ) -> list[DatasetRecord]:
        if hits is None:
            hits = self.hits(*queries, file=False)
        results = self.prepare_search(
            *queries,
            file=False,
            hits=hits,
            offset=offset,
            page_limit=page_limit,
            max_hits=max_hits,
            date_from=date_from,
            date_to=date_to,
        )
        coro = self._datasets(*results, keep_duplicates=keep_duplicates)
        return self._sync(coro)

    def files(
        self,
        *queries: Query,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        keep_duplicates: bool = True,
    ) -> list[File]:
        if hits is None:
            hits = self.hits(*queries, file=True)
        results = self.prepare_search(
            *queries,
            file=True,
            hits=hits,
            offset=offset,
            page_limit=page_limit,
            max_hits=max_hits,
            date_from=date_from,
            date_to=date_to,
        )
        coro = self._files(*results, keep_duplicates=keep_duplicates)
        return self._sync(coro)

    def search_as_queries(
        self,
        *queries: Query,
        file: bool,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 1,
        page_limit: int | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        keep_duplicates: bool = True,
    ) -> Sequence[Query]:
        # For now, return empty list since this is not the priority
        raise NotImplementedError()
        return []

    def search(
        self,
        *queries: Query,
        file: bool,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        keep_duplicates: bool = True,
    ) -> Sequence[File | DatasetRecord]:
        if file:
            return self.files(
                *queries,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
                page_limit=page_limit,
                date_from=date_from,
                date_to=date_to,
                keep_duplicates=keep_duplicates,
            )
        else:
            return self.datasets(
                *queries,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
                page_limit=page_limit,
                date_from=date_from,
                date_to=date_to,
                keep_duplicates=keep_duplicates,
            )
