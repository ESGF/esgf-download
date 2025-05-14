from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, TypeAlias, TypeVar

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup

from httpx import AsyncClient, HTTPError, Request
from rich.pretty import pretty_repr

from esgpull.config import Config
from esgpull.exceptions import SolrUnstableQueryError
from esgpull.models import Dataset, File, Query
from esgpull.tui import logger
from esgpull.utils import format_date_iso, index2url, sync

# workaround for notebooks with running event loop
if asyncio.get_event_loop().is_running():
    import nest_asyncio

    nest_asyncio.apply()


T = TypeVar("T")
RT = TypeVar("RT", bound="Result")
HintsDict: TypeAlias = dict[str, dict[str, int]]
DangerousFacets = {
    "instance_id",
    "dataset_id",
    "master_id",
    "tracking_id",
    "url",
}


@dataclass
class Result:
    query: Query
    file: bool
    request: Request = field(init=False, repr=False)
    json: dict[str, Any] = field(init=False, repr=False)
    exc: BaseException | None = field(init=False, default=None, repr=False)
    processed: bool = field(init=False, default=False, repr=False)

    @property
    def success(self) -> bool:
        return self.exc is None

    def prepare(
        self,
        index_node: str,
        offset: int = 0,
        page_limit: int = 50,
        index_url: str | None = None,
        fields_param: list[str] | None = None,
        facets_param: list[str] | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> None:
        params: dict[str, str | int | bool] = {
            "type": "File" if self.file else "Dataset",
            "offset": offset,
            "limit": page_limit,
            "format": "application/solr+json",
            # "from": self.since,
        }
        if index_url is None:
            index_url = index2url(index_node)
        if fields_param is not None:
            params["fields"] = ",".join(fields_param)
        else:
            params["fields"] = "instance_id"
        if date_from is not None:
            params["from"] = format_date_iso(date_from)
        if date_to is not None:
            params["to"] = format_date_iso(date_to)
        if facets_param is not None:
            if len(set(facets_param) & DangerousFacets) > 0:
                raise SolrUnstableQueryError(pretty_repr(self.query))
            facets_param_str = ",".join(facets_param)
            facets_star = "*" in facets_param_str
            params["facets"] = facets_param_str
        else:
            facets_star = False
        # [?]TODO: add nominal temporal constraints `to`
        # if "start" in facets:
        #     query["start"] = format_date_iso(str(facets.pop("start")))
        # if "end" in facets:
        #     query["end"] = format_date_iso(str(facets.pop("end")))
        solr_terms: list[str] = []
        for name, values in self.query.selection.items():
            value_term = " ".join(values)
            if name == "query":  # freetext case
                solr_terms.append(value_term)
            else:
                if len(values) > 1:
                    value_term = f"({value_term})"
                solr_terms.append(f"{name}:{value_term}")
        if solr_terms:
            params["query"] = " AND ".join(solr_terms)
        for name, option in self.query.options.items(use_default=True):
            if option.is_bool():
                params[name] = option.name
        if params.get("distrib") == "true" and facets_star:
            raise SolrUnstableQueryError(pretty_repr(self.query))
        self.request = Request("GET", index_url, params=params)

    def to(self, subtype: type[RT]) -> RT:
        result: RT = subtype(self.query, self.file)
        result.request = self.request
        result.exc = self.exc
        if result.success:
            result.json = self.json
        return result


@dataclass
class ResultHits(Result):
    data: int = field(init=False, repr=False)

    def process(self) -> None:
        if self.success:
            self.data = self.json["response"]["numFound"]
            self.processed = True
        else:
            self.data = 0


@dataclass
class ResultHints(Result):
    data: HintsDict = field(init=False, repr=False)

    def process(self) -> None:
        self.data = {}
        if self.success:
            facet_fields = self.json["facet_counts"]["facet_fields"]
            for name, value_count in facet_fields.items():
                if len(value_count) == 0:
                    continue
                values: list[str] = value_count[::2]
                counts: list[int] = value_count[1::2]
                self.data[name] = dict(zip(values, counts))
            self.processed = True


@dataclass
class ResultSearch(Result):
    data: Sequence[File | Dataset] = field(init=False, repr=False)

    def process(self) -> None:
        raise NotImplementedError


@dataclass
class ResultDatasets(Result):
    data: Sequence[Dataset] = field(init=False, repr=False)

    def process(self) -> None:
        self.data = []
        if self.success:
            for doc in self.json["response"]["docs"]:
                try:
                    dataset = Dataset.serialize(doc)
                    self.data.append(dataset)
                except KeyError as exc:
                    logger.exception(exc)
            self.processed = True


@dataclass
class ResultFiles(Result):
    data: Sequence[File] = field(init=False, repr=False)

    def process(self) -> None:
        self.data = []
        if self.success:
            for doc in self.json["response"]["docs"]:
                try:
                    file = File.serialize(doc)
                    self.data.append(file)
                except KeyError as exc:
                    logger.exception(exc)
                    fid = doc["instance_id"]
                    logger.warning(f"File {fid} has invalid metadata")
                    logger.debug(pretty_repr(doc))
            self.processed = True


@dataclass
class ResultSearchAsQueries(Result):
    data: Sequence[Query] = field(init=False, repr=False)

    def process(self) -> None:
        self.data = []
        sha = "FILE" if self.file else "DATASET"
        if self.success:
            for doc in self.json["response"]["docs"]:
                query = Query._from_detailed_dict(doc)
                query.sha = f"{sha}:{query.sha}"
                self.data.append(query)
            self.processed = True


def _distribute_hits_impl(hits: list[int], max_hits: int) -> list[int]:
    i = total = 0
    N = len(hits)
    accs = [0.0 for _ in range(N)]
    result = [0 for _ in range(N)]
    steps = [h / (sum(hits) or 1) for h in hits]
    max_hits = min(max_hits, sum(hits))
    while True:
        accs[i] += steps[i]
        step = int(accs[i])
        if total + step >= max_hits:
            result[i] += max_hits - total
            break
        total += step
        accs[i] -= step
        result[i] += step
        i = (i + 1) % N
    return result


def _distribute_hits(
    hits: list[int],
    offset: int,
    max_hits: int | None,
    page_limit: int,
) -> list[list[slice]]:
    offsets = _distribute_hits_impl(hits, offset)
    hits_with_offset = [h - o for h, o in zip(hits, offsets)]
    hits = hits[:]
    if max_hits is not None:
        hits = _distribute_hits_impl(hits_with_offset, max_hits)
    result: list[list[slice]] = []
    for i, hit in enumerate(hits):
        slices = []
        offset = offsets[i]
        fullstop = hit + offset
        for start in range(offset, fullstop, page_limit):
            stop = start + min(page_limit, fullstop - start)
            slices.append(slice(start, stop))
        result.append(slices)
    return result


FileFieldParams = ["*"]
DatasetFieldParams = [
    "instance_id",
    "data_node",
    "size",
    "number_of_files",
]


@dataclass
class Context:
    config: Config = field(default_factory=Config.default)
    client: AsyncClient = field(
        init=False,
        repr=False,
    )
    semaphores: dict[str, asyncio.Semaphore] = field(
        init=False,
        repr=False,
        default_factory=dict,
    )
    noraise: bool = False

    # def __init__(
    #     self,
    #     config: Config | None = None,
    #     *,
    #     # since: str | datetime | None = None,
    # ):
    #     # if since is None:
    #     #     self.since = since
    #     # else:
    #     #     self.since = format_date_iso(since)

    async def __aenter__(self) -> Context:
        if hasattr(self, "client"):
            raise Exception("Context is already initialized.")
        self.client = AsyncClient(timeout=self.config.api.http_timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        if not hasattr(self, "client"):
            raise Exception("Context is not initialized.")
        await self.client.aclose()
        del self.client

    def prepare_hits(
        self,
        *queries: Query,
        file: bool,
        index_url: str | None = None,
        index_node: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[ResultHits]:
        results = []
        for i, query in enumerate(queries):
            result = ResultHits(query, file)
            result.prepare(
                index_node=index_node or self.config.api.index_node,
                page_limit=0,
                index_url=index_url,
                date_from=date_from,
                date_to=date_to,
            )
            results.append(result)
        return results

    def prepare_hints(
        self,
        *queries: Query,
        file: bool,
        facets: list[str],
        index_url: str | None = None,
        index_node: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[ResultHints]:
        results = []
        for i, query in enumerate(queries):
            result = ResultHints(query, file)
            result.prepare(
                index_node=index_node or self.config.api.index_node,
                page_limit=0,
                facets_param=facets,
                index_url=index_url,
                date_from=date_from,
                date_to=date_to,
            )
            results.append(result)
        return results

    def prepare_search(
        self,
        *queries: Query,
        file: bool,
        hits: list[int],
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        index_url: str | None = None,
        index_node: str | None = None,
        fields_param: list[str] | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[ResultSearch]:
        if page_limit is None:
            page_limit = self.config.api.page_limit
        if fields_param is None:
            if file:
                fields_param = FileFieldParams
            else:
                fields_param = DatasetFieldParams
        slices = _distribute_hits(
            hits=hits,
            offset=offset,
            max_hits=max_hits,
            page_limit=page_limit,
        )
        results = []
        for query, query_slices in zip(queries, slices):
            for sl in query_slices:
                result = ResultSearch(query, file=file)
                result.prepare(
                    index_node=index_node or self.config.api.index_node,
                    offset=sl.start,
                    page_limit=sl.stop - sl.start,
                    fields_param=fields_param,
                    index_url=index_url,
                    date_from=date_from,
                    date_to=date_to,
                )
                results.append(result)
        return results

    def prepare_search_distributed(
        self,
        *queries: Query,
        file: bool,
        hints: list[HintsDict],
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        fields_param: list[str] | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[ResultSearch]:
        if page_limit is None:
            page_limit = self.config.api.page_limit
        if fields_param is None:
            if file:
                fields_param = FileFieldParams
            else:
                fields_param = DatasetFieldParams
        hits = self.hits_from_hints(*hints)
        if max_hits is not None:
            hits = _distribute_hits_impl(hits, max_hits)
        results = []
        not_distrib = Query(options=dict(distrib=False))
        for query, query_hints, query_max_hits in zip(queries, hints, hits):
            nodes = query_hints["index_node"]
            nodes_hits = [nodes[node] for node in nodes]
            slices = _distribute_hits(
                hits=nodes_hits,
                offset=offset,
                max_hits=query_max_hits,
                page_limit=page_limit,
            )
            for node, node_slices in zip(nodes, slices):
                for sl in node_slices:
                    result = ResultSearch(query << not_distrib, file=file)
                    result.prepare(
                        index_node=node,
                        offset=sl.start,
                        page_limit=sl.stop - sl.start,
                        fields_param=fields_param,
                        date_from=date_from,
                        date_to=date_to,
                    )
                    results.append(result)
        return results

    async def _fetch_one(self, result: RT) -> RT:
        host = result.request.url.host
        if host not in self.semaphores:
            max_concurrent = self.config.api.max_concurrent
            self.semaphores[host] = asyncio.Semaphore(max_concurrent)
        async with self.semaphores[host]:
            logger.debug(f"GET {host} params={result.request.url.params}")
            try:
                resp = await self.client.send(result.request)
                resp.raise_for_status()
                result.json = json.loads(
                    resp.content.decode(encoding="latin-1")
                )
                logger.info(f"✓ Fetched in {resp.elapsed}s {resp.url}")
            except HTTPError as exc:
                result.exc = exc
            except (Exception, asyncio.CancelledError) as exc:
                result.exc = exc
            return result

    async def _fetch(self, *in_results: RT) -> AsyncIterator[RT]:
        tasks = [
            asyncio.create_task(self._fetch_one(result))
            for result in in_results
        ]
        excs = []
        for task in tasks:
            result = await task
            yield result
            if result.exc is not None:
                excs.append(result.exc)
        if excs:
            group = BaseExceptionGroup("fetch", excs)
            if self.noraise:
                logger.exception(group)
                for exc in excs:
                    logger.exception(exc)
            else:
                raise group

    async def _hits(self, *results: ResultHits) -> list[int]:
        hits = []
        async for result in self._fetch(*results):
            result.process()
            if result.processed:
                hits.append(result.data)
        return hits

    async def _hints(self, *results: ResultHints) -> list[HintsDict]:
        hints: list[HintsDict] = []
        async for result in self._fetch(*results):
            result.process()
            if result.processed:
                hints.append(result.data)
        return hints

    async def _datasets(
        self,
        *results: ResultSearch,
        keep_duplicates: bool,
    ) -> list[Dataset]:
        datasets: list[Dataset] = []
        ids: set[str] = set()
        async for result in self._fetch(*results):
            dataset_result = result.to(ResultDatasets)
            dataset_result.process()
            if dataset_result.processed:
                for d in dataset_result.data:
                    if not keep_duplicates and d.dataset_id in ids:
                        logger.warning(f"Duplicate dataset {d.dataset_id}")
                    else:
                        datasets.append(d)
                        ids.add(d.dataset_id)
        return datasets

    async def _files(
        self,
        *results: ResultSearch,
        keep_duplicates: bool,
    ) -> list[File]:
        files: list[File] = []
        shas: set[str] = set()
        async for result in self._fetch(*results):
            files_result = result.to(ResultFiles)
            files_result.process()
            if files_result.processed:
                for file in files_result.data:
                    if not keep_duplicates and file.sha in shas:
                        logger.warning(f"Duplicate file {file.file_id}")
                    else:
                        files.append(file)
                        shas.add(file.sha)
        return files

    async def _search_as_queries(
        self,
        *results: ResultSearch,
        keep_duplicates: bool,
    ) -> list[Query]:
        queries: list[Query] = []
        async for result in self._fetch(*results):
            queries_result = result.to(ResultSearchAsQueries)
            queries_result.process()
            if queries_result.processed:
                for query in queries_result.data:
                    queries.append(query)
        return queries

    async def _with_client(self, coro: Coroutine[None, None, T]) -> T:
        """
        Async wrapper to create client before await future.
        This is required since asyncio does not provide a way
        to enter an async context in a sync function.
        """
        async with self:
            return await coro

    def free_semaphores(self) -> None:
        self.semaphores = {}

    def _sync(self, coro: Coroutine[None, None, T]) -> T:
        """
        Reset semaphore to ensure none is bound to an expired event loop.
        Run through `_with_client` wrapper to use `async with` synchronously.
        """
        self.free_semaphores()
        return sync(self._with_client(coro))

    async def _gather(self, *coros: Coroutine[None, None, T]) -> list[T]:
        return await asyncio.gather(*coros)

    def sync_gather(self, *coros: Coroutine[None, None, T]) -> list[T]:
        return self._sync(self._gather(*coros))

    def hits(
        self,
        *queries: Query,
        file: bool,
        index_url: str | None = None,
        index_node: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[int]:
        results = self.prepare_hits(
            *queries,
            file=file,
            index_url=index_url,
            index_node=index_node,
            date_from=date_from,
            date_to=date_to,
        )
        return self._sync(self._hits(*results))

    def hits_from_hints(self, *hints: HintsDict) -> list[int]:
        result: list[int] = []
        for hint in hints:
            if len(hint) > 0:
                key = next(iter(hint))
                num = sum(hint[key].values())
            else:
                num = 0
            result.append(num)
        return result

    def hints(
        self,
        *queries: Query,
        file: bool,
        facets: list[str],
        index_url: str | None = None,
        index_node: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[HintsDict]:
        results = self.prepare_hints(
            *queries,
            file=file,
            facets=facets,
            index_url=index_url,
            index_node=index_node,
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
    ) -> list[Dataset]:
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
        if hits is None:
            hits = self.hits(*queries, file=file)
        results = self.prepare_search(
            *queries,
            file=file,
            hits=hits,
            offset=offset,
            page_limit=page_limit,
            max_hits=max_hits,
            date_from=date_from,
            date_to=date_to,
            fields_param=["*"],
        )
        coro = self._search_as_queries(
            *results,
            keep_duplicates=keep_duplicates,
        )
        return self._sync(coro)

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
    ) -> Sequence[File | Dataset]:
        fun: Callable[..., Sequence[File | Dataset]]
        if file:
            fun = self.files
        else:
            fun = self.datasets
        return fun(
            *queries,
            hits=hits,
            offset=offset,
            max_hits=max_hits,
            page_limit=page_limit,
            date_from=date_from,
            date_to=date_to,
            keep_duplicates=keep_duplicates,
        )
