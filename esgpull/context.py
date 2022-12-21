from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Coroutine, TypeAlias, TypeVar

import rich
from exceptiongroup import BaseExceptionGroup
from httpx import AsyncClient, HTTPError, Request
from rich.pretty import pretty_repr

from esgpull.config import Config
from esgpull.exceptions import SolrUnstableQueryError
from esgpull.models import File, Query
from esgpull.tui import logger
from esgpull.utils import index2url  # , format_date

# from datetime import datetime

# workaround for notebooks with running event loop
if asyncio.get_event_loop().is_running():
    import nest_asyncio

    nest_asyncio.apply()

console = rich.console.Console()
FacetCounts: TypeAlias = dict[str, dict[str, int]]

DangerousFacets = set(
    [
        "instance_id",
        "dataset_id",
        "master_id",
        "tracking_id",
        "url",
    ]
)


@dataclass
class Result:
    query: Query
    file: bool
    request: Request = field(init=False, repr=False)
    json: dict[str, Any] = field(init=False, repr=False)
    exc: BaseException | None = field(init=False, default=None, repr=False)

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
        #     query["start"] = format_date(str(facets.pop("start")))
        # if "end" in facets:
        #     query["end"] = format_date(str(facets.pop("end")))
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


def _distribute_hits_impl(hits: list[int], max_hits: int) -> list[int]:
    i = total = 0
    N = len(hits)
    accs = [0.0 for _ in range(N)]
    result = [0 for _ in range(N)]
    steps = [h / sum(hits) for h in hits]
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


@dataclass
class Context:
    config: Config = field(default_factory=Config.default)
    # index_nodes: list[str] | None = None
    client: AsyncClient = field(
        init=False,
        repr=False,
    )
    semaphores: dict[str, asyncio.Semaphore] = field(
        init=False,
        repr=False,
        default_factory=dict,
    )

    # def __init__(
    #     self,
    #     config: Config | None = None,
    #     *,
    #     # since: str | datetime | None = None,
    #     index_nodes: list[str] | None = None,
    # ):
    #     # if since is None:
    #     #     self.since = since
    #     # else:
    #     #     self.since = format_date(since)
    #     # self.index_nodes = index_nodes

    async def __aenter__(self) -> Context:
        if hasattr(self, "client"):
            raise Exception("Context is already initialized.")
        self.client = AsyncClient(timeout=self.config.search.http_timeout)
        return self

    async def __aexit__(self, *exc) -> None:
        if not hasattr(self, "client"):
            raise Exception("Context is not initialized.")
        await self.client.aclose()
        del self.client

    def _init_hits(
        self,
        *queries: Query,
        file: bool,
        index_url: str | None = None,
        index_node: str | None = None,
    ) -> list[Result]:
        results: list[Result] = []
        for i, query in enumerate(queries):
            result = Result(query, file)
            result.prepare(
                index_node=index_node or self.config.search.index_node,
                page_limit=0,
                index_url=index_url,
            )
            results.append(result)
        return results

    def _init_facet_counts(
        self,
        *queries: Query,
        file: bool,
        facets: list[str],
        index_url: str | None = None,
        index_node: str | None = None,
    ) -> list[Result]:
        results: list[Result] = []
        for i, query in enumerate(queries):
            result = Result(query, file)
            result.prepare(
                index_node=index_node or self.config.search.index_node,
                page_limit=0,
                facets_param=facets,
                index_url=index_url,
            )
            results.append(result)
        return results

    # def _init_results_search(
    #     self,
    #     hits: list[int],
    #     file: bool,
    #     max_results: int | None = 200,
    #     offset: int = 0,
    #     page_limit: int | None = None,
    # ) -> list[dict]:
    #     better_distrib: bool
    #     index_nodes: list[str]
    #     if self.distrib and self.index_nodes:
    #         better_distrib = True
    #         index_nodes = self.index_nodes
    #         if max_results is not None:
    #             max_results = max_results // len(index_nodes)
    #         original_query = self.query.clone()
    #         original_distrib = self.distrib
    #         self.distrib = False
    #     else:
    #         better_distrib = False
    #         index_nodes = list(self.query.index_node.values)
    #     if page_limit is None:
    #         page_limit = self.config.search.page_limit
    #     if offset:
    #         offsets = self._adjust_hits(hits, offset)
    #         hits = [h - o for h, o in zip(hits, offsets)]
    #         if max_results is not None:
    #             hits = self._adjust_hits(hits, max_results)
    #     else:
    #         offsets = [0 for _ in hits]
    #         if max_results is not None:
    #             hits = self._adjust_hits(hits, max_results)
    #     queries = []
    #     for index_node in index_nodes:
    #         self.query.index_node = index_node
    #         raw_queries = self._init_results(
    #             offsets=offsets,
    #             file=file,
    #         )
    #         for i, query in list(enumerate(raw_queries)):
    #             nb_queries = (hits[i] - 1) // page_limit + 1
    #             query_offset = offsets[i]
    #             for j in range(nb_queries):
    #                 offset = query_offset + j * page_limit
    #                 page_limit = min(
    #                     page_limit, hits[i] + query_offset - offset
    #                 )
    #                 queries.append(
    #                     query | dict(page_limit=page_limit, offset=offset)
    #                 )
    #     if better_distrib:
    #         self.distrib = original_distrib
    #         self.query = original_query
    #     return queries

    def _init_search(
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
    ) -> list[Result]:
        if page_limit is None:
            page_limit = self.config.search.page_limit
        slices = _distribute_hits(
            hits=hits,
            offset=offset,
            max_hits=max_hits,
            page_limit=page_limit,
        )
        results: list[Result] = []
        for i, query in enumerate(queries):
            for sl in slices[i]:
                result = Result(query, file=file)
                result.prepare(
                    index_node=index_node or self.config.search.index_node,
                    offset=sl.start,
                    page_limit=sl.stop - sl.start,
                    fields_param=fields_param,
                    # fields_param=FileRequires,
                    index_url=index_url,
                )
                results.append(result)
        return results

    async def _fetch_one(self, result: Result) -> Result:
        host = result.request.url.host
        if host not in self.semaphores:
            max_concurrent = self.config.search.max_concurrent
            self.semaphores[host] = asyncio.Semaphore(max_concurrent)
        async with self.semaphores[host]:
            logger.debug(f"GET {host} params={result.request.url.params}")
            try:
                resp = await self.client.send(result.request)
                resp.raise_for_status()
                result.json = resp.json()
                logger.info(f"✓ Fetched in {resp.elapsed}s {resp.url}")
            except HTTPError as exc:
                result.exc = exc
            except (Exception, asyncio.CancelledError) as exc:
                result.exc = exc
            return result

    async def _fetch(self, in_results: list[Result]) -> AsyncIterator[Result]:
        coros = [
            asyncio.ensure_future(self._fetch_one(result))
            for result in in_results
        ]
        excs = []
        for future in coros:
            result = await future
            yield result
            if result.exc is not None:
                excs.append(result.exc)
        if excs:
            raise BaseExceptionGroup("fetch", excs)

    async def _hits(
        self,
        *queries: Query,
        file: bool,
        index_url: str | None = None,
        index_node: str | None = None,
    ) -> list[int]:
        hits = []
        results = self._init_hits(
            *queries,
            file=file,
            index_url=index_url,
            index_node=index_node,
        )
        async for result in self._fetch(results):
            if result.success:
                hits.append(result.json["response"]["numFound"])
            else:
                hits.append(0)
        return hits

    async def _facet_counts(
        self,
        *queries: Query,
        file: bool,
        facets: list[str],
        index_url: str | None = None,
        index_node: str | None = None,
    ) -> list[FacetCounts]:
        results = self._init_facet_counts(
            *queries,
            file=file,
            facets=facets,
            index_url=index_url,
            index_node=index_node,
        )
        facet_counts: list[FacetCounts] = []
        async for result in self._fetch(results):
            if not result.success:
                facet_counts.append({})
                continue
            query_counts: FacetCounts = {}
            facet_fields = result.json["facet_counts"]["facet_fields"]
            for name, value_count in facet_fields.items():
                if len(value_count) == 0:
                    continue
                values: list[str] = value_count[::2]
                counts: list[int] = value_count[1::2]
                query_counts[name] = dict(zip(values, counts))
            facet_counts.append(query_counts)
        return facet_counts

    async def _search_raw(
        self,
        *queries: Query,
        file: bool,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        fields_param: list[str] | None = None,
    ) -> list[str]:
        if hits is None:
            hits = await self._hits(*queries, file=True)
        results = self._init_search(
            *queries,
            file=file,
            hits=hits,
            offset=offset,
            page_limit=page_limit,
            max_hits=max_hits,
            fields_param=fields_param,
            # index_url=index_url,
            # index_node=index_node,
        )
        docs: list[str] = []
        async for result in self._fetch(results):
            if result.success:
                docs.extend(result.json["response"]["docs"])
        return docs

    async def _search_files(
        self,
        *queries: Query,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        keep_duplicates: bool = False,
    ) -> list[File]:
        if hits is None:
            hits = await self._hits(*queries, file=True)
        results = self._init_search(
            *queries,
            file=True,
            hits=hits,
            offset=offset,
            page_limit=page_limit,
            max_hits=max_hits,
            fields_param=["*"]
            # index_url=index_url,
            # index_node=index_node,
        )
        files: list[File] = []
        nb_bad = 0
        shas: set[str] = set()
        async for result in self._fetch(results):
            if not result.success:
                continue
            for doc in result.json["response"]["docs"]:
                try:
                    f = File.serialize(doc)
                    if not keep_duplicates and f.sha in shas:
                        msg = "Duplicate file:"
                        logger.warning(f"{msg}\n{pretty_repr(doc)}")
                    else:
                        files.append(f)
                        shas.add(f.sha)
                except KeyError as exc:
                    logger.exception(exc)
                    if nb_bad == 0:
                        msg = "File with invalid metadata (1st occurence):"
                        logger.warning(f"{msg}\n{pretty_repr(doc)}")
                    nb_bad += 1
        if max_hits is None:
            nb_expected = sum(hits)
        else:
            nb_expected = min(sum(hits), max_hits)
        if nb_bad:
            s = "s" if nb_bad > 1 else ""
            logger.warning(f"Dropped {nb_bad} file{s} with invalid metadata.")
        nb_dup = nb_expected - len(files) - nb_bad
        if nb_dup:
            s = "s" if nb_dup > 1 else ""
            logger.info(f"Dropped {nb_dup} duplicate file{s}.")
        return files

    # async def _search_files(
    #     self,
    #     *queries: Query,
    #     hits: list[int],
    #     # file: bool,
    #     # offset: int = 0,
    #     page_limit: int | None = None,
    #     max_results: int | None = 200,
    #     keep_duplicates: bool = False,
    # ) -> list[dict]:
    #     # if hits is None:
    #     #     hits = await self._hits(*queries, file=file)
    #     requests_per_query = self._init_results_search(
    #         *queries,
    #         hits=hits,
    #         # file=file,
    #         # offset=offset,
    #         page_limit=page_limit,
    #         max_results=max_results,
    #     )
    #     checksums = set()
    #     result = []
    #     nb_bad = 0
    #     streams = [self._fetch(requests) for requests in requests_per_query]
    #     async with merge(*streams).stream() as stream:
    #         async for json in stream:
    #             for doc in json["response"]["docs"]:
    #                 if keep_duplicates:
    #                     result.append(doc)
    #                     continue
    #                 f = File.from_dict(doc)
    #                 f.compute_sha()
    #                 # if file:
    #                 #     try:
    #                 #         f = File.from_dict(doc)
    #                 #     except KeyError:
    #                 #         if nb_bad == 0:
    #                 #             msg = "File with invalid metadata (1st occurence):"
    #                 #             logger.warning(f"{msg}\n{doc}")
    #                 #         nb_bad += 1
    #                 #         continue
    #                 #     checksum = f.checksum
    #                 # else:
    #                 #     checksum = doc["instance_id"]
    #                 if checksum not in checksums:
    #                     result.append(doc)
    #                     checksums.add(checksum)
    #     if max_results is None:
    #         nb_expected = sum(hits)
    #     else:
    #         nb_expected = min(sum(hits), max_results)
    #     f_or_d = "file" if file else "dataset"
    #     if nb_bad:
    #         s = "s" if nb_bad > 1 else ""
    #         logger.warning(
    #             f"Dropped {nb_bad} {f_or_d}{s} with invalid metadata."
    #         )
    #     if not keep_duplicates:
    #         nb_dup = nb_expected - len(checksums) - nb_bad
    #         if nb_dup:
    #             s = "s" if nb_dup > 1 else ""
    #             logger.info(f"Dropped {nb_dup} duplicate {f_or_d}{s}.")
    #     return result

    T = TypeVar("T")

    async def _with_client(self, coro: Coroutine[None, None, T]) -> T:
        """
        Async wrapper to create client before calling coro.
        This is required since asyncio does not provide a way
        to enter an async context in a sync function.
        """
        async with self:
            return await coro

    def free_semaphores(self) -> None:
        self.semaphores = {}

    def sync_run(self, coro: Coroutine[None, None, T]) -> T:
        """
        Reset semaphore to ensure none is bound to an expired event loop.
        Run through `_with_client` wrapper to use `async with` synchronously.
        """
        self.free_semaphores()
        return asyncio.run(self._with_client(coro))

    def hits(
        self,
        *queries: Query,
        file: bool,
        index_url: str | None = None,
        index_node: str | None = None,
    ) -> list[int]:
        return self.sync_run(
            self._hits(
                *queries,
                file=file,
                index_url=index_url,
                index_node=index_node,
            )
        )

    def facet_counts(
        self,
        *queries: Query,
        file: bool,
        facets: list[str],
        index_url: str | None = None,
        index_node: str | None = None,
    ) -> list[FacetCounts]:
        return self.sync_run(
            self._facet_counts(
                *queries,
                file=file,
                facets=facets,
                index_url=index_url,
                index_node=index_node,
            )
        )

    # def options(
    #     self,
    #     file=False,
    #     facets: list[str] | None = None,
    # ) -> list[FacetCounts]:
    #     if facets is not None:
    #         original_facets = self.query.facets.values
    #         self.query.facets = facets
    #     queries = self.query.flatten()
    #     result = []
    #     for query, facet_counts in zip(queries, self.facet_counts(file=file)):
    #         facet_options = {}
    #         for facet, counts in facet_counts.items():
    #             # force all facets if specified, no more no less
    #             if facets is not None:
    #                 if facet in facets:
    #                     facet_options[facet] = counts
    #                 continue
    #             # discard non-facets
    #             if facet not in self.query._facets:
    #                 continue
    #             # keep only when there are 2 or more options
    #             if len(counts) > 1:
    #                 facet_options[facet] = counts
    #         result.append(facet_options)
    #     if facets is not None:
    #         self.query.facets = original_facets
    #     return result

    def search_raw(
        self,
        *queries: Query,
        file: bool,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        fields_param: list[str] | None = None,
    ) -> list[str]:
        return self.sync_run(
            self._search_raw(
                *queries,
                file=file,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
                page_limit=page_limit,
                fields_param=fields_param,
            )
        )

    def search_files(
        self,
        *queries: Query,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        keep_duplicates: bool = False,
    ) -> list[File]:
        return self.sync_run(
            self._search_files(
                *queries,
                hits=hits,
                offset=offset,
                max_hits=max_hits,
                page_limit=page_limit,
                keep_duplicates=keep_duplicates,
            )
        )
