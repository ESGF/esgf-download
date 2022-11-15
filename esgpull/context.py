import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterator, Collection, Coroutine, TypeAlias, TypeVar

import rich
from exceptiongroup import BaseExceptionGroup
from httpx import AsyncClient, HTTPError, Response

from esgpull.config import Config
from esgpull.db.models import File
from esgpull.exceptions import SolrUnstableQueryError
from esgpull.facet import FacetDict
from esgpull.query import Query
from esgpull.tui import logger
from esgpull.utils import Root, format_date, index2url

# workaround for notebooks with running event loop
if asyncio.get_event_loop().is_running():
    import nest_asyncio

    nest_asyncio.apply()

console = rich.console.Console()

FacetCounts: TypeAlias = dict[str, dict[str, int]]
T = TypeVar("T")


class Context:
    def __init__(
        self,
        config: Config | None = None,
        *,
        fields: str = "*",
        distrib: bool = False,
        retracted: bool = False,
        latest: bool | None = True,
        replica: bool | None = None,
        search_batchsize: int = 50,
        since: str | datetime | None = None,
        new_style: bool = True,
        index_nodes: list[str] | None = None,
    ):
        if config is None:
            config = Config.load(Root.get(noraise=True))
        self.config = config
        self.fields = fields
        self.latest = latest
        self.replica = replica
        self.distrib = distrib
        self.retracted = retracted
        self.semaphores: dict[str, asyncio.Semaphore] = {}
        self.search_batchsize = search_batchsize
        if since is None:
            self.since = since
        else:
            self.since = format_date(since)
        self.new_style = new_style
        self.index_nodes = index_nodes
        self.query = Query()

    def _build_query(
        self,
        facets: FacetDict,
        file: bool = False,
        limit: int = 50,
        offset: int | None = None,
        **extra,
    ) -> dict:
        query = {
            "fields": self.fields,
            "type": "File" if file else "Dataset",
            "limit": limit,
            "offset": offset,
            "latest": self.latest,
            "distrib": self.distrib,
            "replica": self.replica,
            "retracted": self.retracted,
            "from": self.since,
            "format": "application/solr+json",
            **extra,
        }
        if "index_node" in facets:
            url = index2url(str(facets.pop("index_node")))
            query["url"] = url
        elif "url" in facets:
            query["url"] = facets.pop("url")
        else:
            query["url"] = index2url(self.config.search.index_node)
        if "facets" in facets:
            facets_option = facets.pop("facets")
            if isinstance(facets_option, str):
                query["facets"] = facets_option
            elif isinstance(facets_option, Collection):
                query["facets"] = ",".join(facets_option)
            else:
                raise TypeError(facets_option)
        if "start" in facets:
            query["start"] = format_date(str(facets.pop("start")))
        if "end" in facets:
            query["end"] = format_date(str(facets.pop("end")))
        if self.new_style:
            facets_: list[str] = []
            if "query" in facets:
                freetext = facets.pop("query")
                if isinstance(freetext, str):
                    facets_.append(freetext)
                elif isinstance(freetext, Collection):
                    facets_.append(" ".join(freetext))
                else:
                    raise TypeError(freetext)
            for name, values in facets.items():
                if isinstance(values, list):
                    values = "(" + " ".join(values) + ")"
                facets_.append(f"{name}:{values}")
            query_ = " AND ".join(facets_)
            if query_:
                query["query"] = query_
        else:
            query.update(facets)
        # [?]TODO: add nominal temporal constraints `to`
        return {k: v for k, v in query.items() if v is not None}

    def _build_queries(
        self,
        offsets: list[int] | None = None,
        **extra,
    ) -> list[dict]:
        result = []
        offset: int | None = None
        for i, flat in enumerate(self.query.flatten()):
            if offsets is not None:
                offset = offsets[i]
            query = self._build_query(flat.dump(), offset=offset, **extra)
            result.append(query)
        return result

    def _build_queries_hits(self, file: bool) -> list[dict]:
        return self._build_queries(limit=0, file=file)

    def _build_queries_facet_counts(self, file: bool) -> list[dict]:
        queries = self._build_queries(limit=0, file=file)
        for query in queries:
            if "facets" not in query:
                query["facets"] = "*"
        return queries

    def _adjust_hits(self, hits: list[int], max_results: int) -> list[int]:
        hits = list(hits)
        indices = list(range(len(hits)))
        i_idx = 0
        total = sum(hits)
        while total > max_results and max_results > 0:
            # `or 1` here to account for the case `max_results < len(hits)`
            max_hit = (max_results // len(indices)) or 1
            i = indices[i_idx]
            count = hits[i]
            if count <= max_hit:
                total -= count
                max_results -= count
                indices.pop(i_idx)
            else:
                diff = min(count % max_hit or max_hit, total - max_results)
                hits[i] -= diff
                total -= diff
                i_idx += 1
            i_idx %= len(indices)
        if total > max_results:
            for i in indices:
                hits[i] = 0
        return hits

    def _build_queries_search(
        self,
        hits: list[int],
        file: bool,
        max_results: int | None = 200,
        offset: int = 0,
        batchsize: int | None = None,
    ) -> list[dict]:
        better_distrib: bool
        index_nodes: list[str]
        if self.distrib and self.index_nodes:
            better_distrib = True
            index_nodes = self.index_nodes
            if max_results is not None:
                max_results = max_results // len(index_nodes)
            original_query = self.query.clone()
            original_distrib = self.distrib
            self.distrib = False
        else:
            better_distrib = False
            index_nodes = list(self.query.index_node.values)
        if batchsize is None:
            batchsize = self.search_batchsize
        if offset:
            offsets = self._adjust_hits(hits, offset)
            hits = [h - o for h, o in zip(hits, offsets)]
            if max_results is not None:
                hits = self._adjust_hits(hits, max_results)
        else:
            offsets = [0 for _ in hits]
            if max_results is not None:
                hits = self._adjust_hits(hits, max_results)
        queries = []
        for index_node in index_nodes:
            self.query.index_node = index_node
            raw_queries = self._build_queries(
                offsets=offsets,
                file=file,
            )
            for i, query in list(enumerate(raw_queries)):
                nb_queries = (hits[i] - 1) // batchsize + 1
                query_offset = offsets[i]
                for j in range(nb_queries):
                    offset = query_offset + j * batchsize
                    limit = min(batchsize, hits[i] + query_offset - offset)
                    queries.append(query | dict(limit=limit, offset=offset))
        if better_distrib:
            self.distrib = original_distrib
            self.query = original_query
        return queries

    @asynccontextmanager
    async def _client(self) -> AsyncIterator[AsyncClient]:
        try:
            client = AsyncClient(timeout=self.config.search.http_timeout)
            yield client
        finally:
            await client.aclose()

    async def _fetch_one(self, url, query, client, sem) -> Response:
        async with sem:
            request = client.build_request("GET", url=url, params=query)
            logger.debug(f"GET {url} params={query}")
            return await client.send(request)

    async def _fetch(self, queries) -> AsyncIterator[dict]:
        async with self._client() as client:
            coroutines = []
            for query in queries:
                url = query.pop("url")
                self.semaphores.setdefault(
                    url,
                    asyncio.Semaphore(self.config.search.max_concurrent),
                )
                sem = self.semaphores[url]
                coro = self._fetch_one(url, query, client, sem)
                coroutines.append(asyncio.ensure_future(coro))
            excs = []
            for future in coroutines:
                try:
                    resp = await future
                    resp.raise_for_status()
                    logger.info(f"✓ Fetched in {resp.elapsed}s {resp.url}")
                    yield resp.json()
                except HTTPError as exc:
                    logger.error(exc)
                except (Exception, asyncio.CancelledError) as exc:
                    excs.append(exc)
            if excs:
                raise BaseExceptionGroup("fetch", excs)

    async def _hits(self, file=False) -> list[int]:
        result = []
        queries = self._build_queries_hits(file)
        async for json in self._fetch(queries):
            result.append(json["response"]["numFound"])
        return result

    def _raise_on_distrib_facet_counts(self) -> None:
        if self.distrib and self.query.facets.isdefault():
            raise SolrUnstableQueryError(self)

    async def _facet_counts(
        self, file=False
    ) -> tuple[list[int], list[FacetCounts]]:
        self._raise_on_distrib_facet_counts()
        facet_counts: list[FacetCounts] = []
        hit_counts: list[int] = []
        queries = self._build_queries_facet_counts(file)
        async for json in self._fetch(queries):
            facets: FacetCounts = {}
            facet_fields = json["facet_counts"]["facet_fields"]
            for facet, value_count in facet_fields.items():
                facets[facet] = {}
                for i in range(0, len(value_count), 2):
                    value: str = value_count[i]
                    count: int = value_count[i + 1]
                    facets[facet][value] = count
            facet_counts.append(facets)
            hit_count: int = json["response"]["numFound"]
            hit_counts.append(hit_count)
        return hit_counts, facet_counts

    async def _search(
        self,
        file: bool,
        max_results: int | None = 200,
        offset: int = 0,
        hits: list[int] | None = None,
        keep_duplicates: bool = False,
    ) -> list[dict]:
        if hits is None:
            hits = await self._hits(file)
        queries = self._build_queries_search(
            hits=hits,
            file=file,
            max_results=max_results,
            offset=offset,
        )
        checksums = set()
        result = []
        nb_bad = 0
        async for json in self._fetch(queries):
            for doc in json["response"]["docs"]:
                if keep_duplicates:
                    result.append(doc)
                    continue
                if file:
                    try:
                        f = File.from_dict(doc)
                    except KeyError:
                        if nb_bad == 0:
                            msg = "File with invalid metadata (1st occurence):"
                            logger.warning(f"{msg}\n{doc}")
                        nb_bad += 1
                        continue
                    checksum = f.checksum
                else:
                    checksum = doc["instance_id"]
                if checksum not in checksums:
                    result.append(doc)
                    checksums.add(checksum)
        if max_results is None:
            nb_expected = sum(hits)
        else:
            nb_expected = min(sum(hits), max_results)
        f_or_d = "file" if file else "dataset"
        if nb_bad:
            s = "s" if nb_bad > 1 else ""
            logger.warning(
                f"Dropped {nb_bad} {f_or_d}{s} with invalid metadata."
            )
        if not keep_duplicates:
            nb_dup = nb_expected - len(checksums) - nb_bad
            if nb_dup:
                s = "s" if nb_dup > 1 else ""
                logger.info(f"Dropped {nb_dup} duplicate {f_or_d}{s}.")
        return result

    def free_semaphores(self) -> None:
        """
        Ensure no semaphore is bound to an expired event loop.
        """
        self.semaphores = {}

    def sync_run(self, coro: Coroutine[None, None, T]) -> T:
        self.free_semaphores()
        return asyncio.run(coro)

    @property
    def hits(self) -> list[int]:
        return self.sync_run(self._hits())

    @property
    def file_hits(self) -> list[int]:
        return self.sync_run(self._hits(file=True))

    def facet_counts(self, file: bool = False) -> list[FacetCounts]:
        hits, facets = self.sync_run(self._facet_counts(file=file))
        return facets

    def options(
        self,
        file=False,
        facets: list[str] | None = None,
    ) -> list[FacetCounts]:
        if facets is not None:
            original_facets = self.query.facets.values
            self.query.facets = facets
        queries = self.query.flatten()
        result = []
        for query, facet_counts in zip(queries, self.facet_counts(file=file)):
            facet_options = {}
            for facet, counts in facet_counts.items():
                # force all facets if specified, no more no less
                if facets is not None:
                    if facet in facets:
                        facet_options[facet] = counts
                    continue
                # discard non-facets
                if facet not in self.query._facets:
                    continue
                # keep only when there are 2 or more options
                if len(counts) > 1:
                    facet_options[facet] = counts
            result.append(facet_options)
        if facets is not None:
            self.query.facets = original_facets
        return result

    def search(
        self,
        *,
        file=False,
        max_results: int | None = 200,
        offset: int = 0,
        hits: list[int] | None = None,
        keep_duplicates: bool = False,
    ) -> list[dict]:
        coro = self._search(
            file,
            max_results=max_results,
            offset=offset,
            hits=hits,
            keep_duplicates=keep_duplicates,
        )
        return self.sync_run(coro)

    def __repr__(self) -> str:
        return f"Context(query={self.query})"
