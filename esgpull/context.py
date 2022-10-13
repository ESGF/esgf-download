from pathlib import Path
from typing import TypeAlias, AsyncIterator

import rich
import asyncio
from datetime import datetime
from httpx import Response, AsyncClient

from esgpull.query import Query
from esgpull.settings import Settings
from esgpull.types import FacetDict, File
from esgpull.utils import format_date, index2url
from esgpull.exceptions import SolrUnstableQueryError


# workaround for notebooks with running event loop
if asyncio.get_event_loop().is_running():
    import nest_asyncio

    nest_asyncio.apply()

console = rich.console.Console()

FacetCounts: TypeAlias = dict[str, dict[str, int]]


class Context:
    """
    [+]TODO: add storage query builder from this
    """

    def __init__(
        self,
        selection_file_path: str | Path | None = None,
        settings: Settings = None,
        /,
        *,
        fields: str = "*",
        distrib: bool = False,
        retracted: bool = False,
        latest: bool | None = None,
        replica: bool | None = None,
        max_concurrent: int = 5,
        search_batchsize: int = 50,
        since: str | datetime | None = None,
        show_url: bool = False,
        new_style: bool = True,
        index_nodes: list[str] | None = None,
    ):
        if settings is None:
            settings = Settings()
        self.settings = settings
        self.fields = fields
        self.latest = latest
        self.replica = replica
        self.distrib = distrib
        self.retracted = retracted
        self.max_concurrent = max_concurrent
        self.semaphores: dict[str, asyncio.Semaphore] = {}
        self.search_batchsize = search_batchsize
        if since is None:
            self.since = since
        else:
            self.since = format_date(since)
        self.show_url = show_url
        self.new_style = new_style
        self.index_nodes = index_nodes
        if selection_file_path is not None:
            self.query = Query.from_file(selection_file_path)
        else:
            self.query = Query()

    def _build_query(
        self,
        facets: FacetDict,
        file: bool = False,
        limit: int = 50,
        offset: int = None,
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
            query["url"] = index2url(self.settings.context.index_node)
        if "facets" in facets:
            query["facets"] = facets.pop("facets")
        if "start" in facets:
            facets["start"] = format_date(str(facets["start"]))
        if "end" in facets:
            facets["end"] = format_date(str(facets["end"]))
        if self.new_style:
            facets_: list[str] = []
            if "query" in facets:
                freetext = facets.pop("query")
                if isinstance(freetext, (list, tuple, set)):
                    facets_.append(" ".join(freetext))
                elif isinstance(freetext, str):
                    facets_.append(freetext)
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

    def _build_queries(self, offsets: list[int] = None, **extra) -> list[dict]:
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
        batchsize: int = None,
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

    async def _fetch_one(self, url, query, client, sem) -> Response:
        async with sem:
            return await client.get(url, params=query)

    async def _fetch(self, queries) -> AsyncIterator[dict]:
        client = AsyncClient(timeout=self.settings.context.http_timeout)
        coroutines = []
        for query in queries:
            url = query.pop("url")
            if url not in self.semaphores:
                self.semaphores[url] = asyncio.Semaphore(self.max_concurrent)
            sem = self.semaphores[url]
            coro = self._fetch_one(url, query, client, sem)
            coroutines.append(asyncio.ensure_future(coro))
        for future in coroutines:
            try:
                resp = await future
                resp.raise_for_status()
                if self.show_url:
                    print(resp.url)
                yield resp.json()
            except Exception:
                # TODO: clearer 404/501/TimeoutError/...
                console.print_exception(show_locals=True)
        await client.aclose()

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
        self, file: bool, max_results: int | None = 200, offset: int = 0
    ) -> list[dict]:
        hits = await self._hits(file)
        queries = self._build_queries_search(
            hits=hits, file=file, max_results=max_results, offset=offset
        )
        ids = set()
        result = []
        async for json in self._fetch(queries):
            for doc in json["response"]["docs"]:
                if file:
                    f = File.from_dict(doc)
                    _id = f.file_id
                else:
                    _id = doc["id"].partition("|")[0]
                if _id not in ids:
                    result.append(doc)
                    ids.add(_id)
        if max_results is None:
            nb_expected = sum(hits)
        else:
            nb_expected = min(sum(hits), max_results)
        nb_dropped = nb_expected - len(ids)
        if nb_dropped:
            print(f"Dropped {nb_dropped} duplicate results.")
        return result

    @property
    def hits(self) -> list[int]:
        return asyncio.run(self._hits())

    @property
    def file_hits(self) -> list[int]:
        return asyncio.run(self._hits(file=True))

    @property
    def facet_counts(self) -> list[FacetCounts]:
        hits, facets = asyncio.run(self._facet_counts())
        return facets

    def options(self, file=False) -> list[FacetCounts]:
        queries = self.query.flatten()
        _, all_facet_counts = asyncio.run(self._facet_counts(file))
        result = []
        for query, facet_counts in zip(queries, all_facet_counts):
            facet_options = {}
            for facet, counts in facet_counts.items():
                if facet not in self.query._facets:
                    continue  # discard non-facets
                if not query[facet].isdefault():
                    continue  # discard facets with value
                if len(counts) > 0:
                    facet_options[facet] = counts
            result.append(facet_options)
        return result

    def search(
        self, *, file=False, max_results: int | None = 200, offset: int = 0
    ) -> list[dict]:
        coro = self._search(file, max_results=max_results, offset=offset)
        return asyncio.run(coro)

    def __repr__(self) -> str:
        return f"Context(query={self.query})"


__all__ = ["Context"]
