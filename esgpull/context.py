from pathlib import Path
from typing import TypeAlias, Optional
from collections.abc import AsyncIterator

import asyncio
import httpx
import datetime

from esgpull.query import Query
from esgpull.types import FacetDict
from esgpull.constants import DEFAULT_ESGF_INDEX
from esgpull.utils import format_date, index2url


# workaround for notebooks with running event loop
if asyncio.get_event_loop().is_running():
    import nest_asyncio

    nest_asyncio.apply()

FacetCounts: TypeAlias = dict[str, dict[str, int]]


class Context:
    """
    [+]TODO: add storage query builder from this
    """

    def __init__(
        self,
        selection_file_path: Optional[str | Path] = None,
        /,
        *,
        # index: str = DEFAULT_ESGF_INDEX,
        fields: str = "*",
        # facets: str = "*",
        latest: bool = None,
        replica: bool = None,
        distrib: bool = False,
        retracted: bool = False,
        max_concurrent: int = 5,
        search_batchsize: int = 50,
        last_update: Optional[str | datetime.datetime] = None,
        show_url: bool = False,
        new_style: bool = True,
    ):
        self.fields = fields
        self.latest = latest
        self.replica = replica
        self.distrib = distrib
        self.retracted = retracted
        self.max_concurrent = max_concurrent
        self.search_batchsize = search_batchsize
        if last_update is None:
            self.last_update = last_update
        else:
            self.last_update = format_date(last_update)
        self.show_url = show_url
        self.new_style = new_style
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
            "from": self.last_update,
            "format": "application/solr+json",
            **extra,
        }
        if "index_node" in facets:
            url = index2url(str(facets.pop("index_node")))
            query["url"] = url
        elif "url" in facets:
            query["url"] = facets.pop("url")
        else:
            query["url"] = index2url(DEFAULT_ESGF_INDEX)
        if "start" in facets:
            facets["start"] = format_date(str(facets["start"]))
        if "end" in facets:
            facets["end"] = format_date(str(facets["end"]))
        if self.new_style:
            facets_: list[str] = []
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
        offset: Optional[int] = None
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
        max_results: int = 200,
        offset: int = 0,
        batchsize: int = None,
    ) -> list[dict]:
        if batchsize is None:
            batchsize = self.search_batchsize
        if offset:
            offsets = self._adjust_hits(hits, offset)
            hits = [h - o for h, o in zip(hits, offsets)]
            hits = self._adjust_hits(hits, max_results)
        else:
            offsets = [0 for _ in hits]
            hits = self._adjust_hits(hits, max_results)
        raw_queries = self._build_queries(offsets=offsets, file=file)
        queries = []
        for i, query in list(enumerate(raw_queries)):
            nb_queries = (hits[i] - 1) // batchsize + 1
            query_offset = offsets[i]
            for j in range(nb_queries):
                offset = query_offset + j * batchsize
                limit = min(batchsize, hits[i] + query_offset - offset)
                queries.append(query | dict(limit=limit, offset=offset))
        return queries

    async def _fetch_one(self, query, client, sem) -> httpx.Response:
        url = query.pop("url")
        async with sem:
            return await client.get(url, params=query)

    async def _fetch(self, queries) -> AsyncIterator[dict]:
        client = httpx.AsyncClient(timeout=20)
        sem = asyncio.Semaphore(self.max_concurrent)
        tasks = []
        for query in queries:
            task = asyncio.ensure_future(self._fetch_one(query, client, sem))
            tasks.append(task)
        for i, future in enumerate(tasks):
            try:
                resp = await future
                resp.raise_for_status()
                if self.show_url:
                    print(resp.url)
                yield resp.json()
            except Exception as e:
                print(queries[i])
                print(e)
        await client.aclose()

    async def _hits(self, file=False) -> list[int]:
        result = []
        queries = self._build_queries_hits(file)
        async for json in self._fetch(queries):
            result.append(json["response"]["numFound"])
        return result

    async def _facet_counts(
        self, file=False
    ) -> tuple[list[int], list[FacetCounts]]:
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
        self, file: bool, max_results: int = 200, offset: int = 0
    ) -> list[dict]:
        hits = await self._hits(file)
        queries = self._build_queries_search(
            hits=hits, file=file, max_results=max_results, offset=offset
        )
        ids = set()
        result = []
        async for json in self._fetch(queries):
            for doc in json["response"]["docs"]:
                if doc["id"] not in ids:
                    result.append(doc)
                    ids.add(doc["id"])
        nb_dropped = min(sum(hits), max_results) - len(ids)
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

    # @property
    def options(self, file=False) -> list[FacetCounts]:
        all_flat = self.query.flatten()
        all_hits, all_facet_counts = asyncio.run(self._facet_counts(file))
        result = []
        for flat, hits, facet_counts in zip(
            all_flat, all_hits, all_facet_counts
        ):
            facet_options = {}
            for facet, counts in facet_counts.items():
                if facet not in self.query._facets:
                    continue  # discard non-facets
                if not flat[facet].isdefault():
                    continue  # discard facets with value
                if len(counts) > 1:
                    facet_options[facet] = counts
            result.append(facet_options)
        return result

    def search(
        self, *, file=False, max_results: int = 200, offset: int = 0
    ) -> list[dict]:
        coro = self._search(file, max_results=max_results, offset=offset)
        return asyncio.run(coro)

    def __repr__(self) -> str:
        return f"Context(query={self.query})"


__all__ = ["Context"]
