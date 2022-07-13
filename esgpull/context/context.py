from collections.abc import AsyncIterator
from typing import TypeAlias, Optional

import asyncio
import httpx
import pandas
import datetime
from urllib.parse import urlparse

from esgpull.context.facets import Facets
from esgpull.context.constants import DEFAULT_ESGF_INDEX


# workaround for notebooks with running event loop
if asyncio.get_event_loop().is_running():
    import nest_asyncio

    nest_asyncio.apply()

FacetCounts: TypeAlias = dict[str, dict[str, int]]


def url_to_index(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc == "":
        return parsed.path
    else:
        return parsed.netloc


def index_to_url(index: str) -> str:
    return "https://" + url_to_index(index) + "/esg-search/search"


def format_date(
    date: Optional[str | datetime.datetime], fmt: str = "%Y-%m-%d"
) -> Optional[str]:
    result: Optional[str]
    if date is None:
        return None
    match date:
        case datetime.datetime():
            ...
        case str():
            date = datetime.datetime.strptime(date, fmt)
        case _:
            raise ValueError(date)
    return date.replace(microsecond=0).isoformat() + "Z"


class Context:
    """
    TODO: add storage query builder from this
    """

    def __init__(
        self,
        selection_file_path: str = None,
        /,
        *,
        # index: str = DEFAULT_ESGF_INDEX,
        fields: str = "*",
        # facets: str = "*",
        latest: bool = None,
        replica: bool = None,
        distrib: bool = False,
        max_concurrent: int = 5,
        search_batchsize: int = 50,
        last_update: Optional[str | datetime.datetime] = None,
        show_url: bool = False,
    ):
        # self.index = url_to_index(index)
        # self.url = index_to_url(self.index)
        self.fields = fields
        # self.facets = facets
        self.latest = latest
        self.replica = replica
        self.distrib = distrib
        self.max_concurrent = max_concurrent
        self.search_batchsize = search_batchsize
        self.last_update = format_date(last_update)
        self.show_url = show_url
        self._facets = Facets()
        if selection_file_path is not None:
            self._facets.load(selection_file_path)

    @property
    def query(self) -> Facets:
        return self._facets

    def _build_query(
        self,
        dump: dict[str, str],
        file=False,
        limit=50,
        offset=None,
        **extra,
    ) -> dict:
        query = {
            "fields": self.fields,
            # "facets": self.facets,
            "type": "File" if file else "Dataset",
            "limit": limit,
            "offset": offset,
            "latest": self.latest,
            "distrib": self.distrib,
            "replica": self.replica,
            "from": self.last_update,
            "format": "application/solr+json",
            **dump,
            **extra,
        }
        if "index_node" in query:
            url = index_to_url(query.pop("index_node"))
            query["url"] = url
        if "url" not in query:
            query["url"] = index_to_url(DEFAULT_ESGF_INDEX)
        if "start" in query:
            query["start"] = format_date(query["start"])
        if "end" in query:
            query["end"] = format_date(query["end"])
        # TODO: add coverage temporal constraints `start/end`
        # TODO: add nominal temporal constraints `to`
        return {k: v for k, v in query.items() if v is not None}

    def _build_queries(self, **extra) -> list[dict]:
        result = []
        for dump in self.query.dump_flat():
            query = self._build_query(dump, **extra)
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

    def _build_queries_search(self, hits: list[int], file: bool) -> list[dict]:
        batchsize = self.search_batchsize
        queries = self._build_queries(limit=batchsize, offset=0, file=file)
        for i, query in list(enumerate(queries)):
            offsets = range(batchsize, hits[i], batchsize)
            for offset in offsets:
                queries.append(query | dict(offset=str(offset)))
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

    async def _search(self, file: bool) -> list[dict]:
        hits = await self._hits(file)
        if sum(hits) > 200:
            raise ValueError(f"Too many: {sum(hits)}")
        queries = self._build_queries_search(hits, file)
        ids = set()
        result = []
        async for json in self._fetch(queries):
            for doc in json["response"]["docs"]:
                if doc["id"] not in ids:
                    result.append(doc)
                    ids.add(doc["id"])
        nb_dropped = sum(hits) - len(ids)
        if nb_dropped:
            print(f"Dropped {nb_dropped} duplicate results.")
        return result

    @property
    def hits(self) -> list[int]:
        return asyncio.run(self._hits())

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
        self, *, file=False, todf=True
    ) -> list[dict] | pandas.DataFrame:
        results = asyncio.run(self._search(file))
        if todf:
            return pandas.DataFrame(results)
        else:
            return results

    def __repr__(self) -> str:
        # return f"Context(index={self.index}, facets={self.query.flatten()})"
        return f"Context(facets={self.query.flatten()})"

    # def __hash__(self) -> int:
    #     """
    #     Might be useful to cache pyesgf `hit_count` calls at some point.
    #     """
    #     return hash(self.index) ^ hash(self.query)


if __name__ == "__main__":
    # TODO: use these as unit tests
    c1 = Context()
    print("c1:", c1)
    print("c1.query:", c1.query)
    print("list(c1.query):", list(c1.query))
    print()

    c2 = Context()
    c1.query["variable_id"] = "toto"
    c2.query.variable_id = "tutu"

    try:
        c1.query["variable"] = "toto"
    except Exception as e:
        print(e)

    print("c1.query.variable_id:", c1.query.variable_id)
    print("c2.query.variable_id:", c2.query.variable_id)
    print()
    print("c1.query:", c1.query)
    print("c2.query:", c2.query)
    print()
    print("c1.query.dump()", c1.query.dump())
    # print(c2.__dict__)
