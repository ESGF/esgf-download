from collections.abc import Sequence
from datetime import datetime
from typing import Any, Literal, TypeVar, overload

from pydantic import BaseModel, Field, PrivateAttr

from esgpull.config import Config
from esgpull.context.solr import SolrContext
from esgpull.context.stac import StacContext
from esgpull.context.types import HintsDict
from esgpull.context.utils import hits_from_hints
from esgpull.models import ApiBackend, DatasetRecord, File, Query

T = TypeVar("T")


class QueryList(BaseModel):
    queries: list[Query]

    def split_by_backend(self) -> dict[ApiBackend, list[Query]]:
        result: dict[ApiBackend, list[Query]]
        result = {backend: [] for backend in ApiBackend}
        for query in self.queries:
            match query.backend:
                case ApiBackend.solr | None:
                    result[ApiBackend.solr].append(query)
                case ApiBackend.stac:
                    result[ApiBackend.stac].append(query)
        return result

    def reorder_from_backend(
        self,
        backend_objs_map: dict[ApiBackend, list[T]],
    ) -> list[T]:
        result: list[T] = []
        cursors = dict.fromkeys(ApiBackend, 0)
        for query in self.queries:
            match query.backend:
                case ApiBackend.solr | None:
                    backend = ApiBackend.solr
                case ApiBackend.stac:
                    backend = ApiBackend.stac
            objs = backend_objs_map[backend]
            cursor = cursors[backend]
            result.append(objs[cursor])
            cursors[backend] += 1
        return result


class Context(BaseModel):
    model_config = {"arbitrary_types_allowed": True}

    config: Config = Field(default_factory=Config.default)
    noraise: bool = False
    _solr: SolrContext = PrivateAttr()
    _stac: StacContext = PrivateAttr()

    def model_post_init(self, context: Any):
        self._solr = SolrContext(config=self.config, noraise=self.noraise)
        self._stac = StacContext(config=self.config, noraise=self.noraise)

    def hits(
        self,
        *queries: Query,
        file: bool,
        index_url: str | None = None,
        index_node: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[int]:
        ql = QueryList(queries=list(queries))
        backend_queries_map = ql.split_by_backend()
        backend_hits_map: dict[ApiBackend, list[int]]
        backend_hits_map = {}
        for backend, backend_queries in backend_queries_map.items():
            match backend:
                case ApiBackend.solr:
                    backend_hits_map[backend] = self._solr.hits(
                        *backend_queries,
                        file=file,
                        index_url=index_url,
                        index_node=index_node,
                        date_from=date_from,
                        date_to=date_to,
                    )
                case ApiBackend.stac:
                    # TODO: warn about unused params index_url/index_node
                    backend_hits_map[backend] = self._stac.hits(
                        *backend_queries,
                        file=file,
                        # index_url=index_url,
                        # index_node=index_node,
                        date_from=date_from,
                        date_to=date_to,
                    )
        return ql.reorder_from_backend(backend_hits_map)

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
        ql = QueryList(queries=list(queries))
        backend_queries_map = ql.split_by_backend()
        backend_hints_map: dict[ApiBackend, list[HintsDict]]
        backend_hints_map = {}
        for backend, backend_queries in backend_queries_map.items():
            match backend:
                case ApiBackend.solr:
                    backend_hints_map[backend] = self._solr.hints(
                        *backend_queries,
                        file=file,
                        facets=facets,
                        index_url=index_url,
                        index_node=index_node,
                        date_from=date_from,
                        date_to=date_to,
                    )
                case ApiBackend.stac:
                    # TODO: warn about unused params index_url/index_node
                    backend_hints_map[backend] = self._stac.hints(
                        *backend_queries,
                        file=file,
                        facets=facets,
                        # index_url=index_url,
                        # index_node=index_node,
                        date_from=date_from,
                        date_to=date_to,
                    )
        return ql.reorder_from_backend(backend_hints_map)

    @overload
    def search(
        self,
        *queries: Query,
        file: Literal[False],
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        keep_duplicates: bool = True,
    ) -> Sequence[DatasetRecord]: ...

    @overload
    def search(
        self,
        *queries: Query,
        file: Literal[True],
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        keep_duplicates: bool = True,
    ) -> Sequence[File]: ...

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
        ql = QueryList(queries=list(queries))
        backend_queries_map = ql.split_by_backend()
        results: Sequence[File | DatasetRecord] = []
        for backend, backend_queries in backend_queries_map.items():
            match backend:
                case ApiBackend.solr:
                    results.extend(
                        self._solr.search(
                            *backend_queries,
                            file=file,
                            hits=hits,
                            offset=offset,
                            max_hits=max_hits,
                            page_limit=page_limit,
                            date_from=date_from,
                            date_to=date_to,
                            keep_duplicates=keep_duplicates,
                        ),
                    )
                case ApiBackend.stac:
                    results.extend(
                        self._stac.search(
                            *backend_queries,
                            file=file,
                            date_from=date_from,
                            date_to=date_to,
                            ## TODO: handle additional arguments if possible
                        ),
                    )
        return results

    def hits_from_hints(self, *hints: HintsDict) -> list[int]:
        return hits_from_hints(*hints)
