from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

from rich.console import Console, ConsoleOptions
from rich.pretty import pretty_repr
from rich.tree import Tree

from esgpull.config import Config
from esgpull.database import Database
from esgpull.exceptions import QueryDuplicate, TooShortKeyError, UnknownFacet
from esgpull.models import sql
from esgpull.models.facet import Facet
from esgpull.models.options import Options
from esgpull.models.query import Query, QueryDict
from esgpull.models.selection import Selection
from esgpull.models.tag import Tag
from esgpull.models.utils import rich_measure_impl


@dataclass(init=False, repr=False)
class Graph:
    db: Database | None
    queries: dict[str, Query]
    _shas_: set[str]
    _name_sha_: dict[str, str]
    _rendered: set[str]

    @classmethod
    def from_config(
        cls,
        config: Config,
        *queries: Query,
        force: bool = False,
        noraise: bool = False,
        load_db: bool = False,
    ) -> Graph:
        db = Database.from_config(config)
        return Graph(
            db,
            *queries,
            force=force,
            noraise=noraise,
            load_db=load_db,
        )

    def __init__(
        self,
        db: Database | None,
        *queries: Query,
        force: bool = False,
        noraise: bool = False,
        load_db: bool = False,
    ) -> None:
        self.db = db
        self.queries = {}
        self._shas_ = set()
        self._name_sha_ = {}
        self._load_db_shas()
        if load_db:
            self.load_db()
        self.add(*queries, force=force, noraise=noraise)

    @staticmethod
    def _expand_name(
        name: str,
        shas: set[str],
        name_sha: dict[str, str],
    ) -> str:
        if name in name_sha:
            sha = name_sha[name]
        elif name in shas:
            sha = name
        else:
            short_name = name
            if short_name.startswith("#"):
                short_name = short_name[1:]
            short_shas = {sha[: len(short_name)]: sha for sha in shas}
            if len(short_shas) < len(shas):
                raise TooShortKeyError(name)
            elif short_name not in short_shas:
                return name
            else:
                sha = short_shas[short_name]
        return sha

    def has(self, *, name: str | None = None, sha: str | None = None) -> bool:
        if name is not None and sha is None:
            sha = self._expand_name(name, self._shas_, self._name_sha_)
        if sha is None:
            raise ValueError("Missing `name` or `sha`")
        return sha in self._shas_

    def get(self, name: str) -> Query:
        sha = self._expand_name(name, self._shas_, self._name_sha_)
        if sha in self.queries:
            ...
        elif self.db is None:
            raise KeyError(name)
        elif (query := self.db.get(Query, sha)) and query is not None:
            self.queries[sha] = query
        else:
            raise KeyError(name)
        return self.queries[sha]

    def get_kids(self, parent_sha: str | None, shas: set[str]) -> list[Query]:
        kids: list[Query] = []
        for sha in shas:
            query = self.get(sha)
            if query.require == parent_sha:
                kids.append(query)
        return kids

    def get_all_kids(self, sha: str | None, shas: set[str]) -> list[Query]:
        shas = set(shas)
        kids = self.get_kids(sha, shas)
        shas = shas - set([query.sha for query in kids])
        for kid in kids:
            query_kids = self.get_all_kids(kid.sha, shas)
            kids.extend(query_kids)
            shas = shas - set([query_kid.sha for query_kid in query_kids])
        return kids

    def get_parent(self, query: Query) -> Query | None:
        if query.require is not None:
            return self.get(query.require)
        else:
            return None

    def get_parents(self, query: Query) -> list[Query]:
        result: list[Query] = []
        kid = self.get_parent(query)
        while kid is not None:
            result.append(kid)
            kid = self.get_parent(kid)
        return result

    def with_tag(self, tag_name: str) -> list[Query]:
        queries: list[Query] = []
        shas: set[str] = set()
        if self.db is not None:
            db_queries = self.db.scalars(sql.query.with_tag(tag_name))
            for query in db_queries:
                queries.append(query)
                shas.add(query.sha)
        for query in self.queries.values():
            if query.sha in shas:
                continue
            tag_names = [tag.name for tag in query.tags]
            if tag_name in tag_names:
                queries.append(query)
        return queries

    def subgraph(
        self,
        *queries: Query,
        kids: bool = True,
        parents: bool = False,
    ) -> Graph:
        if not queries:
            raise ValueError("Cannot subgraph from nothing")
        graph = Graph(None, *queries, force=True)
        shas = set(self._shas_)
        if kids:
            for query in queries:
                shas = shas - graph._shas_
                graph.add(*self.get_all_kids(query.sha, shas), force=True)
        if parents:
            for query in queries:
                shas = shas - graph._shas_
                graph.add(*self.get_parents(query), force=True)
        return graph

    def _load_db_shas(self, full: bool = False) -> None:
        if self.db is not None:
            name_sha: dict[str, str] = {}
            self._shas_ = set(self.db.scalars(sql.query.shas))
            for name, sha in self.db.rows(sql.query.name_sha):
                name_sha[name] = sha
            self._name_sha_ = name_sha

    def load_db(self) -> None:
        if self.db is not None:
            queries = self.db.scalars(sql.query.all)
            self.add(*queries, clone=False, force=True)

    def validate(self, *queries: Query, noraise: bool = False) -> set[str]:
        names = set(self._name_sha_.keys())
        duplicates = {q.name: q for q in queries if q.name in names}
        if duplicates and not noraise:
            raise QueryDuplicate(pretty_repr(duplicates))
        else:
            return set(duplicates.keys())

    def resolve_require(self, query: Query) -> None:
        if query.require is None:
            ...
        elif self.has(sha=query.require):
            ...
        elif self.has(name=query.require):
            parent = self.get(query.require)
            query.require = parent.sha
            query.compute_sha()

    def add(
        self,
        *queries: Query,
        force: bool = False,
        clone: bool = True,
        noraise: bool = False,
    ) -> dict[str, Query]:
        """
        Add new query to the graph.

        - (re)compute sha for each query
        - validate query.name against existing queries
        - populate graph._name_sha_ to enable `graph[query.name]` indexing
        - replace query.require with full sha
        """
        new_shas: set[str] = set(self._shas_)
        new_queries: dict[str, Query] = dict(self.queries.items())
        name_shas: dict[str, list[str]] = {
            name: [sha] for name, sha in self._name_sha_.items()
        }
        queue: list[Query] = [
            query.clone(compute_sha=True) if clone else query
            for query in queries
        ]
        # duplicate_names = self.validate(*queue, noraise=noraise or force)
        replaced: dict[str, Query] = {}
        for query in queue:
            if query.sha in new_shas:
                if force:
                    if query.sha in new_queries:
                        old = new_queries[query.sha]
                    else:
                        old = self.get(query.sha)
                    replaced[query.sha] = old.clone(compute_sha=False)  # True?
                else:
                    raise QueryDuplicate(pretty_repr(query))
            new_shas.add(query.sha)
            new_queries[query.sha] = query
        skip_tags: set[str] = set()
        for sha, query in self.queries.items():
            tag_name = query.tag_name
            if tag_name is not None and tag_name not in skip_tags:
                name_shas.setdefault(tag_name, [])
                name_shas[tag_name].append(query.sha)
                if len(name_shas[tag_name]) > 1:
                    skip_tags.add(tag_name)
        new_name_sha = {
            name: shas[0]
            for name, shas in name_shas.items()
            if name not in skip_tags
        }
        if not force:
            for sha, query in new_queries.items():
                if query.require is not None:
                    sha = self._expand_name(
                        query.require, new_shas, new_name_sha
                    )
                    if sha != query.require:
                        raise ValueError("case change require")
        self.queries = new_queries
        self._shas_ = new_shas
        self._name_sha_ = new_name_sha
        return replaced

    def get_unknown_facets(self) -> set[Facet]:
        if self.db is None:
            raise
        facets: dict[str, Facet] = {}
        for query in self.queries.values():
            for facet in query.selection._facets:
                if facet.sha not in facets:
                    facets[facet.sha] = facet
        shas = list(facets.keys())
        known_shas = self.db.scalars(sql.facet.known_shas(shas))
        unknown_shas = set(shas) - set(known_shas)
        unknown_facets = set([facets[sha] for sha in unknown_shas])
        return unknown_facets

    def _merge(self) -> dict[str, Query]:
        """
        Try to load instances from database into self.db.

        Start with tags, since they are not part of query.sha,
        and there could be new tags to add to an existing query.
        Those new tags need to be merged before adding them to an
        existing query instance from database (autoflush mess).

        Only load options/selection/facets if query is not in db,
        and updated options/selection/facets should change sha value.
        """
        if self.db is None:
            raise
        new_shas: list[str] = []
        for sha, query in self.queries.items():
            is_new = False
            for i, tag in enumerate(query.tags):
                if tag.state.persistent:
                    ...
                elif tag_db := self.db.get(Tag, tag.sha):
                    query.tags[i] = tag_db
                else:
                    is_new = True
                    query.tags[i] = self.db.session.merge(tag)
            if query.state.persistent and not is_new:
                # can skip options/selection/facets since tags are updated
                # self.db.session.flush()  # needed? dont think (autoflush=True)
                continue
            elif query_db := self.db.get(Query, query.sha):
                # save tags since they may have been updated
                new_tags = query.tags[:]
                query = query_db
                if set(query.tags) != set(new_tags):
                    query.tags = new_tags
                self.queries[sha] = query
            else:
                is_new = True
            if query.options.state.persistent:
                ...
            elif options_db := self.db.get(Options, query.options.sha):
                query.options = options_db
            else:
                is_new = True
                query.options = self.db.session.merge(query.options)
            if query.selection.state.persistent:
                ...
            elif selection_db := self.db.get(Selection, query.selection.sha):
                query.selection = selection_db
            else:
                for i, facet in enumerate(query.selection._facets):
                    if facet.state.persistent:
                        ...
                    elif facet_db := self.db.get(Facet, facet.sha):
                        query.selection._facets[i] = facet_db
                    else:
                        raise UnknownFacet(facet)
                is_new = True
                query.selection = self.db.session.merge(query.selection)
            self.queries[sha] = self.db.session.merge(query)
            # self.db.session.flush()
            if is_new:
                new_shas.append(sha)
        return {sha: self.queries[sha] for sha in new_shas}

    def commit(self) -> dict[str, Query]:
        """
        Merge with existing db instances, otherwise insert into new rows.
        """
        if self.db is None:
            raise
        new_queries = self._merge()
        self.db.add(*self.queries.values())
        return new_queries

    def expand(self, name: str) -> Query:
        """
        Expand/unpack `query.requires`, using `query.name` index.
        """
        query = self.get(name)
        while query.require is not None:
            query = self.get(query.require) << query
        return query

    def dump(self) -> list[QueryDict]:
        """
        Dump full graph as list of dicts (yaml selection syntax).
        """
        return [q.asdict() for q in self.queries.values()]

    def fill_tree(self, root: Query | None, tree: Tree) -> None:
        """
        Recursive method to add branches starting from queries with either:
            - require is None
            - require is not in self.queries
        """
        for sha, query in self.queries.items():
            if sha in self._rendered:
                ...
            elif root is None:
                if query.require is None or not self.has(sha=query.require):
                    self._rendered.add(sha)
                    self.fill_tree(query, tree.add(query))
            elif query.require == root.sha:
                self._rendered.add(sha)
                self.fill_tree(query, tree.add(query.no_require()))

    __rich_measure__ = rich_measure_impl

    def __rich_console__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> Iterator[Tree]:
        """
        Returns a `rich.tree.Tree` representing queries and their `require`.
        """
        tree = Tree("", hide_root=True, guide_style="dim")
        self._rendered = set()
        self.fill_tree(None, tree)
        del self._rendered
        yield tree


#     def __rich_measure__(
#         self,
#         console: Console,
#         options: ConsoleOptions,
#     ) -> Measurement:
#         renderables = list(self.__rich_console__(console, options))
#         return measure_renderables(console, options, renderables)
