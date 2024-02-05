from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass

from rich.console import Console, ConsoleOptions
from rich.pretty import pretty_repr
from rich.tree import Tree

from esgpull.config import Config
from esgpull.database import Database
from esgpull.exceptions import (
    GraphWithoutDatabase,
    QueryDuplicate,
    TooShortKeyError,
)
from esgpull.models import Facet, Query, QueryDict, Tag, sql
from esgpull.models.utils import rich_measure_impl


@dataclass(init=False, repr=False)
class Graph:
    queries: dict[str, Query]
    _db: Database | None
    _shas: set[str]
    _name_sha: dict[str, str]
    _rendered: set[str]
    _deleted_shas: set[str]

    @classmethod
    def from_config(cls, config: Config) -> Graph:
        db = Database.from_config(config)
        return Graph(db)

    def __init__(self, db: Database | None) -> None:
        self._db = db
        self.queries = {}
        self._shas = set()
        self._name_sha = {}
        self._deleted_shas = set()
        if db is not None:
            self._load_db_shas()

    @property
    def db(self) -> Database:
        if self._db is None:
            raise GraphWithoutDatabase()
        else:
            return self._db

    @staticmethod
    def matching_shas(name: str, shas: set[str]) -> list[str]:
        shas_copy = list(shas)
        for pos, c in enumerate(name):
            idx = 0
            while idx < len(shas_copy):
                if shas_copy[idx][pos] != c:
                    shas_copy.pop(idx)
                else:
                    idx += 1
        return shas_copy

    @staticmethod
    def _expand_name(
        name: str, shas: set[str], name_sha: dict[str, str]
    ) -> str:
        if name in name_sha:
            sha = name_sha[name]
        elif name in shas:
            sha = name
        else:
            short_name = name
            if short_name.startswith("#"):
                short_name = short_name[1:]
            matching_shas = Graph.matching_shas(short_name, shas)
            if len(matching_shas) > 1:
                raise TooShortKeyError(name)
            elif len(matching_shas) == 1:
                sha = matching_shas[0]
            else:
                sha = name
        return sha

    def __contains__(self, item: Query | str) -> bool:
        match item:
            case Query():
                item.compute_sha()
                sha = item.sha
            case str():
                sha = self._expand_name(item, self._shas, self._name_sha)
            case _:
                raise TypeError(item)
        return sha in self._shas

    def get(self, name: str) -> Query:
        sha = self._expand_name(name, self._shas, self._name_sha)
        if sha in self.queries:
            ...
        elif sha in self._shas:
            query_db = self.db.get(Query, sha)
            if query_db is not None:
                self.queries[sha] = query_db
            else:
                raise
        else:
            raise KeyError(name)
        return self.queries[sha]

    def get_mutable(self, name: str) -> Query:
        sha = self._expand_name(name, self._shas, self._name_sha)
        if sha in self._shas:
            query_db = self.db.get(
                Query,
                sha,
                lazy=False,
                detached=True,
            )
            if query_db is not None:
                self.queries[sha] = query_db
            else:
                raise
        else:
            raise KeyError(name)
        return self.queries[sha]

    def get_children(self, sha: str) -> Sequence[Query]:
        if self._db is None:
            children: list[Query] = []
            for query in self.queries.values():
                if query.require == sha:
                    children.append(query)
        elif sha is None:
            return []
        else:
            return self.db.scalars(sql.query.children(sha))
        return children

    def get_all_children(self, sha: str) -> Sequence[Query]:
        children: list[Query] = []
        for query in self.get_children(sha):
            children.append(query)
            children.extend(self.get_all_children(query.sha))
        return children

    def get_parent(self, query: Query) -> Query | None:
        if query.require is not None:
            return self.get(query.require)
        else:
            return None

    def get_parents(self, query: Query) -> list[Query]:
        result: list[Query] = []
        parent = self.get_parent(query)
        while parent is not None:
            result.append(parent)
            parent = self.get_parent(parent)
        return result

    def get_tags(self) -> list[Tag]:
        return list(self.db.scalars(sql.tag.all()))

    def get_tag(self, name: str) -> Tag | None:
        result: Tag | None = None
        for tag in self.get_tags():
            if tag.name == name:
                result = tag
                break
        return result

    def with_tag(self, tag_name: str) -> list[Query]:
        queries: list[Query] = []
        shas: set[str] = set()
        try:
            db_queries = self.db.scalars(sql.query.with_tag(tag_name))
            for query in db_queries:
                queries.append(query)
                shas.add(query.sha)
        except GraphWithoutDatabase:
            pass
        for sha in self._shas - shas:
            query = self.get(sha)
            if query.get_tag(tag_name) is not None:
                queries.append(query)
        return queries

    def subgraph(
        self,
        *queries: Query,
        children: bool = True,
        parents: bool = False,
        keep_db: bool = False,
    ) -> Graph:
        if not queries:
            raise ValueError("Cannot subgraph from nothing")
        if keep_db:
            graph = Graph(self.db)
            queries_shas = [q.sha for q in queries]
            graph.load_db(*queries_shas)
        else:
            graph = Graph(None)
            graph.add(*queries, force=True, clone=False)
        if children:
            for query in queries:
                query_children = self.get_all_children(query.sha)
                if len(query_children) == 0:
                    continue
                if keep_db:
                    children_shas = [q.sha for q in query_children]
                    graph.load_db(*children_shas)
                else:
                    graph.add(*query_children, force=True, clone=False)
        if parents:
            for query in queries:
                query_parents = self.get_parents(query)
                if len(query_parents) == 0:
                    continue
                if keep_db:
                    parents_shas = [q.sha for q in query_parents]
                    graph.load_db(*parents_shas)
                else:
                    graph.add(*query_parents, force=True, clone=False)
        return graph

    def _load_db_shas(self, full: bool = False) -> None:
        name_sha: dict[str, str] = {}
        self._shas = set(self.db.scalars(sql.query.shas()))
        for name, sha in self.db.rows(sql.query.name_sha()):
            name_sha[name] = sha
        self._name_sha = name_sha

    def load_db(self, *shas: str) -> None:
        if shas:
            unloaded_shas = set(shas)
        else:
            unloaded_shas = set(self._shas) - set(self.queries.keys())
        if unloaded_shas:
            queries = self.db.scalars(sql.query.with_shas(*unloaded_shas))
            for query in queries:
                self.queries[query.sha] = query

    def validate(self, *queries: Query, noraise: bool = False) -> set[str]:
        names = set(self._name_sha.keys())
        duplicates = {q.name: q for q in queries if q.name in names}
        if duplicates and not noraise:
            raise QueryDuplicate(pretty_repr(duplicates))
        else:
            return set(duplicates.keys())

    def resolve_require(self, query: Query) -> None:
        if query.require is None or query.require in self._shas:
            ...
        elif query.require in self:  # self.has(sha=query.require):
            parent = self.get(query.require)
            query.require = parent.sha
            query.compute_sha()
        else:
            query._unknown_require = True  # type: ignore [attr-defined]

    def add(
        self,
        *queries: Query,
        force: bool = False,
        clone: bool = True,
        noraise: bool = False,
    ) -> Mapping[str, Query]:
        """
        Add new query to the graph.

        - (re)compute sha for each query
        - validate query.name against existing queries
        - populate graph._name_sha to enable `graph[query.name]` indexing
        - replace query.require with full sha
        """
        new_shas: set[str] = set(self._shas)
        new_deleted_shas: set[str] = set(self._deleted_shas)
        new_queries: dict[str, Query] = dict(self.queries.items())
        name_shas: dict[str, list[str]] = {
            name: [sha] for name, sha in self._name_sha.items()
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
            if query.sha in new_deleted_shas:
                new_deleted_shas.remove(query.sha)
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
        self._shas = new_shas
        self._deleted_shas = new_deleted_shas
        self._name_sha = new_name_sha
        return replaced

    def get_unknown_facets(self) -> set[Facet]:
        """
        Why was this implemented?
        Maybe useful to enable adding facets (e.g. `table_id:*day*`)
        """
        facets: dict[str, Facet] = {}
        for query in self.queries.values():
            for facet in query.selection._facets:
                if facet.sha not in facets:
                    facets[facet.sha] = facet
        shas = list(facets.keys())
        known_shas = self.db.scalars(sql.facet.known_shas(shas))
        unknown_shas = set(shas) - set(known_shas)
        unknown_facets = {facets[sha] for sha in unknown_shas}
        return unknown_facets

    def merge(self) -> Mapping[str, Query]:
        """
        Try to load instances from database into self.db.

        Start with tags, since they are not part of query.sha,
        and there could be new tags to add to an existing query.
        Those new tags need to be merged before adding them to an
        existing query instance from database (autoflush mess).

        Only load options/selection/facets if query is not in db,
        and updated options/selection/facets should change sha value.
        """
        updated_shas: set[str] = set()
        for sha, query in self.queries.items():
            query_db = self.db.merge(query, commit=True)
            if query is query_db:
                ...
            else:
                updated_shas.add(sha)
                self.queries[sha] = query_db
        for sha in self._deleted_shas:
            query_to_delete = self.db.get(Query, sha)
            if query_to_delete is not None:
                self.db.delete(query_to_delete)
        return {sha: self.queries[sha] for sha in updated_shas}

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

    def asdict(self, files: bool = False) -> Mapping[str, QueryDict]:
        """
        Dump full graph as dict of dict, indexed by each query's sha.
        """
        result = {}
        for sha, query in self.queries.items():
            result[sha] = query.asdict()
            if files:
                result[sha]["files"] = [f.asdict() for f in query.files]
        return result

    def fill_tree(
        self,
        root: Query | None,
        tree: Tree,
        keep_require: bool = False,
    ) -> None:
        """
        Recursive method to add branches starting from queries with either:
            - require is None
            - require is not in self.queries
        """
        for sha, query in self.queries.items():
            query_tree: Tree | None = None
            if sha in self._rendered:
                ...
            elif root is None:
                if (
                    query.require is None or query.require not in self
                ):  # self.has(sha=query.require):
                    self._rendered.add(sha)
                    query_tree = query._rich_tree()
            elif query.require == root.sha:
                self._rendered.add(sha)
                if keep_require:
                    query_tree = query._rich_tree()
                else:
                    query_tree = query.no_require()._rich_tree()
            if query_tree is not None:
                tree.add(query_tree)
                self.fill_tree(query, query_tree)

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
        unrendered = set(self.queries.keys()) - self._rendered
        for sha in unrendered:
            query = self.get(sha)
            if query.require is None:
                continue
            parent = self.get(query.require)
            self.fill_tree(parent, tree, keep_require=True)
        del self._rendered
        yield tree

    def delete(self, query: Query) -> None:
        self._shas.remove(query.sha)
        self.queries.pop(query.sha, None)
        self._deleted_shas.add(query.sha)

    def replace(self, original: Query, new: Query) -> None:
        # if original not in self.db:
        #     raise ValueError(f"{original.name} not found in the database.")
        # elif new in self.db:
        #     raise ValueError(f"{new.name} already in the database.")
        self.delete(original)
        self.add(new)
