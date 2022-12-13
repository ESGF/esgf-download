from collections import Counter
from dataclasses import dataclass, field
from typing import Iterator

import sqlalchemy as sa
from rich.console import Console, ConsoleOptions
from rich.measure import Measurement, measure_renderables
from rich.pretty import pretty_repr
from rich.tree import Tree
from sqlalchemy.orm import Session

from esgpull.exceptions import QueryDuplicate
from esgpull.models import Query, QueryDict, Options, Select, Tag, Facet


@dataclass(init=False)
class Graph:
    session: Session | None
    queries: list[Query]
    _query_map_: dict[str, int] = field(repr=False)
    _rendered: set[int] = field(repr=False)

    def __init__(
        self,
        session: Session | None,
        *queries: Query,
        force: bool = False,
        noraise: bool = False,
    ) -> None:
        self.session = session
        self._query_map_ = {}
        self.queries = []
        self.load_db()
        self.add(*queries, force=force, noraise=noraise)

    def load_db(self) -> None:
        if self.session is not None:
            queries = self.session.scalars(sa.select(Query)).all()
            self.add(*queries, clone=False, noraise=True)

    def validate(self, *queries: Query, noraise: bool = False) -> set[str]:
        names = Counter(q.name for q in list(queries) + self.queries)
        counter = sum(c > 1 for c in names.values())
        if counter == 0:
            return set()
        duplicates = dict(names.most_common(counter))
        if noraise:
            return set(duplicates.keys())
        duplicate_dict: dict[str, list[Query]] = {
            name: [] for name in duplicates
        }
        for query in queries:
            if sum(duplicates.values()) == 0:
                break
            if query.name in duplicates:
                duplicate_dict[query.name].append(query)
                duplicates[query.name] -= 1
        raise QueryDuplicate(pretty_repr(duplicate_dict))

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
        - populate graph._query_map_ to enable `graph[query.name]` indexing
        - replace query.require with sha if using known name (or raise)
        """
        new_queries: list[Query] = list(self.queries)
        new_query_map: dict[str, int] = dict(self._query_map_.items())
        queue: list[Query] = [
            query.clone(compute_sha=True) if clone else query
            for query in queries
        ]
        duplicate_names = self.validate(*queue, noraise=noraise or force)
        duplicate_name_sha_map: dict[str, str] = {}
        replaced: dict[str, Query] = {}
        queries_with_require: list[Query] = []
        for i, query in enumerate(queue):
            query_idx = new_query_map.get(query.name)
            if query.name in duplicate_names:
                # set _sha_name after trying to fetch the query to replace in
                # case a tag change on the new query causes a name collision
                # with another existing query.
                old_name = query.name
                query._sha_name = True
                duplicate_name_sha_map[old_name] = query.name
                if query_idx is None:
                    query_idx = new_query_map.get(query.name)
            if query_idx is None:
                query_idx = new_query_map.get(query.sha)
            if query_idx is not None:
                if force:
                    og = new_queries[query_idx]
                    new_queries[query_idx] = query
                    new_query_map[query.sha] = new_query_map.pop(og.sha)
                    new_query_map[query.name] = new_query_map.pop(og.name)
                    replaced[query.name] = og.clone(compute_sha=False)  # True?
                else:
                    raise QueryDuplicate(pretty_repr(query))
            else:
                new_query_map[query.name] = len(new_queries)
                new_query_map[query.sha] = len(new_queries)
                new_queries.append(query)
            if query.require is not None:
                queries_with_require.append(query)
        for query in queries_with_require:
            if query.require in duplicate_names:
                query.require = duplicate_name_sha_map[query.require]
            elif query.require not in new_query_map:
                raise ValueError
        self.queries = new_queries
        self._query_map_ = new_query_map
        return replaced

    def merge(self) -> list[str]:
        """
        Try to load instances from database into self.session.

        Start with tags, since they are not part of query.sha,
        and there could be new tags to add to an existing query.
        Those new tags need to be merged before adding them to an
        existing query instance from database (autoflush mess).

        Only load options/select/facets if query is not in session,
        and updated options/select/facets should change sha value.
        """
        if self.session is None:
            raise
        new_queries: list[str] = []
        for i, query in enumerate(self.queries):
            is_new = False
            for j, tag in enumerate(query.tags):
                if tag.state.persistent:
                    ...
                elif tag_session := self.session.get(Tag, tag.sha):
                    query.tags[j] = tag_session
                else:
                    is_new = True
                    query.tags[j] = self.session.merge(tag)
            if query.state.persistent and not is_new:
                # can skip options/select/facets since tags are updated
                # self.session.flush()  # needed? dont think (autoflush=True)
                continue
            elif query_session := self.session.get(Query, query.sha):
                # save tags since they may have been updated
                new_tags = query.tags[:]
                query = query_session
                if set(query.tags) != set(new_tags):
                    query.tags = new_tags
                self.queries[i] = query
            else:
                is_new = True
            if query.options.state.persistent:
                ...
            elif options_session := self.session.get(
                Options, query.options.sha
            ):
                query.options = options_session
            else:
                is_new = True
                query.options = self.session.merge(query.options)
            if query.select.state.persistent:
                ...
            elif select_session := self.session.get(Select, query.select.sha):
                query.select = select_session
            else:
                for j, facet in enumerate(query.select.facets):
                    if facet.state.persistent:
                        ...
                    elif facet_session := self.session.get(Facet, facet.sha):
                        query.select.facets[j] = facet_session
                    else:
                        raise ValueError(f"unknown facet: {facet}")
                is_new = True
                query.select = self.session.merge(query.select)
            self.queries[i] = self.session.merge(query)
            # self.session.flush()
            if is_new:
                new_queries.append(self.queries[i].name)
        return new_queries

    def save_new_records(self) -> list[str]:
        """
        Merge with existing db instances, otherwise insert into new rows.
        """
        if self.session is None:
            raise
        new_queries = self.merge()
        self.session.add_all(self.queries)
        self.session.commit()
        return new_queries

    def __getitem__(self, name: str) -> Query:
        return self.queries[self._query_map_[name]]

    def expand(self, name: str) -> Query:
        """
        Expand/unpack `query.requires`, using `query.name` index.
        """
        query = self[name]
        while query.require is not None:
            query = self[query.require] << query
        return query

    def dump(self) -> list[QueryDict]:
        """
        Dump full graph as list of dicts (yaml selection syntax).
        """
        return [q.asdict() for q in self.queries]

    def fill_tree_from(self, root: Query | None, tree: Tree) -> None:
        """
        Recursive method to add branches starting from `root`.
        """
        for i, query in enumerate(self.queries):
            if i in self._rendered:
                continue
            if root is None:
                if query.require is None:
                    self._rendered.add(i)
                    self.fill_tree_from(
                        query, tree.add(query.no_require())
                    )  # TODO: rm no_require?
            else:
                if query.require in [root.name, root.sha]:
                    self._rendered.add(i)
                    self.fill_tree_from(query, tree.add(query.no_require()))

    def get_tree_from(self, name: str | None) -> Tree:
        """
        Returns a `rich.tree.Tree` representing queries and their `require`.
        """
        tree = Tree("", hide_root=True, guide_style="dim")
        self._rendered = set()
        if name is not None:
            root_idx = self._query_map_[name]
            self._rendered.add(root_idx)
            root = self.queries[root_idx]
            tree = tree.add(root)
            self.fill_tree_from(root, tree)
        else:
            self.fill_tree_from(None, tree)
        del self._rendered
        return tree

    def __rich_console__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> Iterator[Tree]:
        yield self.get_tree_from(None)

    def __rich_measure__(
        self,
        console: Console,
        options: ConsoleOptions,
    ) -> Measurement:
        renderables = list(self.__rich_console__(console, options))
        return measure_renderables(console, options, renderables)
