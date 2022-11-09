from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, TypeVar

import yaml

from esgpull.constants import DEFAULT_FACETS, EXTRA_FACETS
from esgpull.exceptions import FacetNameError
from esgpull.facet import (
    Facet,
    FacetDict,
    FacetValues,
    NestedFacetDict,
    is_nested_facet_dict,
    split_nested_facet_dict,
)

DEFAULT_FACET_VALUE: FacetValues = "*"


# Workaround for python < 3.11, required by mypy for `clone` method
# https://peps.python.org/pep-0673/
Self = TypeVar("Self", bound="QueryBase")


@dataclass(repr=False)
class QueryBase:
    """
    Base class for query.
    Defines classmethods to restrict facets to a controlled vocabulary.
    Defines getitem/setitem, and other methods common to query classes.
    """

    _initialized: bool = False
    _facets: dict[str, Facet] = field(default_factory=dict)

    @classmethod
    def add_facet(cls, name: str, default: FacetValues) -> None:
        def getter(self) -> Facet:
            self._facets.setdefault(name, Facet(name, default))
            return self._facets[name]

        def setter(self, values: FacetValues) -> None:
            getter(self)._set(values)

        setattr(cls, name, property(getter, setter))

    @classmethod
    def configure(cls, facets=DEFAULT_FACETS, extra=EXTRA_FACETS) -> None:
        cls.reset_facets()
        for name in facets + extra:
            cls.add_facet(name, DEFAULT_FACET_VALUE)

    @classmethod
    def reset_facets(cls) -> None:
        properties = [
            name
            for name, prop in vars(cls).items()
            if isinstance(prop, property)
        ]
        for name in properties:
            delattr(cls, name)

    def __post_init__(
        self,
    ) -> None:
        for prop in vars(self.__class__).values():
            if isinstance(prop, property):
                prop.__get__(self)  # Fill _facets with Facet instances
        self._initialized = True

    def __getitem__(self, name: str) -> Facet:
        """
        Getter that allows only defined methods or facets, raise otherwise.
        Enables dict-like `[]` behaviour.

        Example:
            ```python
            query = Query()
            print(query["variable_id"])
            # *
            ```

        Example:
            ```python
            query = Query()
            print(query.variable_id)
            # *
            ```
        """
        if name not in self._facets:
            raise FacetNameError(name)
        else:
            return object.__getattribute__(self, name)

    def __setitem__(self, name: str, values: FacetValues) -> None:
        """
        Setter that allows only defined facets, raise otherwise.
        Validates `name` and `values` when `validations` is True.
        Enables dict-like `[]=` behaviour.

        Example:
            ```python
            query = Query()
            query["project"] = "CMIP5"
            query.variable = "ta"
            print(query)
            # Query(project={CMIP5}, variable={ta})
            ```

        Example:
            ```python
            from esgpull import Context
            ctx = Context()
            ctx.query.project = "CMIP5"
            ctx.update()

            ctx.query["mip_era"] = "CMIP6" # TODO: implement value checks
            # ImpossibleFacet: mip_era cannot be set with the current query:
            #     Query(project: CMIP5)

            ctx.query.not_facet = "value"
            # FacetNameError: 'not_facet' is not a valid facet.
            ```
        """
        if name in self.__dict__:
            object.__setattr__(self, name, values)
        elif name not in self._facets:
            raise FacetNameError(name)
        else:
            # validation here
            object.__setattr__(self, name, values)

    def __getattr__(self, name: str) -> Facet:
        """See `help(__getitem__).`"""
        if self._initialized:
            return self[name]
        else:
            raise TypeError  # TODO: figure out whether this line is useful
            # return object.__getattribute__(self, name)

    def __setattr__(self, name: str, values: FacetValues) -> None:
        """See `help(__setitem__).`"""
        if self._initialized:
            self[name] = values
        else:
            object.__setattr__(self, name, values)

    def __iter__(self) -> Iterator[Facet]:
        """
        Iterate over non-default facets, yielding instances of `Facet`.

        Example:
            ```python
            query = Query()
            query.project = "CMIP6"
            for facet in query:
                print(facet)
            # project={CMIP6}
            ```
        """
        for facet in self._facets.values():
            if not facet.isdefault():
                yield facet

    def __len__(self) -> int:
        return len(list(iter(self)))

    def update(self: QueryBase, other: QueryBase, force_append=False) -> None:
        raise NotImplementedError

    def dump(self):
        raise NotImplementedError

    def load(self, source):
        raise NotImplementedError

    def clone(self: Self) -> Self:
        result = self.__class__()
        dump = self.__class__.dump(self)
        result.load(dump)
        return result

    def __add__(self: Self, other: QueryBase) -> Self:
        result = self.clone()
        result.update(other, force_append=True)
        return result


@dataclass(repr=False)
class SimpleQuery(QueryBase):
    """
    Interface to get/set facet values for a flat/single query.

    Each facet is defined dynamically as a property
    to restrict usage to the controlled vocabulary.

    [--]TODO: recode validations? -> in `Context` directly
        Maybe using pydantic models dynloaded from db?
    """

    def update(self: QueryBase, other: QueryBase, force_append=False) -> None:
        for facet in other:
            if force_append or facet.appended:
                self[facet.name] + facet.values
            else:
                self[facet.name] = facet.values

    def dump(self: QueryBase) -> FacetDict:
        """
        Returns facets as a dict.

        Example:
            ```python
            query = SimpleQuery()
            query.project = "CMIP6"
            print(query.dump())
            # {"project": "CMIP6"}
            ```
        """
        result: FacetDict = {}
        for facet in iter(self):
            result.update(facet.dump())
        return result

    def load(self: QueryBase, source: FacetDict) -> None:
        """
        Load facets.
        """
        for name, values in source.items():
            if name.startswith("+"):
                self[name.removeprefix("+")] + values
            else:
                self[name] = values

    def __repr__(self) -> str:
        facets = ", ".join(map(str, self))
        return f"SimpleQuery({facets})"

    def flatten(self) -> list[SimpleQuery]:
        return [self]


@dataclass(repr=False)
class Query(QueryBase):
    """
    Extension of FlatQuery, enabling parallel/split queries.
    """

    requests: list[SimpleQuery] = field(default_factory=list)

    @classmethod
    def from_dict(cls, source: NestedFacetDict) -> Query:
        instance = cls()
        instance.load(source)
        return instance

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        instance: Query | None = None,
    ) -> Query:
        with open(path) as f:
            source = yaml.load(f.read(), Loader=yaml.loader.BaseLoader)
        if instance is None:
            instance = cls()
        instance.load(source)
        return instance

    def tosimple(self) -> SimpleQuery:
        result = SimpleQuery()
        result.load(SimpleQuery.dump(self))
        return result

    def update(self, other: QueryBase, force_append=False) -> None:
        if isinstance(other, Query):
            self.requests += other.requests
        SimpleQuery.update(self, other, force_append)

    def dump(self) -> NestedFacetDict:
        """
        Returns a dict of facets with format:
            ```
            {<facets>, "requests": [{<facets>}, ...]}
            ```

        This is used mainly for storing query as json/yaml.

        Example:
            ```python
            query = Query()
            query.project = "CMIP6"
            query.variable = "first"
            with query:
                query.variable = "second"
            with query:
                query.variable + "third"
            print(query.dump())
            # {'project': 'CMIP6',
            #  'variable': 'first',
            #  'requests': [{'variable': 'second'},
            #               {'+variable': 'third'}]}
            ```
        """
        result: NestedFacetDict = {}
        dump = self.tosimple().dump()
        requests = [r.dump() for r in self.requests]
        if is_nested_facet_dict(dump):
            result = dump
        if requests:
            result["requests"] = requests
        return result

    def load(self, source: NestedFacetDict) -> None:
        simple, requests = split_nested_facet_dict(source)
        SimpleQuery.load(self, simple)
        for request in requests:
            query = SimpleQuery()
            query.load(request)
            self.requests.append(query)

    def load_file(self, path: str | Path) -> None:
        Query.from_file(path, instance=self)

    def __repr__(self) -> str:
        facets = ", ".join(map(str, self))
        requests = ", ".join(map(str, self.requests))
        return f"Query({facets}, requests=[{requests}])"

    def add(self, source: QueryBase | None = None) -> SimpleQuery:
        query: SimpleQuery
        match source:
            case Query():
                query = source.tosimple()
            case SimpleQuery():
                query = source
            case None:
                query = SimpleQuery()
        self.requests.append(query)
        return query

    def flatten(self) -> list[SimpleQuery]:
        result = []
        for request in self.requests:
            clone = self.tosimple()
            clone.update(request)
            for facet in clone:
                facet.appended = False
            result.append(clone)
        if not result:
            clone = self.tosimple()
            for facet in clone:
                facet.appended = False
            result.append(clone)
        return result


# def reduce(self) -> None:
#     """
#     Factorize (-> `use`) values that are present in more than half of the
#     "flat" requests and for which the facet is present in all of them.

#     The threshold at half of the requests is a heuristic for a short syntax
#     but it could certainly be improved. It still sometimes produces
#     non-optimal results dumps (e.g. `time_frequency` in example).
#     Use-facets must be set for all requests as it's not possible to remove
#     a facet value if it is set in `use`.
#     Of course, facets could be reset to the default value, but this kind of
#     hack would consequently require additional keywords (`-<facet>`?) for
#     the yaml/json syntax and clutter the readability/interpretability.

#     [--]TODO: Optimize on reducing number of +facets:
#         ```python
#         {'requests': [{'time_frequency': 'mon', 'variable': 'tasmin'},
#                       {'variable': 'tas,ua'},
#                       {'+time_frequency': 'fx,mon', 'variable': 'tasmax'}],
#          'time_frequency': 'day'}
#         {'requests': [{'time_frequency': 'mon', 'variable': 'tasmin'},
#                       {'time_frequency': 'day', 'variable': 'tas,ua'},
#                       {'+time_frequency': 'fx', 'variable': 'tasmax'}],
#          'time_frequency': 'day,mon'}
#         ```

#     Example:
#         ```python
#         query = Query()
#         with query:
#             query.institution = "IPSL"
#             query.realm = "atmos"
#             query.experiment = ["rcp26", "historical"]
#             query.time_frequency = "mon"
#             query.variable = "tasmin"
#         with query:
#             query.institution = "IPSL"
#             query.realm = "atmos"
#             query.experiment = ["rcp85", "historical"]
#             query.time_frequency = "day"
#             query.variable = "tas"
#         with query:
#             query.realm = "atmos"
#             query.experiment = "historical"
#             query.time_frequency = ["fx", "day", "mon"]
#             query.variable = "tasmax"
#         query.reduce()
#         print(query.dump())
#         # {'experiment': 'historical',
#         #  'realm': 'atmos',
#         #  'requests': [{'+experiment': 'rcp26',
#         #                'institution': 'IPSL',
#         #                'time_frequency': 'mon',
#         #                'variable': 'tasmin'},
#         #               {'+experiment': 'rcp85',
#         #                'institution': 'IPSL',
#         #                'time_frequency': 'day',
#         #                'variable': 'tas'},
#         #               {'+time_frequency': 'fx', 'variable': 'tasmax'}],
#         #  'time_frequency': 'mon,day'}
#         ```
#     """
#     flat = self.flatten()
#     num_requests = len(flat)
#     min_for_use = num_requests // 2
#     facet_counts: dict[str, int] = {}
#     value_counts: dict[str, dict[str, int]] = {}
#     common: dict[str, set[str]] = {}
#     use: FacetDict = {}
#     requests: list[FacetDict] = []

#     # First loop: count facets and values
#     for query in flat:
#         for facet in query:
#             if facet.isdefault():
#                 continue
#             values = set(facet.values)
#             facet_counts.setdefault(facet.name, 0)
#             facet_counts[facet.name] += 1
#             for value in values:
#                 value_counts.setdefault(facet.name, {})
#                 value_counts[facet.name].setdefault(value, 0)
#                 value_counts[facet.name][value] += 1

#     # Second loop: include facet values meeting the requirements
#     for name in facet_counts:
#         common.setdefault(name, set())
#         if facet_counts[name] < num_requests:
#             continue
#         for value, count in value_counts[name].items():
#             if count > min_for_use:
#                 common[name].add(value)

#     # Third loop: create `use` dict, nonempty values concatenated to string
#     for name, values in common.items():
#         if values:
#             use[name] = ",".join(values)

#     # Fourth loop: create `requests` list, infer values to append
#     for query in flat:
#         request: FacetDict = {}
#         for facet in query:
#             contains_common = facet.values.issuperset(common[facet.name])
#             if facet.name in use and contains_common:
#                 # If the set of common values for this facet is a subset
#                 # of the full set of values on this request, we can infer
#                 # that this facet is appended on this requests.
#                 facet.appended = True
#                 # Filter out common values
#                 facet.values -= common[facet.name]
#             if facet.values:
#                 request |= facet.dump()
#         if request:
#             requests.append(request)

#     state = State(use=use, requests=requests)
#     self._setdefault(full=True) # self.reset ?
#     self.load(state)


SimpleQuery.configure()
Query.configure()
