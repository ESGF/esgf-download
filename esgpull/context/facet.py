from __future__ import annotations
from typing import Iterator, NewType

from esgpull.context.constants import (
    DefaultConstraints,
    DefaultConstraintsWithValue,
)

FacetName = NewType("FacetName", str)
FacetValue = NewType("FacetValue", str)


class Facet:
    def __init__(self, *, name: str = None, default: str = "*"):
        if name is not None:
            self.name: FacetName = FacetName(name)
        self.default: FacetValue = FacetValue(default)

    def __set_name__(self, owner, name: str):
        self.name = FacetName(name)

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        match instance.__dict__.get(self.name):
            case str(value):
                return value
            case None:
                instance.__dict__[self.name] = self.default
                return instance.__dict__[self.name]
            case _:
                raise ValueError

    def __set__(self, instance, value: FacetValue):
        match value:
            case str():
                instance.__dict__[self.name] = value
            case _:
                iname = instance.__class__.__name__
                raise TypeError(
                    f"'{iname}.{self.name} = {value}' should be a string."
                )


class Facets:
    """
    Interface to get/set facet values.

    Each facet is defined using a data descriptor
    to restrict usage to a CV (controlled vocabulary).

    New facets can be added as class attributes with:
    `Facet(name=<facet-name>, default=<default-value>)`
    """

    # Add custom facets here
    experiment_id = Facet(default="r1i1p1f1")

    def __init__(self) -> None:
        # Fill `__dict__` by forcing a `getattr` for each facet.
        list(self)

    def __getitem__(self, name: str) -> str:
        """
        Enables dict-like get behaviour.

        Example:
            ```python
            facets = Facets()
            print(facets["variable_id"])
            # *
            ```
        """
        return getattr(self, name)

    def __getattr__(self, name: str) -> str:
        """
        This is effectively useless, but mypy complains without...
        """
        return getattr(Facets, name).__get__(self)

    def __setattr__(self, name: str, value: str):
        """
        Disables the ability to set an attribute that is not a facet.

        Example:
            ```python
            facets = Facets()
            facets.not_facet = "value"
            # AttributeError: type object 'Facets' has no attribute 'not_facet'
            ```
        """
        getattr(Facets, name).__set__(self, value)

    def __setitem__(self, name: str, value: str):
        """
        Expands `__setattr__` to dict-like set.

        Example:
            ```python
            facets = Facets()
            facets["not_facet"] = "value"
            # AttributeError: type object 'Facets' has no attribute 'not_facet'
            ```
        """
        setattr(self, name, value)

    def __iter__(self) -> Iterator[tuple[str, str]]:
        """
        Iterate over all facets, yielding tuples of `(name, value)`

        Example:
            ```python
            facets = Facets()
            for name, value in facets:
                print(name, value)
                break
            # experiment_id *
            ```
        """
        for name, facet in vars(Facets).items():
            if isinstance(facet, Facet):
                yield (name, getattr(self, name))

    @property
    def nondefault(self) -> Constraints:
        return Constraints(self)

    @staticmethod
    def tostring(it: Iterator[tuple[str, str]]) -> str:
        return "[" + ", ".join(f"{name}={facet}" for name, facet in it) + "]"

    def __repr__(self) -> str:
        return self.tostring(iter(self))


class Constraints:
    def __init__(self, facets: Facets):
        self._facets = facets

    def __iter__(self) -> Iterator[tuple[str, str]]:
        for name, facet in self._facets:
            default = getattr(Facets, name).default
            if facet != default:
                yield (name, facet)

    def __repr__(self) -> str:
        return Facets.tostring(iter(self))


for name in DefaultConstraints:
    setattr(Facets, name, Facet(name=name, default="*"))
for name, value in DefaultConstraintsWithValue.items():
    setattr(Facets, name, Facet(name=name, default=value))

__all__ = ["Facet", "Facets"]
