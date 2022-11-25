from __future__ import annotations

from typing import TypeAlias

from attrs import Attribute, define, field, setters

FacetValues: TypeAlias = str | list[str]
FacetDict: TypeAlias = dict[str, FacetValues]
SelectionFileDict: TypeAlias = list[FacetDict]


def tolist(values: str | list[str]) -> list[str]:
    if isinstance(values, str):
        return [values]
    else:
        return values


@define
class Facet:
    values: list[str] = field(
        factory=list,
        converter=tolist,
        on_setattr=setters.convert,
    )
    replace: bool = field(default=True, repr=False)

    @classmethod
    def new(
        cls,
        instance: type,
        attr: Attribute,
        new_value: Facet | FacetValues,
    ) -> Facet:
        if isinstance(new_value, Facet):
            return new_value
        else:
            return cls(new_value)

    def __iadd__(self, values: FacetValues) -> Facet:
        self.values.extend(tolist(values))
        self.replace = False
        return self
