from __future__ import annotations

from typing import TYPE_CHECKING, Iterator

from attrs import define, field, make_class

from esgpull.exceptions import AlreadySetFacet
from esgpull.query.utils import FacetValuesT, hashitems, tuplify


@define
class Facet:
    values: tuple[str, ...] = ()

    @staticmethod
    def new(value: Facet | FacetValuesT) -> Facet:
        if isinstance(value, Facet):
            return value
        else:
            return Facet(tuplify(value))

    def __bool__(self) -> bool:
        return len(self.values) > 0


class SelectBase:
    __slots__: tuple[str, ...]

    @classmethod
    def new(
        cls,
        value: dict[str, Facet | FacetValuesT] | SelectBase | None = None,
        **facets: Facet | FacetValuesT,
    ) -> SelectBase:
        for k, v in facets.items():
            if isinstance(v, list):
                facets[k] = tuple(v)
        if isinstance(value, SelectBase):
            return value
        elif isinstance(value, dict):
            facets = dict(hashitems(value) | hashitems(facets))
        result = cls()
        for name, values in facets.items():
            result[name] = values
        return result

    @property
    def names(self) -> tuple[str, ...]:
        return self.__slots__

    def __getattr__(self, name: str) -> Facet:
        if name in self.names:
            return super().__getattribute__(name)
        else:
            raise AttributeError(name)

    def __getitem__(self, name: str) -> Facet:
        if name in self.names:
            return getattr(self, name)
        else:
            raise KeyError(name)

    def __setattr__(self, name: str, value: Facet | FacetValuesT):
        if name in self.names:
            facet: Facet | None = getattr(self, name, None)
            if facet:
                raise AlreadySetFacet(name, ", ".join(facet.values))
            super().__setattr__(name, Facet.new(value))
        else:
            raise AttributeError(name)

    def __setitem__(self, name: str, value: Facet | FacetValuesT):
        if name in self.names:
            setattr(self, name, value)
        else:
            raise KeyError(name)

    def items(self, keep_default: bool = False) -> Iterator[tuple[str, Facet]]:
        for name in self.names:
            facet = getattr(self, name)
            if not keep_default and not facet.values:
                continue
            yield name, facet

    def __bool__(self) -> bool:
        return next(self.items(), None) is not None

    def asdict(self) -> dict[str, FacetValuesT]:
        result: dict[str, FacetValuesT] = {}
        for name, facet in self.items():
            values: FacetValuesT = facet.values
            if len(values) == 1:
                values = values[0]
            else:
                values = list(values)
            result[name] = values
        return result

    def __rich_repr__(self) -> Iterator[tuple[str, FacetValuesT]]:
        for name, facet in self.items():
            values: FacetValuesT = facet.values
            if len(values) == 1:
                values = values[0]
            yield name, values

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        items = [f"{k}={v}" for k, v in self.__rich_repr__()]
        return f"{cls_name}(" + ", ".join(items) + ")"


DefaultFacets = [
    "query",
    "start",
    "end",
    "facets",
    "url",
    "data_node",
    "index_node",
    "master_id",
    "instance_id",  # search does not work with instance_id
    "title",
    "variable_long_name",
    "experiment_family",
]
BaseFacets = [
    "project",
    "mip_era",
    "experiment",
    "experiment_id",
    "institute",
    "institution_id",
    "model",
    "table_id",
    "activity_id",
    "ensemble",
    "variant_label",
    "realm",
    "frequency",
    "time_frequency",
    "variable",
    "variable_id",
    "dataset_id",
    "source_id",
    "domain",
    "driving_model",
    "rcm_name",
    "member_id",
    "cmor_table",
]


if TYPE_CHECKING:
    Select = SelectBase
else:
    SelectField = field(factory=Facet)
    Select = make_class(
        "Select",
        attrs={name: SelectField for name in DefaultFacets + BaseFacets},
        bases=(SelectBase,),
        repr=False,
        slots=True,
    )
