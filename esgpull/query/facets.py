from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Iterator, TypeAlias, cast

from attrs import Attribute, define, field, make_class

from esgpull.exceptions import AlreadySetFacet


class Facet(tuple[str, ...]):
    def __new__(cls, values: FacetValues | None = None) -> Facet:
        # Known mypy issue on tuple subclass
        # https://github.com/python/mypy/issues/8957
        if values is None:
            return super().__new__(cls)
        elif isinstance(values, str):
            return super().__new__(cls, (values,))  # type: ignore
        else:
            return super().__new__(cls, values)  # type: ignore


FacetValues: TypeAlias = str | list[str] | Facet


class SelectBase:
    __attrs_attrs__: tuple[Attribute, ...]

    @classmethod
    def configure(cls, *names: str) -> type[SelectBase]:
        return make_class(
            "Select",
            attrs={name: field(factory=Facet) for name in names},
            bases=(cls,),
            repr=False,
            slots=False,
        )

    @classmethod
    def new(
        cls,
        value: dict[str, FacetValues] | SelectBase | None = None,
        **facets: FacetValues,
    ) -> SelectBase:
        for k, v in facets.items():
            facets[k] = Facet(v)
        if isinstance(value, SelectBase):
            return value
        elif isinstance(value, dict):
            for k, v in value.items():
                value[k] = Facet(v)
            facets = dict(value.items() | facets.items())
        result = cls()
        for name, values in facets.items():
            result[name] = values
        return result

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(attr.name for attr in self.__attrs_attrs__)

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

    def __setattr__(self, name: str, value: FacetValues):
        if name in self.names:
            facet: Facet = getattr(self, name, Facet())
            if len(facet) > 0:
                raise AlreadySetFacet(name, ", ".join(facet))
            super().__setattr__(name, Facet(value))
        else:
            raise AttributeError(name)

    def __setitem__(self, name: str, value: FacetValues):
        if name in self.names:
            setattr(self, name, value)
        else:
            raise KeyError(name)

    def items(self, keep_default: bool = False) -> Iterator[tuple[str, Facet]]:
        for name in self.names:
            facet = getattr(self, name)
            if not keep_default and len(facet) == 0:
                continue
            yield name, facet

    def __bool__(self) -> bool:
        return next(self.items(), None) is not None

    # def sha1(self) -> str:
    #     return sha1(str())

    def asdict(self) -> dict[str, FacetValues]:
        result: dict[str, FacetValues] = {}
        for name, facet in self.items():
            values: FacetValues
            if len(facet) == 1:
                result[name] = facet[0]
            else:
                result[name] = list(facet)
        return result

    def __rich_repr__(self) -> Iterator[tuple[str, FacetValues]]:
        for name, facet in self.items():
            if len(facet) == 1:
                yield name, facet[0]
            else:
                yield name, facet

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
    Select = SelectBase.configure(*DefaultFacets, *BaseFacets)
