from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, Sequence

from attrs import Attribute, field, make_class

from esgpull.query.facet import Facet, FacetValues


class SelectBase:
    __attrs_attrs__: Sequence[Attribute]

    @classmethod
    def from_dict(cls, **facets: str | list[str]) -> SelectBase:
        result = cls()
        for key, values in facets.items():
            if key[0] == "+":
                key = key[1:]
                replace = False
            else:
                replace = True
            facet = Facet(values, replace=replace)
            setattr(result, key, facet)
        return result

    def items(
        self, renderable: bool = False, keep_default: bool = False
    ) -> Iterator[tuple[str, Facet]]:
        for attr in self.__attrs_attrs__:
            name = attr.name
            facet = getattr(self, name)
            if renderable and not facet.replace:
                name = f"+{name}"
            if not keep_default and not facet.values:
                continue
            yield name, facet

    def __rich_repr__(self) -> Iterator[tuple[str, FacetValues]]:
        for name, facet in self.items(renderable=True):
            values: FacetValues = facet.values
            if len(values) == 1:
                values = values[0]
            yield name, values

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        items: list[str] = []
        for name, facet in self.items(renderable=True):
            values = str(facet.values)
            if len(facet.values) == 1:
                values = values[1:-1]
            items.append(f"{name}={values}")
        return f"{cls_name}(" + ", ".join(items) + ")"

    def dump(self) -> dict[str, str | list[str]]:
        result: dict[str, str | list[str]] = {}
        for name, facet in self.items():
            if not facet.values:
                continue
            if not facet.replace:
                name = f"+{name}"
            if len(facet.values) == 1:
                result[name] = facet.values[0]
            else:
                result[name] = facet.values
        return result


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

SelectField = field(
    factory=Facet,
    on_setattr=Facet.new,
)

if TYPE_CHECKING:

    class Select(SelectBase):
        ...

else:
    Select: type[SelectBase] = make_class(
        "Select",
        attrs={name: SelectField for name in DefaultFacets + BaseFacets},
        bases=(SelectBase,),
        repr=False,
        slots=True,
    )
