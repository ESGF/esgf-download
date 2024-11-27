from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from typing import ClassVar, TypeAlias

import sqlalchemy as sa
from rich.pretty import pretty_repr
from sqlalchemy.orm import Mapped, relationship

from esgpull.exceptions import AlreadySetFacet, DuplicateFacet
from esgpull.models.base import Base, Sha
from esgpull.models.facet import Facet

FacetValues: TypeAlias = str | list[str]

selection_facet_proxy = sa.Table(
    "selection_facet",
    Base.metadata,
    sa.Column(
        "selection_sha", Sha, sa.ForeignKey("selection.sha"), primary_key=True
    ),
    sa.Column("facet_sha", Sha, sa.ForeignKey("facet.sha"), primary_key=True),
)


def opposite(facet_name: str) -> str:
    if facet_name[0] == "!":
        return facet_name[1:]
    else:
        return f"!{facet_name}"


class Selection(Base):
    __tablename__ = "selection"
    _facet_names: ClassVar[set[str]] = set()

    _facets: Mapped[list[Facet]] = relationship(
        secondary=selection_facet_proxy,
        default_factory=list,
    )

    @classmethod
    def _add_property(cls, name: str) -> None:
        def getter(self: Selection) -> list[str]:
            indices = self._facet_map_.get(name, set())
            return sorted([self._facets[idx].value for idx in indices])

        def setter(self: Selection, values: FacetValues):
            other = opposite(name)
            if name in self._facet_map_:
                raise AlreadySetFacet(name, ", ".join(self[name]))
            elif other in self._facet_map_:
                raise AlreadySetFacet(other, ", ".join(self[other]))
            facet_map_name = set()
            if isinstance(values, str):
                iter_values = enumerate([values])
            else:
                iter_values = enumerate(values)
            offset = len(self._facets)
            for i, value in iter_values:
                facet = Facet(name=name, value=value)
                if facet in self._facets:
                    raise DuplicateFacet(
                        facet.name,
                        facet.value,
                        pretty_repr(self),
                    )
                self._facets.append(facet)
                facet_map_name.add(offset + i)
            self._facet_map_[name] = facet_map_name

        setattr(cls, name, property(getter, setter))

    @classmethod
    def reset(cls) -> None:
        cls.configure(*DefaultFacets, *BaseFacets, replace=True)

    @classmethod
    def configure(cls, *names: str, replace: bool = True) -> None:
        nameset = set(names) | {f"!{name}" for name in names}
        if replace:
            for name in cls._facet_names:
                delattr(cls, name)
            new_names = nameset
            cls._facet_names = nameset
        else:
            new_names = nameset - cls._facet_names
            cls._facet_names |= new_names
        for name in new_names:
            cls._add_property(name)

    def __init__(
        self,
        facets: list[Facet] | None = None,
        **kwargs: FacetValues,
    ):
        if facets is None:
            self._facets = []
        else:
            self._facets = facets
        self._init_facet_map()
        for name, values in kwargs.items():
            self[name] = values

    def _init_facet_map(self) -> None:
        self._facet_map_: dict[str, set[int]] = {}
        for i, facet in enumerate(self._facets):
            self._facet_map_.setdefault(facet.name, set())
            self._facet_map_[facet.name].add(i)

    def __getitem__(self, name: str) -> list[str]:
        if name in self._facet_names:
            return getattr(self, name)
        else:
            raise KeyError(name)

    def __setitem__(self, name: str, value: FacetValues):
        if name in self._facet_names:
            setattr(self, name, value)
        else:
            raise KeyError(name)

    def items(self) -> Iterator[tuple[str, list[str]]]:
        if not hasattr(self, "_facet_map_"):
            self._init_facet_map()
        for name in sorted(self._facet_map_.keys()):
            yield name, self[name]

    def __bool__(self) -> bool:
        return bool(self._facets)

    def _as_bytes(self) -> bytes:
        return str(tuple(self.items())).encode()

    def compute_sha(self) -> None:
        for facet in self._facets:
            if facet.sha is None:
                facet.compute_sha()
        super().compute_sha()

    def asdict(self) -> MutableMapping[str, FacetValues]:
        result: dict[str, FacetValues] = {}
        for name, facet in self.items():
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
    "grid_label",
    "nominal_resolution",
]


Selection.reset()
