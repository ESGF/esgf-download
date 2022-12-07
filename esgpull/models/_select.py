from typing import ClassVar, Iterator, TypeAlias

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, relationship

from esgpull.exceptions import AlreadySetFacet
from esgpull.models.base import Base, Sha
from esgpull.models.facet import Facet

FacetValues: TypeAlias = str | list[str]

select_facet_proxy = sa.Table(
    "select_facet",
    Base.metadata,
    sa.Column(
        "select_sha", Sha, sa.ForeignKey("select.sha"), primary_key=True
    ),
    sa.Column("facet_sha", Sha, sa.ForeignKey("facet.sha"), primary_key=True),
)


class Select(Base):
    __tablename__ = "select"
    __facet_names: ClassVar[set[str]] = set()

    facets: Mapped[list[Facet]] = relationship(
        secondary=select_facet_proxy,
        default_factory=list,
    )

    @classmethod
    def configure(cls, *names: str, replace: bool = True) -> None:
        if replace:
            cls.__facet_names = set(names)
        else:
            cls.__facet_names |= set(names)

    def __init__(
        self,
        facets: list[Facet] | None = None,
        **kwargs: FacetValues,
    ):
        if facets is None:
            self.facets = []
        else:
            self.facets = facets
        self._init_facet_map()
        for name, values in kwargs.items():
            self[name] = values

    def _init_facet_map(self) -> None:
        self._facet_map_: dict[str, set[int]] = {}
        for i, facet in enumerate(self.facets):
            self._facet_map_.setdefault(facet.name, set())
            self._facet_map_[facet.name].add(i)

    def __getattr__(self, name: str) -> list[str]:
        # `__sql_attrs__` and `facets` already covered in __getattribute__
        if name in self.__facet_names:
            indices = self._facet_map_.get(name, set())
            return sorted([self.facets[idx].value for idx in indices])
        else:
            raise AttributeError(name)

    def __getitem__(self, name: str) -> list[str]:
        if name in self.__facet_names:
            return getattr(self, name)
        else:
            raise KeyError(name)

    def __setattr__(self, name: str, values: FacetValues):
        if name in self.__sql_attrs__ + ("facets", "_facet_map_"):
            super().__setattr__(name, values)
        elif name in self.__facet_names:
            if name in self._facet_map_:
                raise AlreadySetFacet(name, ", ".join(self[name]))
            self._facet_map_[name] = set()
            if isinstance(values, str):
                values = [values]
            offset = len(self.facets)
            for i, value in enumerate(values):
                facet = Facet(name=name, value=value)
                if facet in self.facets:
                    raise ValueError(facet)
                self.facets.append(facet)
                self._facet_map_[name].add(offset + i)
        else:
            raise AttributeError(name)

    def __setitem__(self, name: str, value: FacetValues):
        if name in self.__facet_names:
            setattr(self, name, value)
        else:
            raise KeyError(name)

    def items(self) -> Iterator[tuple[str, list[str]]]:
        if not hasattr(self, "_facet_map_"):
            self._init_facet_map()
        for name in sorted(self._facet_map_.keys()):
            yield name, self[name]

    def __bool__(self) -> bool:
        return bool(self.facets)

    def _as_bytes(self) -> bytes:
        return str(tuple(self.items())).encode()

    def compute_sha(self) -> None:
        for facet in self.facets:
            if facet.sha is None:
                facet.compute_sha()
        super().compute_sha()

    def asdict(self) -> dict[str, FacetValues]:
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
]


Select.configure(*DefaultFacets, *BaseFacets, replace=True)
