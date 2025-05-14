from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from enum import Enum

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from esgpull.models.base import Base


class Option(Enum):
    false = 0, False
    true = 1, True
    none = 2, None
    notset = 3, None

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str) and value in cls._member_map_:
            return cls[value]
        elif isinstance(value, bool):
            if value:
                return cls.true
            else:
                return cls.false
        elif value is None:
            return cls.none
        else:
            raise ValueError(value)

    def is_set(self) -> bool:
        return self in [
            Option.true,
            Option.false,
            Option.none,
        ]

    def is_bool(self) -> bool:
        return self in [Option.true, Option.false]

    def __bool__(self) -> bool:
        if not self.is_bool():
            raise ValueError(self)
        return self == Option.true


class Options(Base):
    __tablename__ = "options"

    distrib: Mapped[Option] = mapped_column(sa.Enum(Option))
    latest: Mapped[Option] = mapped_column(sa.Enum(Option))
    replica: Mapped[Option] = mapped_column(sa.Enum(Option))
    retracted: Mapped[Option] = mapped_column(sa.Enum(Option))

    _distrib_ = Option(True)
    _latest_ = Option(True)
    _replica_ = Option(None)
    _retracted_ = Option(False)

    @classmethod
    def default(cls) -> Options:
        return cls(
            cls._distrib_,
            cls._latest_,
            cls._replica_,
            cls._retracted_,
        )

    @classmethod
    def _set_defaults(
        cls,
        distrib: str | bool | None,
        latest: str | bool | None,
        replica: str | bool | None,
        retracted: str | bool | None,
    ) -> None:
        cls._distrib_ = Option(distrib)
        cls._latest_ = Option(latest)
        cls._replica_ = Option(replica)
        cls._retracted_ = Option(retracted)

    def __init__(
        self,
        distrib: Option | str | bool | None = Option.notset,
        latest: Option | str | bool | None = Option.notset,
        replica: Option | str | bool | None = Option.notset,
        retracted: Option | str | bool | None = Option.notset,
    ):
        setattr(self, "distrib", distrib)
        setattr(self, "latest", latest)
        setattr(self, "replica", replica)
        setattr(self, "retracted", retracted)

    def __setattr__(
        self,
        name: str,
        value: Option | str | bool | None,
    ) -> None:
        if name in self.__sql_attrs__:
            super().__setattr__(name, value)
        elif name in self._names:
            super().__setattr__(name, Option(value))
        else:
            raise AttributeError(name)

    def __getitem__(self, name: str) -> Option:
        if name in self._names:
            return getattr(self, name)
        else:
            raise KeyError(name)

    def __setitem__(
        self,
        name: str,
        value: Option | str | bool | None,
    ) -> None:
        setattr(self, name, value)

    def _as_bytes(self) -> bytes:
        self_tuple = (self.distrib, self.latest, self.replica, self.retracted)
        return str(self_tuple).encode()

    def items(
        self,
        use_default: bool = False,
        keep_notset: bool = False,
    ) -> Iterator[tuple[str, Option]]:
        default = self.default()
        for name in self._names:
            option = getattr(self, name, Option.notset)
            if option.is_set():
                yield name, option
            elif use_default:
                yield name, getattr(default, name)
            elif keep_notset:
                yield name, option

    def asdict(self) -> MutableMapping[str, bool | None]:
        return {name: option.value[1] for name, option in self.items()}

    def __bool__(self) -> bool:
        return next(self.items(), None) is not None

    def __rich_repr__(self) -> Iterator:
        for name, option in self.items():
            yield name, option.value

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        items = [f"{k}={v}" for k, v in self.__rich_repr__()]
        return f"{cls_name}(" + ", ".join(items) + ")"

    def trackable(self) -> bool:
        return all(opt.is_set() for (_, opt) in self.items(keep_notset=True))

    def apply_defaults(self, parent: Options):
        for name, opt in self.items(keep_notset=True):
            if opt.is_set():
                continue
            elif parent[name].is_set():
                self[name] = parent[name]
            else:
                self[name] = self.default()[name]
