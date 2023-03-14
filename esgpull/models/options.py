from __future__ import annotations

from enum import Enum, auto
from typing import Iterator, Mapping

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from esgpull.models.base import Base


class Option(Enum):
    false = auto(), False
    true = auto(), True
    none = auto(), None
    notset = auto(), None

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

    def _as_bytes(self) -> bytes:
        self_tuple = (self.distrib, self.latest, self.replica, self.retracted)
        return str(self_tuple).encode()

    def items(
        self,
        use_default: bool = False,
        keep_notset: bool = False,
    ) -> Iterator[tuple[str, Option]]:
        for name in self._names:
            option = getattr(self, name, Option.notset)
            if option.is_set():
                yield name, option
            elif use_default:
                yield name, getattr(DefaultOptions, name)
            elif keep_notset:
                yield name, option

    def asdict(self) -> Mapping[str, bool | None]:
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


DefaultOptions = Options(
    distrib=False,
    latest=True,
    replica=None,
    retracted=False,
)
