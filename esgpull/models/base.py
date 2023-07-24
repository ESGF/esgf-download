from collections.abc import Mapping
from dataclasses import Field
from hashlib import sha1
from typing import Any, ClassVar, TypeVar, cast

import sqlalchemy as sa
from sqlalchemy.orm import (
    DeclarativeBase,
    InstanceState,
    Mapped,
    MappedAsDataclass,
    mapped_column,
)

T = TypeVar("T")
Sha = sa.String(40)


class Base(MappedAsDataclass, DeclarativeBase):
    __dataclass_fields__: ClassVar[dict[str, Field]]
    __sql_attrs__ = ("id", "sha", "_sa_instance_state", "__dataclass_fields__")

    sha: Mapped[str] = mapped_column(
        Sha,
        init=False,
        repr=False,
        primary_key=True,
    )

    @property
    def _names(self) -> tuple[str, ...]:
        result: tuple[str, ...] = ()
        for name in self.__dataclass_fields__:
            if name in self.__sql_attrs__:
                continue
            result += (name,)
        return result

    def _as_bytes(self) -> bytes:
        raise NotImplementedError

    def compute_sha(self) -> None:
        self.sha = sha1(self._as_bytes()).hexdigest()

    @property
    def state(self) -> InstanceState:
        return cast(InstanceState, sa.inspect(self))

    def asdict(self) -> Mapping[str, Any]:
        raise NotImplementedError
