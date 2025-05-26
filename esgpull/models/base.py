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


# Base class for all models - provides core SQLAlchemy functionality
class _BaseModel(MappedAsDataclass, DeclarativeBase):
    __dataclass_fields__: ClassVar[dict[str, Field]]
    __sql_attrs__ = ("id", "_sa_instance_state", "__dataclass_fields__")

    @property
    def _names(self) -> tuple[str, ...]:
        result: tuple[str, ...] = ()
        for name in self.__dataclass_fields__:
            if name in self.__sql_attrs__:
                continue
            result += (name,)
        return result

    @property
    def state(self) -> InstanceState:
        return cast(InstanceState, sa.inspect(self))

    def asdict(self) -> Mapping[str, Any]:
        raise NotImplementedError


# Base class for models that use SHA as primary key
class Base(_BaseModel):
    __abstract__ = True
    __sql_attrs__ = ("id", "sha", "_sa_instance_state", "__dataclass_fields__")

    sha: Mapped[str] = mapped_column(
        Sha,
        init=False,
        repr=False,
        primary_key=True,
    )

    def _as_bytes(self) -> bytes:
        raise NotImplementedError

    def compute_sha(self) -> None:
        self.sha = sha1(self._as_bytes()).hexdigest()


# Base class for models that don't use SHA (e.g., Dataset)
class BaseNoSHA(_BaseModel):
    __abstract__ = True
    __sql_attrs__ = ("id", "_sa_instance_state", "__dataclass_fields__")


# Keep SHAKeyMixin for backward compatibility if needed
SHAKeyMixin = Base
