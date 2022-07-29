from __future__ import annotations
from typing import TypeAlias, Optional

import sqlalchemy as sa

from esgpull.utils import errors

Row: TypeAlias = sa.engine.row.Row
Registry: TypeAlias = sa.orm.registry
Engine: TypeAlias = sa.future.engine.Engine
Session: TypeAlias = sa.orm.session.Session
Result: TypeAlias = sa.engine.result.Result
Columns: TypeAlias = Optional[list[sa.Column | sa.UniqueConstraint]]
SelectStmt: TypeAlias = sa.sql.selectable.Select


class Table:
    """
    Base class for tables.

    Defines a common `map` method to link a dataclass to a SqlAlchemy Table.
    """

    __name__: str = NotImplemented
    __table__: sa.Table = NotImplemented
    __columns__: list[sa.Column | sa.Constraint] = NotImplemented

    # /!\ @property before @classmethod does not work.
    @classmethod
    @property
    def is_mapped(cls) -> bool:
        return cls.__table__ is not NotImplemented

    @classmethod
    def create_table(cls, session: Session) -> None:
        if not cls.is_mapped:
            raise errors.NotMappedError(cls.__name__)
        engine = session.bind
        cls.__table__.metadata.create_all(engine, tables=[cls.__table__])
