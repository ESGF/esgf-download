from __future__ import annotations
import abc
from typing import TypeAlias, Optional

import sqlalchemy as sa

from esgpull.utils import Semver, errors

Row: TypeAlias = sa.engine.row.Row
Registry: TypeAlias = sa.orm.registry
Engine: TypeAlias = sa.future.engine.Engine
Session: TypeAlias = sa.orm.session.Session
Result: TypeAlias = sa.engine.result.Result
Columns: TypeAlias = Optional[list[sa.Column]]
SelectStmt: TypeAlias = sa.sql.selectable.Select


class AbstractTable(abc.ABC):
    """
    Abstract table class.
    Concrete subclasses must define `get_columns`.
    """

    @classmethod
    @abc.abstractmethod
    def get_columns(cls, version: Semver) -> Columns:
        """
        Must be implemented.

        Template:
            @staticmethod
            def get_columns(version: Semver) -> Columns:
                columns: Columns = None
                match version:
                    case Semver(3):
                        columns = [sa.Column(...), ...]
                    case Semver(...):
                        ...
                return columns

        """
        raise TypeError(
            f"Can't instantiate abstract class {cls.__name__} with abstract "
            f"method get_columns, see help({cls.__name__}.get_columns)"
        )


class Table(AbstractTable):
    """
    Base class for tables.

    Defines a common `map` method to link a dataclass to a SqlAlchemy Table.
    """

    __name__: str = NotImplemented
    __table__: sa.Table = NotImplemented

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

    @classmethod
    def map(cls, mapper: Registry, version: Semver) -> Table:
        if cls.is_mapped:
            raise errors.AlreadyMappedError(cls.__name__)
        print(f"mapping {cls.__name__} table...")
        mapped = type(cls.__name__, (cls,), {})
        columns = cls.get_columns(version)
        if columns is None:
            match version:
                case Semver():
                    raise errors.UnsupportedSemverError(version)
                case _:
                    raise errors.NotASemverError(version)
        table = sa.Table(cls.__name__.lower(), mapper.metadata, *columns)
        mapper.map_imperatively(mapped, table)
        return mapped
