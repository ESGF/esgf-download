from __future__ import annotations

from typing import Any, Callable, Sequence, TypeAlias

import sqlalchemy as sa
import sqlalchemy.orm

from esgpull.db.models import Table

Row: TypeAlias = sa.engine.row.Row
Result: TypeAlias = sa.engine.result.Result
Session: TypeAlias = sa.orm.session.Session
SelectStmt: TypeAlias = sa.sql.selectable.Select


class SelectContext:
    """
    Interface to simplify `sqlalchemy.select` usage with custom
    `Database` objects.

    The query must start with a `select` method to register an initial
    statement in the context. Any new `select` will erase previous statements.
    After that, any regular sqlalchemy method can be used to further refine the
    statement, using `ctx.<sqlalchemy-method>(...)`.
    Operations can also be chained, the same way as in sqlalchemy.

    Tables are copied as context attributes to enable shorter syntax.

    Example:
        ```python
        from esgpull.db.core import Database
        from esgpull.db.utils import SelectContext
        from esgpull.db.models import File, Param, Version
        from esgpull.utils import format_size

        db = Database(...)


        with db.select(Version.version) as stmt:
            print("version: ", stmt.scalar)

        with db.select(File.file_id, File.size) as stmt:
            stmt.where(File.file_id >= 1)
            for id, size in stmt.result:
                print(f"id: {id}, size: {format_size(size)})")

        with db.select(Param) as stmt:
            for param in stmt.where(Param.name.like("%ess")).scalars:
                print(param)

        # version:  3.10
        # id: 1, size: 1.9 GiB
        # id: 2, size: 2.2 GiB
        # id: 3, size: 2.2 GiB
        # Param(id=1, name='access', value='Globus', last_updated=None)
        # Param(id=2, name='access', value='GridFTP', last_updated=None)
        # Param(id=3, name='access', value='HTTPServer', last_updated=None)
        # Param(id=4, name='access', value='LAS', last_updated=None)
        # Param(id=5, name='access', value='OPENDAP', last_updated=None)
        ```
    """

    def __init__(self, session: Session, *tables: type[Table]) -> None:
        self.session = session
        self.stmt: SelectStmt = sa.select(*tables)

        # for name, table in db.tables.items():
        #     setattr(self, name, table)

    def __getattr__(self, attr) -> Callable[..., SelectContext]:
        """
        Enables chaining operations on `self.stmt`.

        Methods of `CompoundSelect` that are called on `self.stmt` will be
        called inside `stmt_setter`, such that the stmt is registered in the
        current context.
        which exists simply to allow `()` after
        python's `__getattr__` which only
        stmt_setter(...) is returned used to catch `*args/**kwargs`

        Looks complex but isn't really...
        """
        assert self.stmt is not None

        def stmt_setter(*a, **kw):
            self.stmt = getattr(self.stmt, attr)(*a, **kw)
            return self

        return stmt_setter

    def execute(self) -> Result:
        """
        Execute and returns the context's statement.
        """
        assert self.stmt is not None
        return self.session.execute(self.stmt)

    @property
    def results(self) -> Sequence[Row]:
        """
        Returns statement's result as a list of sqlalchemy rows.
        """
        return self.execute().all()

    @property
    def scalars(self) -> Sequence[Any]:
        """
        Returns statement's result as a list of sqlalchemy scalars.
        """
        return self.execute().scalars().all()

    @property
    def scalar(self) -> Any:
        """
        Returns statement's result as a single sqlalchemy scalar.
        """
        result = self.scalars
        assert len(result) == 1
        return result[0]
