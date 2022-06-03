from __future__ import annotations
from typing import Any, Optional, Callable
from contextlib import AbstractContextManager

import sqlalchemy as sa

from esgpull.storage.sqlite.core import SqliteStorage
from esgpull.storage.sqlite.tables import Table
from esgpull.storage.sqlite.types import Row, Result, SelectStmt


class SelectContext(AbstractContextManager):
    """
    Context manager to simplify `sqlalchemy.select` usage with custom
    `SqliteStorage` objects.

    The query must start with a `select` method to register an initial
    statement in the context. Any new `select` will erase previous statements.
    After that, any regular sqlalchemy method can be used to further refine the
    statement, using `ctx.<sqlalchemy-method>(...)`.
    Operations can also be chained, the same way as in sqlalchemy.

    Tables are copied as context attributes to enable shorter syntax.

    Example:
        ```python
        from esgpull.storage.sqlite import SqliteStorage, SelectContext
        from humanize import naturalsize

        storage = SqliteStorage(...)


        with SelectContext(storage) as ctx:
            ctx.select(ctx.Version.version)
            print("version: ", ctx.scalar)

            ctx.select(ctx.File.file_id, ctx.File.size)
            ctx.where(ctx.File.file_id >= 1)
            for id, size in ctx.result:
                print(f"id: {id}, size: {naturalsize(size)})")

            ctx.select(ctx.Param).where(ctx.Param.name.like("%ess"))
            for param in ctx.scalars:
                print(param)

        # version:  3.10
        # id: 1, size: 1.9 GB
        # id: 2, size: 2.2 GB
        # id: 3, size: 2.2 GB
        # Param(id=1, name='access', value='Globus', last_updated=None)
        # Param(id=2, name='access', value='GridFTP', last_updated=None)
        # Param(id=3, name='access', value='HTTPServer', last_updated=None)
        # Param(id=4, name='access', value='LAS', last_updated=None)
        # Param(id=5, name='access', value='OPENDAP', last_updated=None)
        ```
    """

    def __init__(self, storage: SqliteStorage) -> None:
        self.storage = storage
        self.stmt: Optional[SelectStmt] = None
        for table in self.storage.tables:
            setattr(self, table.__name__, table)

    def __enter__(self) -> SelectContext:
        """
        Nothing more to do here than returning the current context.
        """
        return self

    def __exit__(self, *exc):
        """
        We don't really need this. Maybe sometime later...
        """
        ...

    def select(self, *tables: Table) -> SelectContext:
        """
        Initialize the statement to be run with a regular `select`.

        The syntax is the same as sqlalchemy's.
        Returns self to enable chaining with `CompoundSelect` methods.
        """
        self.stmt = sa.select(*tables)
        return self

    def __getattr__(self, attr) -> Callable[..., SelectContext]:
        """
        Enables chaining operations on `self.stmt`.

        Methods of `CompoundSelect` that are called on `self.stmt` will be
        called inside `argbuster`, such that the stmt is registered in the
        current context.
        which exists simply to allow `()` after
        python's `__getattr__` which only
        argbuster(...) is returned used to catch `*args/**kwargs`

        Looks complex but isn't really...
        """
        assert self.stmt is not None

        def argbuster(*a, **kw):
            self.stmt = getattr(self.stmt, attr)(*a, **kw)
            return self

        return argbuster

    def execute(self) -> Result:
        """
        Execute and returns the context's statement.
        """
        assert self.stmt is not None
        return self.storage.session.execute(self.stmt)

    @property
    def result(self) -> list[Row]:
        """
        Returns statement's result as a list of sqlalchemy rows.
        """
        return self.execute().all()

    @property
    def scalars(self) -> list[Any]:
        """
        Returns statement's result as a list of sqlalchemy scalars.
        """
        return self.execute().scalars().all()

    @property
    def scalar(self) -> Any:
        """
        Returns statement's result as a single sqlalchemy scalar.
        """
        return self.execute().scalar()
