from __future__ import annotations
import abc
from typing import overload, Literal, Any

# import pyessv
from enum import Enum, auto

from esgpull.utils import Semver, errors
from esgpull.storage import json, sqlite


class StorageMode(Enum):
    Sqlite = auto()
    Json = auto()


# [--]TODO: figure out how to do nice+easy interfacing / backend swap.
class Storage(abc.ABC):
    """
    Abstract storage class.

    Also its own factory via the `StorageMode` enum, mypy seems ok with it,
    as long as __new__ is exhaustively defined using typing's overload/Literal.
    """

    semver: Semver = NotImplemented

    @overload
    def __new__(
        cls, mode: Literal[StorageMode.Sqlite], *args: Any, **kwargs: Any
    ) -> SqliteStorage:
        ...

    @overload
    def __new__(
        cls, mode: Literal[StorageMode.Json], *args: Any, **kwargs: Any
    ) -> JsonStorage:
        ...

    def __new__(cls, mode, *args, **kwargs):
        match mode:
            case StorageMode.Sqlite:
                return super().__new__(SqliteStorage)
            case StorageMode.Json:
                return super().__new(JsonStorage)
            case _:
                raise errors.UnknownMode(mode)

    # @abc.abstractmethod
    # def get_file(self):
    #     ...


class StorageModeCatcher:
    """
    Hack to remove `mode` from __init__ arguments...
    """

    def __init__(self, mode: StorageMode, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class SqliteStorage(StorageModeCatcher, sqlite.SqliteStorage, Storage):
    """
    Inherits, in this order for method resolution:
        - from StorageModeCatcher to remove `mode` argument
        - from sqlite.SqliteStorage for actual implementation
        - from Storage to tell mypy it is a concrete subclass.
    """


class JsonStorage(StorageModeCatcher, json.JsonStorage, Storage):
    """
    Inherits, in this order for method resolution:
        - from StorageModeCatcher to remove `mode` argument
        - from json.JsonStorage for actual implementation
        - from Storage to tell mypy it is a concrete subclass.
    """
