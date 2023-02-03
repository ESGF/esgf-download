from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class Result(Generic[T]):
    data: T
    ok: bool
    err: BaseException | None


@dataclass
class Ok(Result[T]):
    ok: bool = field(default=True, init=False)
    err: None = field(default=None, init=False)


@dataclass
class Err(Result[T]):
    ok: bool = field(default=False, init=False)
    err: BaseException = field()


# class Result:
#     ok: bool
#     file: File
#     completed: int
#     err: BaseException | None


# class Ok(Result):
#     ok = True
#     file: File
#     completed: int
#     err = None

# def __init__(self, file: File, completed: int) -> None:
#     self.file = file
#     self.completed = completed


# class Err(Result):
#     ok = False
#     file: File
#     completed: int
#     err: BaseException

#     def __init__(self, file: File, completed: int, err: BaseException) -> None:
#         self.file = file
#         self.completed = completed
#         self.err = err
