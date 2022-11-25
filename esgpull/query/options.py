from __future__ import annotations

from enum import Enum, auto
from typing import Iterator

from attrs import define, field, setters


class Option(Enum):
    notset = auto()
    none = auto()
    true = auto()
    false = auto()

    def __repr__(self) -> str:
        return self.name.upper()

    def isset(self) -> bool:
        return self in [
            Option.true,
            Option.false,
            Option.none,
        ]

    def isbool(self) -> bool:
        return self in [Option.true, Option.false]

    def __bool__(self) -> bool:
        if not self.isbool():
            raise ValueError(self)
        return self == Option.true

    @staticmethod
    def parse(value: Option | bool | None) -> Option:
        if isinstance(value, Option):
            return value
        elif value is None:
            return Option.none
        elif value is False:
            return Option.false
        elif value is True:
            return Option.true
        else:
            raise ValueError(value)


OptionsField = field(
    default=Option.notset,
    on_setattr=setters.convert,
    converter=Option.parse,
)


@define
class Options:
    distrib: Option = OptionsField
    latest: Option = OptionsField
    replica: Option = OptionsField
    retracted: Option = OptionsField

    def __rich_repr__(self) -> Iterator[tuple[str, str]]:
        for attr in self.__attrs_attrs__:
            value = getattr(self, attr.name)
            if value.isset():
                yield attr.name, value
