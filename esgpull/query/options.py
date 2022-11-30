from __future__ import annotations

from enum import Enum, auto
from typing import Iterator, TypeAlias

from attrs import define, field, setters

OptionValueT: TypeAlias = str | bool | None


class Option(Enum):
    notset = -1
    none = None
    true = True
    false = False
    # notset = auto()
    # none = auto()
    # true = auto()
    # false = auto()

    @staticmethod
    def new(value: Option | OptionValueT) -> Option:
        if isinstance(value, Option):
            return value
        elif isinstance(value, str):
            return Option[value]
        elif isinstance(value, bool) or value is None:
            return Option(value)
        # elif value is None:
        #     return Option.none
        # elif value is False:
        #     return Option.false
        # elif value is True:
        #     return Option.true
        else:
            raise ValueError(value)

    def __repr__(self) -> str:
        return self.name

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


# OptionsField = field(
#     default=Option.notset,
#     on_setattr=setters.convert,
#     converter=Option.new,
# )


class OptionsBase:
    __slots__: tuple[str, ...]


@define(slots=True)
class Options(OptionsBase):
    # TODO: figure out how to define default value on notset
    distrib: Option | OptionValueT = Option.notset
    latest: Option | OptionValueT = Option.notset
    replica: Option | OptionValueT = Option.notset
    retracted: Option | OptionValueT = Option.notset

    @classmethod
    def new(
        cls,
        value: dict[str, Option | OptionValueT] | Options | None = None,
        **options: Option | OptionValueT,
    ) -> Options:
        if isinstance(value, Options):
            return value
        elif isinstance(value, dict):
            options = dict(value.items() | options.items())
        result = cls()
        for name, option in options.items():
            setattr(result, name, Option.new(option))
        return result

    def items(self) -> Iterator[tuple[str, Option]]:
        for name in self.__slots__:
            option = getattr(self, name)
            if option.is_set():
                yield name, option

    def __bool__(self) -> bool:
        return next(self.items(), None) is not None

    def __rich_repr__(self) -> Iterator:
        yield from self.items()

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        items = [f"{k}={v.value}" for k, v in self.__rich_repr__()]
        return f"{cls_name}(" + ", ".join(items) + ")"

    def __setattr__(self, name: str, value: Option | OptionValueT) -> None:
        super().__setattr__(name, Option.new(value))
