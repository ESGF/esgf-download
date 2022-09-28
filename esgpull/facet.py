from __future__ import annotations

from esgpull.types import FacetValues, FacetDict


class Facet:
    def __init__(self, name: str, default: FacetValues) -> None:
        self.name = name
        self.values = self._cast(default)
        self.default = frozenset(self.values)
        self.appended = False

    def reset(self) -> None:
        self.values = set(self.default)

    def isdefault(self) -> bool:
        return self.values == self.default

    @property
    def fmt_name(self) -> str:
        name = self.name
        if self.appended:
            name = "+" + name
        return name

    def dump(self) -> FacetDict:
        values: list[str] | str = sorted(self.values)
        if len(values) == 1:
            values = values[0]
        return {self.fmt_name: values}

    def __repr__(self) -> str:
        return f"{self.fmt_name}={self.values}"

    def _cast(self, values: FacetValues) -> set[str]:
        if isinstance(values, (list, set, tuple)):
            result = set(values)
        elif isinstance(values, str):
            result = set(map(str.strip, values.split(",")))
        else:
            raise ValueError(self.name, values)
        return result

    def _set(self, values: Facet | FacetValues) -> None:
        """
        Overloading assignment requires a class property's setter that calls
        this method.
        """
        if isinstance(values, Facet):
            return  # Required for `+=` / `__iadd__`.
        self.values = self._cast(values)
        self.appended = False

    def __add__(self, values: FacetValues) -> None:
        """
        Append to existing values or replace default value using `+` operator.
        `appended` is set to `True` in any case as appending is most likely
        to be used with a fresh subquery.

        Example:
            ```python
            f = Facet("name", default="*")
            f += "first"
            f += "second"
            print(f)
            # name: [second,first]
            ```
        """
        if self.isdefault():
            self._set(values)
        else:
            self.values |= self._cast(values)
        self.appended = True


__all__ = ["Facet"]
