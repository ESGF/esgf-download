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
        values: list[str] | str = list(self.values)
        if len(values) == 1:
            values = values[0]
        return {self.fmt_name: values}

    def __repr__(self) -> str:
        return ",".join(self.values)

    def tostring(self) -> str:
        result = str(self)
        if len(self.values) > 1:
            result = "[" + result + "]"
        return f"{self.name}:{result}"

    def _cast(self, values: FacetValues) -> set[str]:
        if isinstance(values, (list, set, tuple)):
            result = set(values)
        elif isinstance(values, str):
            result = set(map(str.strip, values.split(",")))
        else:
            raise ValueError(self.name, values)
        return result

    def _set(self, values: FacetValues | Facet) -> None:
        """
        We can only "overload" the `=` operator with a property, so this
        method implements the replacement, and is called from `__set__`.
        """
        if isinstance(values, Facet):
            return  # Required for `+=` / `__iadd__`.
        self.values = self._cast(values)
        self.appended = False

    def _append(self, values: FacetValues) -> None:
        """
        Append values, replace if default.
        `appended` is set to `True` in both cases as appending is most likely
        done on a freshly created query with all defaults.
        """
        values = self._cast(values)
        if self.isdefault():
            self.values = values
        else:
            self.values |= values
        self.appended = True

    def __iadd__(self, values: FacetValues) -> Facet:
        """
        Define `+=` as appending operator.

        Example:
            ```python
            f = Facet("name", default="*")
            f += "first"
            f += "second"
            print(f.tostring())
            # name: [second,first]
            ```
        """
        self._append(values)
        return self


__all__ = ["Facet"]
