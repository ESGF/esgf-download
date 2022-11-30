from typing import Any, ItemsView, TypeAlias, TypeVar

ArrayStr: TypeAlias = list[str] | tuple[str, ...]
FacetValuesT: TypeAlias = str | ArrayStr

T = TypeVar("T", bound=ArrayStr)


def tuplify(values: FacetValuesT) -> tuple[str, ...]:
    if isinstance(values, str):
        return (values,)
    else:
        return tuple(values)


def hashitems(d: dict[str, Any]) -> ItemsView:
    new_d: dict[str, str | tuple[str, ...]] = {}
    for k, v in d.items():
        if isinstance(v, (list, tuple, set)):
            new_d[k] = tuple(v)
        # elif isinstance(v, dict):
        #     new_d[k] = dict(hashitems(v))
        else:
            new_d[k] = v
    return new_d.items()
