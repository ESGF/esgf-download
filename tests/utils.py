from typing import Any, Mapping


def dict_equals_ignore(
    d1: Mapping[str, Any],
    d2: Mapping[str, Any],
    ignore_keys: list[str],
) -> bool:
    d1 = {k: v for k, v in d1.items() if k not in ignore_keys}
    d2 = {k: v for k, v in d2.items() if k not in ignore_keys}
    return d1 == d2
