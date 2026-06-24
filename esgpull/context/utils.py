from esgpull.context.types import HintsDict


def hits_from_hints(*hints: HintsDict) -> list[int]:
    result: list[int] = []
    for hint in hints:
        if len(hint) > 0:
            key = next(iter(hint))
            num = sum(hint[key].values())
        else:
            num = 0
        result.append(num)
    return result
