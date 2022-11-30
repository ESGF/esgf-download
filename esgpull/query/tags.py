from attrs import define


@define(slots=False)
class Tag:
    value: str
    description: str | None = None
