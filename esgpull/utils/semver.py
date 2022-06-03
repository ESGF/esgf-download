from __future__ import annotations
from typing import Optional


class Semver:
    __match_args__ = ("major", "minor", "patch")

    def __init__(
        self,
        major: int | str,
        minor: Optional[int] = None,
        patch: Optional[int] = None,
        /,
    ) -> None:
        if isinstance(major, str):
            major, minor, patch = Semver.from_string(major)
        self.major: int = major
        self.minor: Optional[int] = minor
        self.patch: Optional[int] = patch

    @staticmethod
    def from_string(src: str) -> tuple[int, Optional[int], Optional[int]]:
        match list(map(int, src.split("."))):
            case [major]:
                minor = patch = None
            case [major, minor]:
                patch = None
            case [major, minor, patch]:
                ...
            case _:
                raise TypeError(src)
        return major, minor, patch

    def __repr__(self) -> str:
        s = str(self.major)
        if self.minor is not None:
            s += f".{self.minor}"
            if self.patch is not None:
                s += f".{self.patch}"
        return s

    def totuple(self) -> tuple[int, Optional[int], Optional[int]]:
        return (self.major, self.minor, self.patch)

    def __eq__(self, other) -> bool:
        return self.totuple() == other.totuple()

    def __ge__(self, other: Semver) -> bool:
        return self.totuple() >= other.totuple()

    def __le__(self, other: Semver) -> bool:
        return other >= self

    def __gt__(self, other: Semver) -> bool:
        return self.totuple() > other.totuple()

    def __lt__(self, other: Semver) -> bool:
        return other > self


# # TODO: these as unit tests
# if __name__ == "__main__":
#     assert str(Semver(1)) == "1"
#     assert str(Semver(1, 2)) == "1.2"
#     assert str(Semver(1, 2, 3)) == "1.2.3"

#     v = Semver("1")
#     assert all([v.major == 1, v.minor is None, v.patch is None])

#     v = Semver("1.2")
#     assert all([v.major == 1, v.minor == 2, v.patch is None])

#     v = Semver("1.2.3")
#     assert all([v.major == 1, v.minor == 2, v.patch == 3])

#     try:
#         Semver()
#     except TypeError:
#         assert True

#     try:
#         Semver(1, 2, 3, 4)
#     except TypeError:
#         assert True

#     try:
#         Semver("1.2.3.4")
#     except TypeError:
#         assert True

#     assert Semver(1, 3) > Semver(1, 2)
#     assert Semver(1, 3) < Semver(2, 1)
