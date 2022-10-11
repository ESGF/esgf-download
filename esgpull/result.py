from esgpull.types import File


class Result:
    file: File
    ok: bool
    data: bytes | None
    err: Exception | None

    def __init__(self, file: File) -> None:
        self.file = file


class Ok(Result):
    __match_args__ = (
        "file",
        "data",
    )

    file: File
    ok = True
    data: bytes
    err = None


class Err(Result):
    __match_args__ = (
        "file",
        "err",
    )

    file: File
    ok = False
    data = None
    err: Exception


__all__ = ["Result", "Ok", "Err"]
