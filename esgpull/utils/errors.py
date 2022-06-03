class EsgpullException(Exception):
    msg: str = NotImplemented

    def __init__(self, name):
        self.message = self.msg.format(name)
        super().__init__(self.message)


class UnsupportedSemverError(EsgpullException):
    msg = """The version {} is not supported."""


class NotASemverError(EsgpullException):
    msg = """Expected a version but got '{}'."""


class AlreadyMappedError(EsgpullException):
    msg = """This instance of '{}' already has a mapping."""


class NotMappedError(EsgpullException):
    msg = """{} has no mapping."""


class ModeError(EsgpullException):
    msg = """Unknown mode: '{}'."""


__all__ = [
    "UnsupportedSemverError",
    "NotASemverError",
    "AlreadyMappedError",
    "NotMappedError",
    "ModeError",
]
