class EsgpullException(Exception):
    msg: str = NotImplemented

    def __init__(self, *args):
        self.message = self.msg.format(*args)
        super().__init__(self.message)


class NoRootError(EsgpullException):
    msg = "`ST_HOME` not set."


class NotSupportedVersion(EsgpullException):
    msg = "The version {} is not supported."


class NotASemverError(EsgpullException):
    msg = "Expected a version but got '{}'."


class AlreadyMappedError(EsgpullException):
    msg = "This instance of '{}' already has a mapping."


class NotMappedError(EsgpullException):
    msg = "{} has no mapping."


class UnknownMode(EsgpullException):
    msg = "Unknown mode: '{}'."


class UnknownFacetName(AttributeError, EsgpullException):
    """
    AttributeError is required for autocomplete engines (e.g. jupyter).
    """

    msg = "'{}' is not a valid facet."


class ImpossibleFacet(EsgpullException):
    msg = """{} cannot be set with the current facets:
    {}"""


class UnknownFacetValue(EsgpullException):
    msg = "'{}' is not valid for {}."


class OutsideContext(EsgpullException):
    msg = """Facets should only be set/appended to inside a `with` contextG
    {}"""


__all__ = [
    "NoRootError",
    "NotSupportedVersion",
    "NotASemverError",
    "AlreadyMappedError",
    "NotMappedError",
    "UnknownMode",
    "UnknownFacetName",
    "ImpossibleFacet",
    "UnknownFacetValue",
    "OutsideContext",
]
