class EsgpullException(Exception):
    msg: str = NotImplemented

    def __init__(self, *args):
        self.message = self.msg.format(*args)
        super().__init__(self.message)


class NoRootError(EsgpullException):
    msg = "Environment variable `ST_HOME` must be set."


class UnknownFacetName(AttributeError, EsgpullException):
    """
    AttributeError is required for autocomplete engines (e.g. jupyter).
    """

    msg = "'{}' is not a valid facet."


# # error to be used with validation implemented
# class ImpossibleFacet(EsgpullException):
#     msg = """Facet '{}' is not available with this query:
#     {}"""


class UnknownFacetValue(EsgpullException):
    msg = "'{}' is not valid for {}."


class UnsupportedSource(EsgpullException):
    msg = """This source cannot be loaded as a query:
    {}"""


__all__ = [
    "NoRootError",
    "UnknownFacetName",
    # "ImpossibleFacet",
    "UnknownFacetValue",
    "UnsupportedSource",
]
