class EsgpullException(Exception):
    msg: str = NotImplemented

    def __init__(self, *args):
        self.message = self.msg.format(*args)
        super().__init__(self.message)


class NoRootError(EsgpullException):
    msg = "Environment variable `ESGPULL_HOME` must be set."


class UnknownFacetName(AttributeError, EsgpullException):
    """
    AttributeError is required for autocomplete engines (e.g. jupyter).
    """

    msg = "'{}' is not a valid facet."


# # errors meant for use when validation is implemented
# class UnknownFacetValue(EsgpullException):
#     msg = "'{}' is not valid for {}."
# class ImpossibleFacet(EsgpullException):
#     msg = """Facet '{}' is not available with this query:
#     {}"""


class UnstableSolrQuery(EsgpullException):
    msg = """Solr can not handle this query:
    {}"""


class UnsupportedSource(EsgpullException):
    msg = """This source cannot be loaded as a query:
    {}"""


__all__ = [
    "NoRootError",
    "UnknownFacetName",
    "UnstableSolrQuery",
    "UnsupportedSource",
]
