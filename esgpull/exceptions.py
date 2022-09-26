class EsgpullException(Exception):
    msg: str = NotImplemented

    def __init__(self, *args):
        self.message = self.msg.format(*args)
        super().__init__(self.message)


class NoRootError(EsgpullException):
    msg = "Environment variable `ESGPULL_HOME` must be set."


class FacetNameError(AttributeError, EsgpullException):
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


class SolrUnstableQueryError(EsgpullException):
    msg = """Solr can not handle this query:
    {}"""


class QuerySourceError(EsgpullException):
    msg = """This source cannot be parsed as a query:
    {}"""


class DownloadMethodError(EsgpullException):
    msg = """{} is not a valid download method. Choose from:
    * Download
    * ChunkedDownload
    * MultiSourceChunkedDownload
    """


__all__ = [
    "NoRootError",
    "FacetNameError",
    "SolrUnstableQueryError",
    "QuerySourceError",
    "DownloadMethodError",
]
