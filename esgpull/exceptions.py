from esgpull.constants import ROOT_ENV


class EsgpullException(Exception):
    msg: str = NotImplemented

    def __init__(self, *args, **kwargs):
        self.message = self.msg.format(*args, **kwargs)
        super().__init__(self.message.strip())


class NoRootError(EsgpullException):
    msg = f"Environment variable `{ROOT_ENV}` must be set."


class InvalidInstallDirectory(EsgpullException):
    msg = """{path}

To setup a new install directory, please run:
$ esgpull self install

or to set this location as the install directory:
$ esgpull self install {path}
"""


class PathAlreadyInstalled(EsgpullException):
    msg = """{path}
{msg}
"""


class NameAlreadyInstalled(EsgpullException):
    msg = """{name}
{msg}
"""


# class UnknownFacet(EsgpullException):
#     msg = "{}"


class FacetNameError(EsgpullException, AttributeError):
    """
    AttributeError is required for autocomplete engines (e.g. jupyter).
    """

    msg = "'{}' is not a valid facet."


class AlreadySetFacet(EsgpullException):
    msg = "'{}' is already set to [{}]"


class DuplicateFacet(EsgpullException):
    msg = "'{}:{}'\n{}"


class QueryDuplicate(EsgpullException):
    msg = "{}"


class PageIndexError(EsgpullException, IndexError):
    msg = "Cannot show page {}/{}."


# # errors meant for use when validation is implemented
# class UnknownFacetValue(EsgpullException):
#     msg = "'{}' is not valid for {}."
# class ImpossibleFacet(EsgpullException):
#     msg = """Facet '{}' is not available with this query:
#     {}"""


class SolrUnstableQueryError(EsgpullException):
    msg = """
    Solr can not handle this query:
    {}
    """


class QuerySourceError(EsgpullException):
    msg = """
    This source cannot be parsed as a query:
    {}
    """


class TooShortKeyError(EsgpullException, KeyError):
    msg = "{}"


class GraphWithoutDatabase(EsgpullException):
    msg = "Graph is not connected to a database."


class DownloadKindError(EsgpullException):
    msg = """{} is not a valid download kind. Choose from:
    * Download
    * ChunkedDownload
    * MultiSourceChunkedDownload
    """


class DownloadSizeError(EsgpullException):
    msg = """
    Downloaded file is larger than expected: {} > {}
    """


class DownloadCancelled(EsgpullException):
    msg = """
    Download cancelled by user.
    """


class NoClauseError(EsgpullException):
    msg = """
    No clause provided (query might be empty).
    """


class VirtualConfigError(EsgpullException):
    msg = """
    This config was not loaded from a file.
    """


class UnregisteredInstallPath(EsgpullException):
    msg = "{}"


class UnknownInstallName(EsgpullException):
    msg = "{!r}"
