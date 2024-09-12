# from esgpull.constants import ROOT_ENV


class EsgpullException(Exception):
    msg: str = NotImplemented

    def __init__(self, *args, **kwargs):
        self.message = self.msg.format(*args, **kwargs)
        super().__init__(self.message.strip())


# class NoRootError(EsgpullException):
#     msg = f"Environment variable `{ROOT_ENV}` must be set."


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


class BadConfigError(EsgpullException):
    msg = """
    Please fix your config, located at {}
    """


class VirtualConfigError(EsgpullException):
    msg = """
    This config was not loaded from a file.
    """


class InstallException(EsgpullException): ...


class UnknownDefaultQueryID(EsgpullException):
    msg = "{}"


class UntrackableQuery(EsgpullException):
    msg = """
    {} cannot be tracked, it has unset options.
    """


class UnsetOptionsError(EsgpullException):
    msg = """
    {} has some unset options.
    """


class UnregisteredInstallPath(InstallException):
    msg = "{}"


class UnknownInstallName(InstallException):
    msg = "{!r}"


class NoInstallPath(InstallException):
    msg = """Choose or install one

Show existing install locations with:
$ esgpull self choose

Install a new location with:
$ esgpull self install
"""


class InvalidInstallPath(InstallException):
    msg = """{path}

Choose this install location with:
$ esgpull self choose {path}

Install a new location with:
$ esgpull self install
"""


class AlreadyInstalledPath(InstallException):
    msg = """{path}
{msg}
"""


class AlreadyInstalledName(InstallException):
    msg = """{name}
{msg}
"""
