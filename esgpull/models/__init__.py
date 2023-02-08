from typing import TypeVar

from esgpull.models.base import Base
from esgpull.models.dataset import Dataset
from esgpull.models.facet import Facet
from esgpull.models.file import FastFile, FileStatus
from esgpull.models.options import Option, Options
from esgpull.models.query import File, LegacyQuery, Query, QueryDict
from esgpull.models.selection import Selection
from esgpull.models.synda_file import SyndaFile
from esgpull.models.tag import Tag

Table = TypeVar("Table", bound=Base)

__all__ = [
    "Base",
    "Dataset",
    "Facet",
    "FastFile",
    "File",
    "FileStatus",
    "LegacyQuery",
    "Option",
    "Options",
    "Query",
    "QueryDict",
    "Selection",
    "SyndaFile",
    "Table",
    "Tag",
]
