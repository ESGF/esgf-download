from esgpull.esgpull import Esgpull

MAJOR, MINOR, PATCH = 4, 0, 0
VERSION = 4, 0, 0
__version__ = VERSION
__versionstr__ = ".".join(map(str, VERSION))
__all__ = ["Esgpull"]
