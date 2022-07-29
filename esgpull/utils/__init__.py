import humanize

from esgpull.utils.semver import Semver
import esgpull.utils.errors as errors


def naturalsize(value: int | float) -> str:
    """Get size in KiB / MiB / GiB / etc."""
    return humanize.naturalsize(value, "unix")


__all__ = ["Semver", "errors", "naturalsize"]
