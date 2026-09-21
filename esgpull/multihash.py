from enum import Enum

from esgpull.exceptions import UnknownMultihashError


class MultihashAlg(Enum):
    sha1 = 0x11
    sha256 = 0x12
    sha512 = 0x13
    sha3_512 = 0x14
    sha3_256 = 0x16


# Code initially from esgf-prepare :
# https://github.com/ESGF/esgf-prepare/blob/58d6b19369d9a0d5e00d68c299b663b0a6addeb0/esgprep/_utils/checksum.py#L49
def varint_encode(n: int) -> bytes:
    """
    Encode an integer as varint (variable-length integer encoding).

    Args:
        n: The integer to encode

    Returns:
        The varint-encoded bytes
    """
    result = bytearray()
    while n >= 0x80:
        result.append((n & 0x7F) | 0x80)
        n >>= 7
    result.append(n & 0x7F)
    return bytes(result)


def varint_decode(data: bytes, offset: int = 0) -> tuple[int, int]:
    """
    Decode a varint from bytes.

    Args:
        data: The bytes to decode from
        offset: Starting position in the bytes

    Returns:
        A tuple of (decoded_value, new_offset)
    """
    result = 0
    shift = 0
    pos = offset

    while pos < len(data):
        byte = data[pos]
        result |= (byte & 0x7F) << shift
        pos += 1
        if (byte & 0x80) == 0:
            break
        shift += 7

    return result, pos


def detect_multihash_algo(hash_hex: str) -> MultihashAlg:
    """
    Detect the multihash algorithm from a multihash hex string.

    Args:
        hash_hex: The multihash as a hexadecimal string

    Returns:
        The algorithm name (e.g., "sha2-256") or None if not a valid multihash
    """

    # Convert hex to bytes
    hash_bytes = bytes.fromhex(hash_hex)

    # Decode the algorithm code (first varint)
    code, offset = varint_decode(hash_bytes, 0)

    # Find the algorithm by code
    if code in MultihashAlg:
        return MultihashAlg(code)
    else:
        raise UnknownMultihashError(code, hash_hex)

