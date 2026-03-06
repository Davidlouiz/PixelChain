"""Epoch identifier utilities.

Epochs are named with alphabetic strings: a, b, ..., z, aa, ab, ..., az, ba, ..., zz, aaa, ...
This is essentially a bijective base-26 numeration using lowercase letters.
"""


def epoch_to_index(epoch: str) -> int:
    """Convert an epoch string to a zero-based index.

    'a' -> 0, 'b' -> 1, ..., 'z' -> 25, 'aa' -> 26, 'ab' -> 27, ...
    """
    result = 0
    for ch in epoch:
        result = result * 26 + (ord(ch) - ord("a") + 1)
    return result - 1


def index_to_epoch(index: int) -> str:
    """Convert a zero-based index to an epoch string.

    0 -> 'a', 25 -> 'z', 26 -> 'aa', 27 -> 'ab', ...
    """
    if index < 0:
        raise ValueError("Index must be non-negative")
    n = index + 1
    chars = []
    while n > 0:
        n -= 1
        chars.append(chr(ord("a") + (n % 26)))
        n //= 26
    return "".join(reversed(chars))


def next_epoch(epoch: str) -> str:
    """Return the epoch following the given one."""
    return index_to_epoch(epoch_to_index(epoch) + 1)


def prev_epoch(epoch: str) -> str:
    """Return the epoch preceding the given one, or None for epoch 'a'."""
    idx = epoch_to_index(epoch)
    if idx == 0:
        return None
    return index_to_epoch(idx - 1)


def epoch_cmp(a: str, b: str) -> int:
    """Compare two epochs. Returns <0, 0, or >0."""
    return epoch_to_index(a) - epoch_to_index(b)
