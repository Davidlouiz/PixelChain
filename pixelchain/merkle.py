"""Merkle tree for canvas state verification."""

from __future__ import annotations

import hashlib
from typing import List


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def merkle_root(leaves: List[bytes]) -> bytes:
    """Compute the Merkle root of a list of 32-byte leaf hashes.

    If the list is empty, returns the hash of an empty byte string.
    If the number of leaves is odd, the last leaf is duplicated.
    """
    if not leaves:
        return _sha256(b'')

    layer = list(leaves)
    while len(layer) > 1:
        if len(layer) % 2 == 1:
            layer.append(layer[-1])  # duplicate last
        next_layer = []
        for i in range(0, len(layer), 2):
            next_layer.append(_sha256(layer[i] + layer[i + 1]))
        layer = next_layer
    return layer[0]


def canvas_merkle_root(canvas: bytearray) -> bytes:
    """Compute Merkle root of the full 1000×1000 RGB canvas.

    Each pixel (3 bytes) is hashed individually to form a leaf.
    Total leaves: 1,000,000.
    """
    num_pixels = len(canvas) // 3
    leaves = []
    for i in range(num_pixels):
        offset = i * 3
        pixel_bytes = canvas[offset:offset + 3]
        leaves.append(_sha256(pixel_bytes))
    return merkle_root(leaves)
