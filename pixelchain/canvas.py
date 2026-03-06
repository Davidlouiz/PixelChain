"""Canvas state management — 1000×1000 RGB24 pixel grid."""

from __future__ import annotations

import base64
import copy
from typing import Optional, Tuple

from .config import CANVAS_WIDTH, CANVAS_HEIGHT, CANVAS_BYTES
from .merkle import canvas_merkle_root


class Canvas:
    """In-memory representation of the 1000×1000 RGB canvas."""

    def __init__(self, data: Optional[bytearray] = None):
        if data is not None:
            if len(data) != CANVAS_BYTES:
                raise ValueError(
                    f"Canvas data must be exactly {CANVAS_BYTES} bytes, got {len(data)}"
                )
            self._data = bytearray(data)
        else:
            # initialise to black
            self._data = bytearray(CANVAS_BYTES)

    def _offset(self, x: int, y: int) -> int:
        if not (0 <= x < CANVAS_WIDTH and 0 <= y < CANVAS_HEIGHT):
            raise ValueError(f"Coordinates ({x}, {y}) out of range")
        return (y * CANVAS_WIDTH + x) * 3

    def get_pixel(self, x: int, y: int) -> Tuple[int, int, int]:
        """Return (r, g, b) at the given coordinates."""
        off = self._offset(x, y)
        return (self._data[off], self._data[off + 1], self._data[off + 2])

    def set_pixel(self, x: int, y: int, r: int, g: int, b: int):
        """Set pixel colour at the given coordinates."""
        off = self._offset(x, y)
        self._data[off] = r & 0xFF
        self._data[off + 1] = g & 0xFF
        self._data[off + 2] = b & 0xFF

    def merkle_root(self) -> bytes:
        """Compute the Merkle root of the current canvas state."""
        return canvas_merkle_root(self._data)

    def to_base64(self) -> str:
        """Serialise the full canvas to base64."""
        return base64.b64encode(self._data).decode("ascii")

    @classmethod
    def from_base64(cls, b64: str) -> "Canvas":
        """Deserialise a canvas from base64."""
        data = bytearray(base64.b64decode(b64))
        return cls(data)

    @property
    def raw(self) -> bytearray:
        return self._data

    def copy(self) -> "Canvas":
        return Canvas(copy.copy(self._data))
