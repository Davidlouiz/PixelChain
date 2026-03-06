"""Core data structures: Pixel and ClosureBlock."""

from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass, field
from typing import List, Optional


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _difficulty_target(difficulty_bits: int) -> int:
    """Return the maximum hash value (as int) for a given difficulty in bits.

    difficulty_bits = number of leading zero bits required.
    Target = 2^(256 - difficulty_bits) - 1
    """
    if difficulty_bits <= 0:
        return (1 << 256) - 1  # accept everything
    if difficulty_bits >= 256:
        return 0
    return (1 << (256 - difficulty_bits)) - 1


def hash_meets_difficulty(hash_bytes: bytes, difficulty_bits: int) -> bool:
    """Check if a hash (32 bytes) meets the required difficulty."""
    hash_int = int.from_bytes(hash_bytes, "big")
    return hash_int <= _difficulty_target(difficulty_bits)


# ---------------------------------------------------------------------------
# Pixel
# ---------------------------------------------------------------------------


@dataclass
class Pixel:
    """A single pixel placement with proof-of-work."""

    x: int  # 0..999
    y: int  # 0..999
    r: int  # 0..255
    g: int  # 0..255
    b: int  # 0..255
    epoch: str
    closure_hash: bytes  # hash of the previous epoch's closure block (32 bytes)
    prev_pixel_hash: Optional[
        bytes
    ]  # hash of previous pixel at same (x,y), None if first
    nonce: int = 0
    hash: bytes = field(default=b"\x00" * 32, repr=False)

    def serialise_for_hash(self) -> bytes:
        """Serialise all fields (except hash) for SHA-256 computation."""
        epoch_bytes = self.epoch.encode("ascii")
        parts = [
            struct.pack(">HH", self.x, self.y),  # 4 bytes
            struct.pack("BBB", self.r, self.g, self.b),  # 3 bytes
            struct.pack(">H", len(epoch_bytes)),  # 2 bytes length prefix
            epoch_bytes,  # variable
            self.closure_hash,  # 32 bytes
        ]
        if self.prev_pixel_hash is not None:
            parts.append(b"\x01")  # marker: has prev
            parts.append(self.prev_pixel_hash)  # 32 bytes
        else:
            parts.append(b"\x00")  # marker: no prev
        parts.append(struct.pack(">I", self.nonce))  # 4 bytes
        return b"".join(parts)

    def compute_hash(self) -> bytes:
        """Compute SHA-256 of the pixel data."""
        return _sha256(self.serialise_for_hash())

    def mine(self, difficulty_bits: int, cancel_event=None) -> bool:
        """Mine until a valid nonce is found. Returns True if found, False if cancelled."""
        self.nonce = 0
        while True:
            if cancel_event is not None and cancel_event.is_set():
                return False
            self.hash = self.compute_hash()
            if hash_meets_difficulty(self.hash, difficulty_bits):
                return True
            self.nonce += 1
            if self.nonce > 0xFFFFFFFF:
                self.nonce = 0  # wrap around (in practice we'd adjust other fields)

    def verify(self, difficulty_bits: int) -> bool:
        """Verify the pixel's proof-of-work."""
        expected = self.compute_hash()
        return expected == self.hash and hash_meets_difficulty(
            self.hash, difficulty_bits
        )

    @property
    def color_hex(self) -> str:
        return f"{self.r:02X}{self.g:02X}{self.b:02X}"

    @color_hex.setter
    def color_hex(self, value: str):
        self.r = int(value[0:2], 16)
        self.g = int(value[2:4], 16)
        self.b = int(value[4:6], 16)

    def pow_work(self) -> int:
        """Estimated work represented by this pixel's hash.

        Work = 2^256 / (hash_value + 1).
        """
        hash_int = int.from_bytes(self.hash, "big")
        return (1 << 256) // (hash_int + 1)

    def to_dict(self) -> dict:
        d = {
            "type": "pixel",
            "x": self.x,
            "y": self.y,
            "color": self.color_hex,
            "epoch": self.epoch,
            "closure_hash": self.closure_hash.hex(),
            "nonce": self.nonce,
            "hash": self.hash.hex(),
        }
        if self.prev_pixel_hash is not None:
            d["prev_pixel_hash"] = self.prev_pixel_hash.hex()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Pixel":
        color = d["color"]
        r, g, b = int(color[0:2], 16), int(color[2:4], 16), int(color[4:6], 16)
        prev_hash = (
            bytes.fromhex(d["prev_pixel_hash"])
            if "prev_pixel_hash" in d and d["prev_pixel_hash"]
            else None
        )
        px = cls(
            x=d["x"],
            y=d["y"],
            r=r,
            g=g,
            b=b,
            epoch=d["epoch"],
            closure_hash=bytes.fromhex(d["closure_hash"]),
            prev_pixel_hash=prev_hash,
            nonce=d["nonce"],
            hash=bytes.fromhex(d["hash"]),
        )
        return px


# ---------------------------------------------------------------------------
# Closure Block
# ---------------------------------------------------------------------------


@dataclass
class ClosureBlock:
    """Epoch closure block."""

    epoch: str
    prev_closure_hash: bytes  # 32 bytes (zero-bytes for epoch "a")
    merkle_root: bytes  # 32 bytes — Merkle root of the full canvas state
    total_work: int  # cumulative work since genesis
    pixel_hashes: List[str] = field(
        default_factory=list
    )  # sorted hex hashes of winning pixels
    nonce: int = 0
    hash: bytes = field(default=b"\x00" * 32, repr=False)
    timestamp: float = (
        0.0  # time this closure was created (unix ts), for difficulty calc
    )

    def serialise_for_hash(self) -> bytes:
        epoch_bytes = self.epoch.encode("ascii")
        parts = [
            self.prev_closure_hash,  # 32 bytes
            struct.pack(">H", len(epoch_bytes)),  # 2 bytes
            epoch_bytes,
            self.merkle_root,  # 32 bytes
            struct.pack(">Q", self.total_work),  # 8 bytes
            struct.pack(">I", len(self.pixel_hashes)),  # 4 bytes: count
        ]
        for h in self.pixel_hashes:
            parts.append(bytes.fromhex(h))  # 32 bytes each
        parts.append(struct.pack(">I", self.nonce))  # 4 bytes
        return b"".join(parts)

    def compute_hash(self) -> bytes:
        return _sha256(self.serialise_for_hash())

    def mine(self, difficulty_bits: int, cancel_event=None) -> bool:
        self.nonce = 0
        while True:
            if cancel_event is not None and cancel_event.is_set():
                return False
            self.hash = self.compute_hash()
            if hash_meets_difficulty(self.hash, difficulty_bits):
                return True
            self.nonce += 1
            if self.nonce > 0xFFFFFFFF:
                self.nonce = 0

    def verify(self, difficulty_bits: int) -> bool:
        expected = self.compute_hash()
        return expected == self.hash and hash_meets_difficulty(
            self.hash, difficulty_bits
        )

    def pow_work(self) -> int:
        hash_int = int.from_bytes(self.hash, "big")
        return (1 << 256) // (hash_int + 1)

    def to_dict(self) -> dict:
        return {
            "type": "closure",
            "epoch": self.epoch,
            "prev_closure_hash": self.prev_closure_hash.hex(),
            "merkle_root": self.merkle_root.hex(),
            "total_work": self.total_work,
            "pixel_hashes": self.pixel_hashes,
            "nonce": self.nonce,
            "hash": self.hash.hex(),
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ClosureBlock":
        return cls(
            epoch=d["epoch"],
            prev_closure_hash=bytes.fromhex(d["prev_closure_hash"]),
            merkle_root=bytes.fromhex(d["merkle_root"]),
            total_work=d["total_work"],
            pixel_hashes=d.get("pixel_hashes", []),
            nonce=d["nonce"],
            hash=bytes.fromhex(d["hash"]),
            timestamp=d.get("timestamp", 0.0),
        )
