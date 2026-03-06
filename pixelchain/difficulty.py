"""Difficulty calculation and proof-of-work utilities."""

from __future__ import annotations

from .config import (
    INITIAL_DIFFICULTY_BITS,
    MAX_DIFFICULTY_ADJUSTMENT,
    TARGET_EPOCH_DURATION,
)


def compute_new_difficulty(prev_difficulty_bits: float, actual_duration: float) -> float:
    """Compute the difficulty for the next epoch.

    new_difficulty = prev_difficulty × (target_time / actual_time)
    Clamped to [prev / 4, prev × 4].

    We use a float to represent fractional difficulty bits for smoother adjustment.
    The actual target is derived from the float at verification time.
    """
    if actual_duration <= 0:
        actual_duration = 1.0  # safety

    ratio = TARGET_EPOCH_DURATION / actual_duration
    # Clamp ratio to [1/4, 4]
    ratio = max(1.0 / MAX_DIFFICULTY_ADJUSTMENT, min(MAX_DIFFICULTY_ADJUSTMENT, ratio))

    new_diff = prev_difficulty_bits * ratio
    # Ensure minimum difficulty
    if new_diff < INITIAL_DIFFICULTY_BITS:
        new_diff = INITIAL_DIFFICULTY_BITS
    return new_diff


def difficulty_target(difficulty_bits: float) -> int:
    """Convert a (potentially fractional) difficulty into a 256-bit target value.

    We interpret difficulty_bits as the number of leading zero bits in the hash.
    For fractional bits, we interpolate: target = 2^(256 - floor(bits)) - 1
    scaled by the fractional part.
    """
    bits = max(0.0, min(255.0, difficulty_bits))
    int_bits = int(bits)
    # Simple: use integer bits for the target
    if int_bits >= 256:
        return 0
    if int_bits <= 0:
        return (1 << 256) - 1
    return (1 << (256 - int_bits)) - 1


def hash_meets_target(hash_bytes: bytes, difficulty_bits: float) -> bool:
    """Check whether a hash meets the given difficulty."""
    target = difficulty_target(difficulty_bits)
    hash_int = int.from_bytes(hash_bytes, 'big')
    return hash_int <= target
