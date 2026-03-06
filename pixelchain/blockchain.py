"""Blockchain state manager — epoch chain, pixel acceptance, reorganisation."""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from .canvas import Canvas
from .config import INITIAL_DIFFICULTY_BITS, PIXELS_PER_EPOCH
from .difficulty import compute_new_difficulty, hash_meets_target
from .epoch_util import epoch_to_index, index_to_epoch, next_epoch, prev_epoch
from .models import ClosureBlock, Pixel

logger = logging.getLogger(__name__)

GENESIS_CLOSURE_HASH = b"\x00" * 32  # sentinel for epoch "a"


@dataclass
class EpochState:
    """State of a single epoch."""

    epoch: str
    difficulty: float
    closure_hash: bytes  # hash of the previous epoch's closure block
    pixels: Dict[str, Pixel] = field(default_factory=dict)  # hash_hex -> Pixel
    # For each coordinate, track the current best pixel (heaviest PoW chain)
    coord_best: Dict[Tuple[int, int], Pixel] = field(default_factory=dict)
    # Per-coord cumulative work for conflict resolution
    coord_work: Dict[Tuple[int, int], int] = field(
        default_factory=lambda: defaultdict(int)
    )
    pixel_count: int = 0
    closure: Optional[ClosureBlock] = None
    start_time: float = field(default_factory=time.time)


class Blockchain:
    """Manages the full blockchain state: epochs, closures, canvas, and branch selection."""

    _MAX_ORPHANS = 2000  # hard cap to prevent DoS

    def __init__(self):
        # Chain of closure blocks indexed by epoch name
        self.closures: Dict[str, ClosureBlock] = {}
        # Epoch states (current + potentially some cached)
        self.epoch_states: Dict[str, EpochState] = {}
        # The canvas at the tip of the dominant branch
        self.canvas = Canvas()
        # Current epoch and difficulty
        self.current_epoch = "a"
        self.current_difficulty: float = INITIAL_DIFFICULTY_BITS
        # Total cumulative work on the dominant branch
        self.total_work: int = 0
        # Pixel hashes we've seen (for dedup)
        self._seen_hashes: Set[str] = set()
        # Orphan buffer: pixels whose prev_pixel_hash isn't known yet.
        # Key = prev_pixel_hash hex they're waiting for, value = list of pixels.
        self._orphans: Dict[str, List[Pixel]] = defaultdict(list)
        self._orphan_count: int = 0
        # Chain-work cache: hash_hex → cumulative work along prev_pixel_hash chain.
        # chain_work(P) = P.pow_work() + chain_work(parent) if parent else P.pow_work()
        # Deterministic: depends only on the DAG structure, not arrival order.
        self._chain_work: Dict[str, int] = {}
        # Callbacks for UI notification
        self._on_pixel_confirmed = None
        self._on_epoch_change = None
        self._on_canvas_update = None

        # Initialise the first epoch
        self._init_epoch("a", GENESIS_CLOSURE_HASH, INITIAL_DIFFICULTY_BITS)

    def _init_epoch(self, epoch: str, prev_closure_hash: bytes, difficulty: float):
        """Initialise a new epoch state."""
        state = EpochState(
            epoch=epoch,
            difficulty=difficulty,
            closure_hash=prev_closure_hash,
        )
        self.epoch_states[epoch] = state

    # -------------------------------------------------------------------
    # Pixel acceptance
    # -------------------------------------------------------------------

    def accept_pixel(self, pixel: Pixel) -> bool:
        """Validate and accept a pixel into the current epoch.

        Returns True if accepted, False if rejected.
        """
        # Basic validation
        if pixel.epoch != self.current_epoch:
            logger.debug(
                "Pixel epoch %s != current %s", pixel.epoch, self.current_epoch
            )
            return False

        state = self.epoch_states.get(self.current_epoch)
        if state is None:
            return False

        # Check closure hash matches
        if pixel.closure_hash != state.closure_hash:
            logger.debug("Pixel closure_hash mismatch")
            return False

        # Verify PoW
        if not hash_meets_target(pixel.hash, state.difficulty):
            logger.debug("Pixel PoW insufficient")
            return False

        # Verify hash computation
        if pixel.compute_hash() != pixel.hash:
            logger.debug("Pixel hash verification failed")
            return False

        hash_hex = pixel.hash.hex()
        if hash_hex in self._seen_hashes:
            return False  # duplicate

        # Check epoch not already closed
        if state.closure is not None:
            return False

        # Check epoch pixel count
        if state.pixel_count >= PIXELS_PER_EPOCH - 1:  # leave last slot for closure
            return False

        # Validate prev_pixel_hash
        if pixel.prev_pixel_hash is not None:
            parent_hex = pixel.prev_pixel_hash.hex()
            if parent_hex not in self._chain_work:
                # Parent not yet applied — buffer as orphan.
                # We check _chain_work (not _seen_hashes) because a pixel may
                # be "seen" but sitting in the orphan buffer itself.  We need
                # the parent's chain_work to be computed before the child.
                if self._orphan_count < self._MAX_ORPHANS:
                    self._orphans[parent_hex].append(pixel)
                    self._orphan_count += 1
                    self._seen_hashes.add(hash_hex)  # prevent re-buffering
                    logger.debug(
                        "Orphan pixel %s waiting for parent %s",
                        hash_hex[:16],
                        parent_hex[:16],
                    )
                    return True  # accepted (deferred)
                else:
                    logger.debug("Orphan buffer full, rejecting pixel")
                    return False

        # Accept the pixel (and release any orphans waiting for it)
        self._apply_pixel(pixel, state)
        self._release_orphans(hash_hex, state)
        return True

    def _apply_pixel(self, pixel: Pixel, state: EpochState):
        """Apply a validated pixel to the epoch state and canvas."""
        hash_hex = pixel.hash.hex()
        state.pixels[hash_hex] = pixel
        state.pixel_count += 1
        self._seen_hashes.add(hash_hex)

        coord = (pixel.x, pixel.y)
        pixel_work = pixel.pow_work()

        # Compute cumulative chain work by walking prev_pixel_hash links.
        # The orphan buffer guarantees the parent is always applied before
        # its children, so _chain_work[parent] is always available here.
        chain_work = pixel_work
        if pixel.prev_pixel_hash is not None:
            parent_hex = pixel.prev_pixel_hash.hex()
            chain_work += self._chain_work.get(parent_hex, 0)
        self._chain_work[hash_hex] = chain_work

        # Conflict resolution: highest cumulative chain work wins.
        # Drawing *on top* of an existing pixel accumulates PoW, so the
        # new pixel is virtually guaranteed to replace the old one.
        # This is deterministic and order-independent: chain_work depends
        # only on the DAG structure (prev_pixel_hash links), not on the
        # order in which pixels were received.
        existing_chain_work = state.coord_work[coord]

        if coord not in state.coord_best:
            replace = True
        elif chain_work > existing_chain_work:
            replace = True
        elif chain_work == existing_chain_work:
            # Deterministic tie-break: lower hash wins
            replace = pixel.hash < state.coord_best[coord].hash
        else:
            replace = False

        # Always count this pixel's individual work (order-independent total)
        self.total_work += pixel_work

        if replace:
            state.coord_best[coord] = pixel
            state.coord_work[coord] = chain_work
            # Update canvas
            self.canvas.set_pixel(pixel.x, pixel.y, pixel.r, pixel.g, pixel.b)
            # Notify
            if self._on_canvas_update:
                self._on_canvas_update(pixel.x, pixel.y, pixel.r, pixel.g, pixel.b)

        if self._on_pixel_confirmed:
            self._on_pixel_confirmed(pixel)

    def _release_orphans(self, parent_hex: str, state: EpochState):
        """Recursively release orphaned pixels whose parent has just been accepted."""
        waiting = self._orphans.pop(parent_hex, None)
        if not waiting:
            return
        for orphan in waiting:
            self._orphan_count -= 1
            # The orphan already passed all validation in accept_pixel and
            # was added to _seen_hashes.  Just apply it and recurse.
            self._apply_pixel(orphan, state)
            self._release_orphans(orphan.hash.hex(), state)

    # -------------------------------------------------------------------
    # Closure block acceptance
    # -------------------------------------------------------------------

    def accept_closure(self, closure: ClosureBlock) -> bool:
        """Validate and accept a closure block."""
        state = self.epoch_states.get(closure.epoch)
        if state is None:
            return False

        # Already closed?
        if state.closure is not None:
            return False

        # Check prev_closure_hash
        if closure.prev_closure_hash != state.closure_hash:
            logger.debug("Closure prev_closure_hash mismatch")
            return False

        # Check PoW
        if not hash_meets_target(closure.hash, state.difficulty):
            logger.debug("Closure PoW insufficient")
            return False

        if closure.compute_hash() != closure.hash:
            logger.debug("Closure hash verification failed")
            return False

        # Check that epoch has enough pixels (closure is the 65536th item)
        if state.pixel_count < PIXELS_PER_EPOCH - 1:
            logger.debug("Not enough pixels for closure: %d", state.pixel_count)
            return False

        # Verify Merkle root matches current canvas state
        expected_merkle = self.canvas.merkle_root()
        if closure.merkle_root != expected_merkle:
            logger.debug("Merkle root mismatch")
            return False

        # Accept
        state.closure = closure
        closure.timestamp = closure.timestamp or time.time()
        self.closures[closure.epoch] = closure
        self.total_work += closure.pow_work()

        logger.info("Epoch %s closed. Total work: %d", closure.epoch, self.total_work)

        # Advance to next epoch
        self._advance_epoch(closure)
        return True

    def _advance_epoch(self, closure: ClosureBlock):
        """Transition to the next epoch after a closure."""
        new_epoch = next_epoch(closure.epoch)
        state = self.epoch_states.get(closure.epoch)

        # Compute new difficulty based on actual epoch duration
        actual_duration = closure.timestamp - state.start_time
        new_difficulty = compute_new_difficulty(state.difficulty, actual_duration)

        self.current_epoch = new_epoch
        self.current_difficulty = new_difficulty

        self._init_epoch(new_epoch, closure.hash, new_difficulty)

        logger.info(
            "Advanced to epoch %s, difficulty %.2f bits", new_epoch, new_difficulty
        )

        if self._on_epoch_change:
            self._on_epoch_change(new_epoch, new_difficulty)

    # -------------------------------------------------------------------
    # Branch selection & reorg
    # -------------------------------------------------------------------

    def compute_branch_weight(self) -> int:
        """Compute total weight of the current branch."""
        weight = 0
        # Sum all closure PoW
        for closure in self.closures.values():
            weight += closure.pow_work()
        # Add current epoch pixel PoW
        state = self.epoch_states.get(self.current_epoch)
        if state:
            for pixel in state.pixels.values():
                weight += pixel.pow_work()
        return weight

    def should_reorg(self, other_work: int) -> bool:
        """Check if a competing branch with other_work is heavier."""
        return other_work > self.total_work

    def get_orphaned_pixels(self, epoch: str) -> List[Pixel]:
        """Get all pixels from an epoch that would need remining after a reorg."""
        state = self.epoch_states.get(epoch)
        if state is None:
            return []
        return list(state.pixels.values())

    # -------------------------------------------------------------------
    # State queries
    # -------------------------------------------------------------------

    def get_current_state(self) -> dict:
        """Return a summary of the current blockchain state."""
        state = self.epoch_states.get(self.current_epoch)
        return {
            "epoch": self.current_epoch,
            "difficulty": self.current_difficulty,
            "pixel_count": state.pixel_count if state else 0,
            "total_work": self.total_work,
            "num_closures": len(self.closures),
        }

    def has_seen(self, hash_hex: str) -> bool:
        return hash_hex in self._seen_hashes

    def get_closures_from(self, from_epoch: str) -> List[ClosureBlock]:
        """Return all closure blocks starting from the given epoch."""
        result = []
        idx = epoch_to_index(from_epoch)
        while True:
            ep = index_to_epoch(idx)
            if ep in self.closures:
                result.append(self.closures[ep])
                idx += 1
            else:
                break
        return result

    def get_epoch_pixels(self, epoch: str) -> List[Pixel]:
        """Return all pixels in a given epoch."""
        state = self.epoch_states.get(epoch)
        if state is None:
            return []
        return list(state.pixels.values())

    def get_missing_pixels(self, epoch: str, known_hashes: List[str]) -> List[Pixel]:
        """Return pixels in the epoch that are not in known_hashes."""
        state = self.epoch_states.get(epoch)
        if state is None:
            return []
        known_set = set(known_hashes)
        return [p for h, p in state.pixels.items() if h not in known_set]

    # -------------------------------------------------------------------
    # Persistence
    # -------------------------------------------------------------------

    def save_state(self, data_dir: str):
        """Save the full blockchain state to disk so it can be resumed."""
        os.makedirs(data_dir, exist_ok=True)
        chain_path = os.path.join(data_dir, "chain.jsonl")

        # Collect ordered closures, then pixels per epoch
        ordered_epochs: List[str] = []
        idx = 0
        while True:
            ep = index_to_epoch(idx)
            if ep in self.closures:
                ordered_epochs.append(ep)
                idx += 1
            else:
                break

        with open(chain_path, "w") as f:
            # Write closures and their epoch's pixels in order
            for ep in ordered_epochs:
                closure = self.closures[ep]
                state = self.epoch_states.get(ep)
                if state:
                    for pixel in state.pixels.values():
                        f.write(
                            json.dumps(pixel.to_dict(), separators=(",", ":")) + "\n"
                        )
                f.write(json.dumps(closure.to_dict(), separators=(",", ":")) + "\n")

            # Write current (open) epoch pixels
            state = self.epoch_states.get(self.current_epoch)
            if state:
                for pixel in state.pixels.values():
                    f.write(json.dumps(pixel.to_dict(), separators=(",", ":")) + "\n")

        logger.info(
            "Saved state to %s (%d closures, epoch %s with %d pixels)",
            chain_path,
            len(ordered_epochs),
            self.current_epoch,
            self.epoch_states[self.current_epoch].pixel_count
            if self.current_epoch in self.epoch_states
            else 0,
        )

    def load_state(self, data_dir: str) -> bool:
        """Load blockchain state from disk.  Returns True if state was loaded."""
        chain_path = os.path.join(data_dir, "chain.jsonl")
        if not os.path.exists(chain_path):
            return False

        pixels_loaded = 0
        closures_loaded = 0

        # Temporarily suppress callbacks during replay
        old_cu = self._on_canvas_update
        old_ec = self._on_epoch_change
        old_pc = self._on_pixel_confirmed
        self._on_canvas_update = None
        self._on_epoch_change = None
        self._on_pixel_confirmed = None

        try:
            with open(chain_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    d = json.loads(line)
                    if d.get("type") == "pixel":
                        pixel = Pixel.from_dict(d)
                        if self.accept_pixel(pixel):
                            pixels_loaded += 1
                        else:
                            logger.warning(
                                "Failed to replay pixel %s", d.get("hash", "?")[:16]
                            )
                    elif d.get("type") == "closure":
                        closure = ClosureBlock.from_dict(d)
                        if self.accept_closure(closure):
                            closures_loaded += 1
                        else:
                            logger.warning(
                                "Failed to replay closure for epoch %s",
                                d.get("epoch", "?"),
                            )
        finally:
            # Restore callbacks
            self._on_canvas_update = old_cu
            self._on_epoch_change = old_ec
            self._on_pixel_confirmed = old_pc

        logger.info(
            "Loaded state: %d pixels, %d closures → epoch %s (difficulty %.2f)",
            pixels_loaded,
            closures_loaded,
            self.current_epoch,
            self.current_difficulty,
        )
        return pixels_loaded > 0 or closures_loaded > 0
