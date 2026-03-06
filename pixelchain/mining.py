"""Pixel pool and mining engine.

The pool holds pixels requested by the local user (via the browser).
The miner runs in background threads, mining PoW for each queued pixel.
"""

from __future__ import annotations

import logging
import threading
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from .models import Pixel

logger = logging.getLogger(__name__)


@dataclass
class PoolEntry:
    """A pixel in the mining pool."""

    id: str  # UUID assigned by the browser
    x: int
    y: int
    r: int
    g: int
    b: int
    status: str = "pending"  # pending, mining, confirmed, cancelled
    cancel_event: threading.Event = field(default_factory=threading.Event)
    pixel: Optional[Pixel] = None  # filled once mining starts


class MiningPool:
    """Manages the local pixel pool and mining threads."""

    def __init__(
        self,
        get_epoch: Callable[[], str],
        get_difficulty: Callable[[], float],
        get_closure_hash: Callable[[], bytes],
        get_prev_pixel_hash: Callable[[int, int], Optional[bytes]],
        on_mined: Callable[[PoolEntry], None],
        num_workers: int = 2,
    ):
        self._entries: Dict[str, PoolEntry] = {}
        self._queue: List[str] = []  # entry IDs waiting to be mined
        self._lock = threading.Lock()
        self._get_epoch = get_epoch
        self._get_difficulty = get_difficulty
        self._get_closure_hash = get_closure_hash
        self._get_prev_pixel_hash = get_prev_pixel_hash
        self._on_mined = on_mined
        self._num_workers = num_workers
        self._workers: List[threading.Thread] = []
        self._stop_event = threading.Event()
        self._work_available = threading.Event()

    def start(self):
        """Start mining worker threads."""
        for i in range(self._num_workers):
            t = threading.Thread(
                target=self._worker_loop, name=f"miner-{i}", daemon=True
            )
            t.start()
            self._workers.append(t)
        logger.info("Started %d mining workers", self._num_workers)

    def stop(self):
        """Stop all mining workers."""
        self._stop_event.set()
        self._work_available.set()
        # Cancel all pending entries
        with self._lock:
            for entry in self._entries.values():
                entry.cancel_event.set()
        for t in self._workers:
            t.join(timeout=2)
        self._workers.clear()

    def add_pixel(
        self, pixel_id: str, x: int, y: int, r: int, g: int, b: int
    ) -> PoolEntry:
        """Add a pixel request to the pool."""
        entry = PoolEntry(id=pixel_id, x=x, y=y, r=r, g=g, b=b)
        with self._lock:
            self._entries[pixel_id] = entry
            self._queue.append(pixel_id)
        self._work_available.set()
        logger.info(
            "Added pixel %s to pool at (%d,%d) color %02X%02X%02X",
            pixel_id,
            x,
            y,
            r,
            g,
            b,
        )
        return entry

    def cancel_pixel(self, pixel_id: str) -> bool:
        """Cancel a pixel in the pool. Returns True if found."""
        with self._lock:
            entry = self._entries.get(pixel_id)
            if entry is None:
                return False
            entry.status = "cancelled"
            entry.cancel_event.set()
            if pixel_id in self._queue:
                self._queue.remove(pixel_id)
        logger.info("Cancelled pixel %s", pixel_id)
        return True

    def clear_all(self):
        """Cancel and remove all pixels from the pool."""
        with self._lock:
            for entry in self._entries.values():
                entry.cancel_event.set()
            self._entries.clear()
            self._queue.clear()
        logger.info("Cleared entire mining pool")

    def confirm_pixel(self, pixel_id: str):
        """Mark a pixel as confirmed (accepted into the blockchain)."""
        with self._lock:
            entry = self._entries.get(pixel_id)
            if entry:
                entry.status = "confirmed"

    def requeue_all(self):
        """Requeue all non-confirmed pixels for re-mining (after a reorg)."""
        with self._lock:
            requeued = []
            for eid, entry in self._entries.items():
                if entry.status in ("mining", "pending"):
                    entry.cancel_event.set()  # stop current mining
                    entry.cancel_event = threading.Event()  # fresh event
                    entry.status = "pending"
                    if eid not in self._queue:
                        self._queue.append(eid)
                    requeued.append(eid)
        if requeued:
            self._work_available.set()
            logger.info("Requeued %d pixels for re-mining", len(requeued))
        return requeued

    def get_pool_state(self) -> List[dict]:
        """Return current pool state for UI."""
        with self._lock:
            result = []
            for entry in self._entries.values():
                if entry.status in ("pending", "mining"):
                    result.append(
                        {
                            "id": entry.id,
                            "x": entry.x,
                            "y": entry.y,
                            "color": f"{entry.r:02X}{entry.g:02X}{entry.b:02X}",
                            "status": entry.status,
                        }
                    )
            return result

    def remove_confirmed(self):
        """Clean up confirmed/cancelled entries."""
        with self._lock:
            to_remove = [
                eid
                for eid, e in self._entries.items()
                if e.status in ("confirmed", "cancelled")
            ]
            for eid in to_remove:
                del self._entries[eid]

    def _worker_loop(self):
        """Mining worker loop."""
        while not self._stop_event.is_set():
            self._work_available.wait(timeout=1.0)
            if self._stop_event.is_set():
                break

            entry = self._get_next_entry()
            if entry is None:
                self._work_available.clear()
                continue

            self._mine_entry(entry)

    def _get_next_entry(self) -> Optional[PoolEntry]:
        """Pop the next entry to mine."""
        with self._lock:
            while self._queue:
                eid = self._queue.pop(0)
                entry = self._entries.get(eid)
                if entry and entry.status == "pending":
                    entry.status = "mining"
                    return entry
        return None

    def _mine_entry(self, entry: PoolEntry):
        """Mine a single pool entry."""
        epoch = self._get_epoch()
        difficulty = self._get_difficulty()
        closure_hash = self._get_closure_hash()
        prev_pixel_hash = self._get_prev_pixel_hash(entry.x, entry.y)

        pixel = Pixel(
            x=entry.x,
            y=entry.y,
            r=entry.r,
            g=entry.g,
            b=entry.b,
            epoch=epoch,
            closure_hash=closure_hash,
            prev_pixel_hash=prev_pixel_hash,
        )

        entry.pixel = pixel
        logger.debug(
            "Mining pixel %s at (%d,%d) epoch=%s diff=%.1f",
            entry.id,
            entry.x,
            entry.y,
            epoch,
            difficulty,
        )

        success = pixel.mine(int(difficulty), cancel_event=entry.cancel_event)

        if success and entry.status == "mining":
            logger.info("Mined pixel %s: hash=%s", entry.id, pixel.hash.hex()[:16])
            self._on_mined(entry)
        elif entry.status == "cancelled":
            logger.debug("Mining cancelled for pixel %s", entry.id)
        else:
            # Cancelled due to reorg or stop — requeue if still desired
            with self._lock:
                if entry.status == "mining" and not self._stop_event.is_set():
                    entry.status = "pending"
                    entry.cancel_event = threading.Event()
                    self._queue.append(entry.id)
                    self._work_available.set()
