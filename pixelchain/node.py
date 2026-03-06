"""Node — central orchestrator for the PixelChain peer.

Ties together the blockchain, P2P network, mining pool, and WebSocket UI.
Handles all protocol messages and bootstrap/sync logic.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from typing import Dict, List, Optional, Set

from .blockchain import Blockchain, GENESIS_CLOSURE_HASH
from .config import Config, PIXELS_PER_EPOCH, PROTOCOL_VERSION
from .epoch_util import epoch_to_index
from .mining import MiningPool, PoolEntry
from .models import ClosureBlock, Pixel
from .network import P2PNetwork, PeerInfo

logger = logging.getLogger(__name__)


class Node:
    """A PixelChain peer node."""

    def __init__(self, config: Config):
        self.config = config
        self.blockchain = Blockchain()
        self.network = P2PNetwork(
            bind_address=config.bind_address,
            port=config.tcp_port,
            max_outbound=config.max_outbound,
        )

        # Mining pool
        self.pool = MiningPool(
            get_epoch=lambda: self.blockchain.current_epoch,
            get_difficulty=lambda: self.blockchain.current_difficulty,
            get_closure_hash=self._get_current_closure_hash,
            get_prev_pixel_hash=self._get_prev_pixel_hash,
            on_mined=self._on_pixel_mined,
            get_canvas_color=lambda x, y: self.blockchain.canvas.get_pixel(x, y),
            num_workers=2,
        )

        # WebSocket connections (browser clients)
        self._ws_clients: Set = set()

        # Track pixel pool ids -> pixel hashes for confirmation
        self._pool_pixel_map: Dict[str, str] = {}  # pool_id -> hash_hex

        # Event loop reference (set in start())
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._save_handle = None  # debounced save timer

        # Closure mining state
        self._closure_cancel_event: Optional[threading.Event] = None
        self._closure_thread: Optional[threading.Thread] = None
        self._closure_mining_epoch: Optional[str] = None  # epoch being mined

        # Pending closures: when a gossiped closure can't be accepted yet
        # (missing pixels / Merkle mismatch), we buffer it and retry as
        # pixels arrive.  Key = epoch name, value = (ClosureBlock, raw msg dict, peer address)
        self._pending_closures: Dict[str, tuple] = {}

        # Set up callbacks
        self.blockchain._on_canvas_update = self._notify_canvas_update
        self.blockchain._on_epoch_change = self._notify_epoch_change
        self.blockchain._on_pixel_confirmed = self._on_blockchain_pixel_confirmed

        self.network.set_message_handler(self._handle_peer_message)
        self.network.set_need_peers_handler(self._discover_peers)
        self.network.set_handshake_info_provider(self._get_handshake_info)
        self.network.set_peer_change_handler(self._notify_peer_count)

    def _get_handshake_info(self):
        """Return (current_epoch, closure_work) for P2P handshake."""
        return self.blockchain.current_epoch, self.blockchain.closure_work

    # -------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------

    async def start(self):
        """Start the node."""
        self._loop = asyncio.get_running_loop()

        # Load persisted state if available
        if self.blockchain.load_state(self.config.data_dir):
            logger.info(
                "Resumed from saved state — epoch %s, %d closures",
                self.blockchain.current_epoch,
                len(self.blockchain.closures),
            )

        await self.network.start()
        self.pool.start()

        # Check if epoch closure is needed (e.g. after loading saved state)
        self._check_epoch_closure()

        # Bootstrap: connect to initial peers
        for addr in self.config.bootstrap_peers:
            asyncio.create_task(self.network.connect_to_peer(addr))

        logger.info(
            "Node started on port %d (WS: %d)",
            self.config.tcp_port,
            self.config.ws_port,
        )

    async def stop(self):
        """Stop the node."""
        self._cancel_closure_mining()
        self.pool.stop()
        await self.network.stop()
        # Save state on clean shutdown
        self._save()
        logger.info("Node stopped")

    def _save(self):
        """Persist blockchain state to disk."""
        try:
            self.blockchain.save_state(self.config.data_dir)
        except Exception:
            logger.exception("Failed to save blockchain state")

    def _schedule_save(self):
        """Schedule a debounced save (max once every 5s)."""
        if not hasattr(self, "_save_handle") or self._save_handle is None:
            pass  # no pending save
        else:
            return  # already scheduled

        def do_save():
            self._save_handle = None
            self._save()

        self._save_handle = self._loop.call_later(5.0, do_save)

    # -------------------------------------------------------------------
    # Mining callbacks
    # -------------------------------------------------------------------

    def _get_current_closure_hash(self) -> bytes:
        """Return the closure hash for the current epoch."""
        state = self.blockchain.epoch_states.get(self.blockchain.current_epoch)
        if state:
            return state.closure_hash
        return GENESIS_CLOSURE_HASH

    def _get_prev_pixel_hash(self, x: int, y: int) -> Optional[bytes]:
        """Return the hash of the current best pixel at (x, y) in the current epoch."""
        state = self.blockchain.epoch_states.get(self.blockchain.current_epoch)
        if state:
            best = state.coord_best.get((x, y))
            if best:
                return best.hash
        return None

    def _on_pixel_mined(self, entry: PoolEntry):
        """Called from mining thread — schedule blockchain work on the event loop."""
        pixel = entry.pixel
        if pixel is None:
            # Same-color short-circuit: pixel was never mined, just notify UI.
            pool_id = entry.id
            self._loop.call_soon_threadsafe(
                lambda pid=pool_id: self._loop.create_task(
                    self._ws_broadcast({"type": "pool_confirmed", "id": pid})
                )
            )
            return

        # IMPORTANT: Do NOT call accept_pixel here (mining thread).
        # All blockchain mutations must happen on the event loop thread
        # to avoid race conditions with incoming peer pixels.
        pool_id = entry.id
        self._loop.call_soon_threadsafe(
            lambda e=entry, p=pixel, pid=pool_id: self._loop.create_task(
                self._on_pixel_mined_on_loop(e, p, pid)
            )
        )

    async def _on_pixel_mined_on_loop(
        self, entry: PoolEntry, pixel: Pixel, pool_id: str
    ):
        """Process a mined pixel on the event loop (thread-safe).

        The pixel has already been mined by a worker thread.  We accept it
        into the blockchain and notify browsers / peers.

        We do NOT requeue on coord-competition loss: the pool deduplicates
        by coordinate, so only the latest user-intended colour is queued.
        If this pixel lost, its colour was already superseded by a newer
        pool entry at the same coord (which will chain on top and win).
        """
        # If the entry was already requeued (e.g. by requeue_all during epoch
        # change) while the mined callback was in flight, don't touch it.
        if entry.status != "mining":
            logger.debug(
                "Local pixel %s already requeued (status=%s), skipping",
                pool_id,
                entry.status,
            )
            return

        accepted = self.blockchain.accept_pixel(pixel)
        if accepted:
            hash_hex = pixel.hash.hex()
            self._pool_pixel_map[pool_id] = hash_hex
            entry.status = "confirmed"
            logger.info("Local pixel %s confirmed: hash=%s", pool_id, hash_hex[:16])
            # Notify browser FIRST (fast), then broadcast to peers (may block)
            await self._ws_broadcast({"type": "pool_confirmed", "id": pool_id})
            await self.network.broadcast(pixel.to_dict())
        else:
            # Pixel rejected — likely epoch changed or epoch full.
            # If the pixel was mined for a stale epoch, requeue it so it
            # gets re-mined with the correct epoch/closure_hash.
            epoch_changed = pixel.epoch != self.blockchain.current_epoch
            epoch_full = False
            if not epoch_changed:
                state = self.blockchain.epoch_states.get(self.blockchain.current_epoch)
                if state and state.pixel_count >= PIXELS_PER_EPOCH - 1:
                    epoch_full = True

            if epoch_changed or epoch_full:
                logger.info(
                    "Local pixel %s rejected (%s), requeuing",
                    pool_id,
                    "epoch changed" if epoch_changed else "epoch full",
                )
                entry.status = "pending"
                entry.cancel_event = threading.Event()
                entry.pixel = None
                with self.pool._lock:
                    if entry.id not in self.pool._queue:
                        self.pool._queue.append(entry.id)
                self.pool._work_available.set()
            else:
                # Truly rejected (duplicate, invalid, etc.) — drop it.
                entry.status = "confirmed"
                logger.debug("Local pixel %s rejected, dropping", pool_id)
                await self._ws_broadcast({"type": "pool_confirmed", "id": pool_id})

    def _on_blockchain_pixel_confirmed(self, pixel: Pixel):
        """Called when any pixel is confirmed on the blockchain."""
        # Check if it's one of our pool pixels
        hash_hex = pixel.hash.hex()
        for pool_id, phash in list(self._pool_pixel_map.items()):
            if phash == hash_hex:
                self.pool.confirm_pixel(pool_id)
        # Schedule periodic save
        self._schedule_save()
        # Try to apply any pending closure (pixels may have been missing)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                lambda: self._loop.create_task(self._try_pending_closure())
            )
        # Check if epoch is ready for closure
        self._check_epoch_closure()

    # -------------------------------------------------------------------
    # Closure block mining
    # -------------------------------------------------------------------

    def _check_epoch_closure(self):
        """Check if the current epoch has enough pixels to mine a closure block."""
        state = self.blockchain.epoch_states.get(self.blockchain.current_epoch)
        if state is None:
            return
        if state.closure is not None:
            return  # already closed
        if state.pixel_count < PIXELS_PER_EPOCH - 1:
            return  # not enough pixels yet
        # Already mining closure for this epoch?
        if self._closure_mining_epoch == self.blockchain.current_epoch:
            return

        logger.info(
            "Epoch %s has %d/%d pixels — starting closure mining",
            self.blockchain.current_epoch,
            state.pixel_count,
            PIXELS_PER_EPOCH,
        )
        self._start_closure_mining()

    def _start_closure_mining(self):
        """Start mining a closure block in a background thread."""
        # Cancel any previous closure mining
        self._cancel_closure_mining()

        epoch = self.blockchain.current_epoch
        state = self.blockchain.epoch_states.get(epoch)
        if state is None:
            return

        self._closure_cancel_event = threading.Event()
        self._closure_mining_epoch = epoch

        # Build the closure block
        closure = ClosureBlock(
            epoch=epoch,
            prev_closure_hash=state.closure_hash,
            merkle_root=self.blockchain.canvas.merkle_root(),
            total_work=self.blockchain.total_work,
            pixel_hashes=self.blockchain.get_epoch_pixel_hashes(epoch),
        )

        difficulty = int(state.difficulty)
        epoch_difficulty = state.difficulty
        epoch_start_time = state.start_time
        cancel_event = self._closure_cancel_event

        def mine_closure():
            logger.info(
                "Mining closure block for epoch %s (difficulty %d bits)",
                epoch,
                difficulty,
            )
            success = closure.mine(difficulty, cancel_event=cancel_event)
            if success and not cancel_event.is_set():
                closure.timestamp = time.time()
                # Compute and store difficulty for the next epoch
                from .difficulty import compute_new_difficulty
                actual_duration = closure.timestamp - epoch_start_time
                closure.next_difficulty = compute_new_difficulty(
                    epoch_difficulty, actual_duration
                )
                logger.info(
                    "Closure block mined for epoch %s: hash=%s",
                    epoch,
                    closure.hash.hex()[:16],
                )
                # Schedule acceptance on the event loop
                self._loop.call_soon_threadsafe(
                    lambda c=closure: self._loop.create_task(
                        self._on_closure_mined_on_loop(c)
                    )
                )
            else:
                logger.debug("Closure mining cancelled for epoch %s", epoch)

        self._closure_thread = threading.Thread(
            target=mine_closure, name=f"closure-miner-{epoch}", daemon=True
        )
        self._closure_thread.start()

    def _cancel_closure_mining(self):
        """Cancel any in-progress closure mining."""
        if self._closure_cancel_event is not None:
            self._closure_cancel_event.set()
            self._closure_cancel_event = None
        self._closure_mining_epoch = None
        # Don't join — the thread is a daemon and will exit on its own
        self._closure_thread = None

    async def _on_closure_mined_on_loop(self, closure: ClosureBlock):
        """Process a locally mined closure block on the event loop."""
        accepted = self.blockchain.accept_closure(closure)
        if accepted:
            logger.info(
                "Local closure for epoch %s accepted — advancing to %s",
                closure.epoch,
                self.blockchain.current_epoch,
            )
            self._closure_mining_epoch = None
            # Requeue pool pixels for new epoch
            requeued = self.pool.requeue_all()
            for rid in requeued:
                await self._ws_broadcast({"type": "pool_requeued", "id": rid})
            # Broadcast closure to peers
            await self.network.broadcast(closure.to_dict())
            # Save state
            self._schedule_save()
        else:
            logger.warning(
                "Local closure for epoch %s rejected (stale?)", closure.epoch
            )

    # -------------------------------------------------------------------
    # P2P message handling
    # -------------------------------------------------------------------

    async def _handle_peer_message(self, peer: PeerInfo, msg: dict):
        """Handle an incoming message from a peer."""
        msg_type = msg.get("type")
        if msg_type is None:
            return

        handlers = {
            "hello": self._handle_hello,
            "pixel": self._handle_pixel,
            "closure": self._handle_closure,
            "get_closures": self._handle_get_closures,
            "closures": self._handle_closures,
            "get_canvas": self._handle_get_canvas,
            "canvas": self._handle_canvas,
            "get_epoch_pixels": self._handle_get_epoch_pixels,
            "epoch_pixels": self._handle_epoch_pixels,
            "get_epoch_sync": self._handle_get_epoch_sync,
            "epoch_sync": self._handle_epoch_sync,
            "get_peers": self._handle_get_peers,
            "peers": self._handle_peers,
            "reject": self._handle_reject,
        }

        handler = handlers.get(msg_type)
        if handler:
            await handler(peer, msg)
        else:
            logger.debug("Unknown message type '%s' from %s", msg_type, peer.address)

    async def _handle_hello(self, peer: PeerInfo, msg: dict):
        """Handle handshake from a peer."""
        version = msg.get("version", "")
        if version != PROTOCOL_VERSION:
            logger.warning("Peer %s has incompatible version %s", peer.address, version)

        peer.best_closure = msg.get("best_closure", "a")
        peer.best_work = msg.get("best_work", 0)
        peer.handshake_done = True

        # Always sync with peer — both sides may have unique pixels
        # (e.g. after a network partition)
        my_work = self.blockchain.closure_work
        if peer.best_work != my_work:
            logger.info(
                "Peer %s work differs (%d vs %d), syncing",
                peer.address,
                peer.best_work,
                my_work,
            )
        else:
            logger.info("Peer %s same work (%d), syncing anyway", peer.address, my_work)

        # Request closures
        await self.network.send_to_peer(
            peer.address,
            {
                "type": "get_closures",
                "from_epoch": "a",
            },
        )

        # If we already have closures (i.e. we're not behind),
        # also request current epoch pixels immediately.
        # Otherwise, _handle_closures will request canvas + pixels
        # after catching up.
        if peer.best_work <= my_work:
            peer_epoch = peer.best_closure if peer.best_closure else "a"
            await self.network.send_to_peer(
                peer.address,
                {
                    "type": "get_epoch_pixels",
                    "epoch": peer_epoch,
                },
            )

        # Store listen_port if present for peer discovery
        listen_port = msg.get("listen_port")
        if listen_port and peer.inbound:
            host = peer.address.rsplit(":", 1)[0]
            canonical = f"{host}:{listen_port}"
            self.network.known_addresses.add(canonical)

    async def _handle_pixel(self, peer: PeerInfo, msg: dict):
        """Handle a gossipped pixel."""
        hash_hex = msg.get("hash", "")
        if self.network.is_seen(hash_hex) or self.blockchain.has_seen(hash_hex):
            return

        self.network._mark_seen(hash_hex)

        try:
            pixel = Pixel.from_dict(msg)
        except Exception as e:
            logger.debug("Invalid pixel from %s: %s", peer.address, e)
            peer.trust_score -= 1
            return

        accepted = self.blockchain.accept_pixel(pixel)
        if accepted:
            # Relay to other peers
            await self.network.broadcast(msg, exclude=peer.address)
        else:
            peer.trust_score -= 1

    async def _handle_closure(self, peer: PeerInfo, msg: dict):
        """Handle a gossipped closure block."""
        hash_hex = msg.get("hash", "")
        if self.network.is_seen(hash_hex):
            return

        self.network._mark_seen(hash_hex)

        try:
            closure = ClosureBlock.from_dict(msg)
        except Exception as e:
            logger.debug("Invalid closure from %s: %s", peer.address, e)
            peer.trust_score -= 1
            return

        # --- Fork detection via gossip ---
        # If we already have a *different* closure for this epoch,
        # the peer is on a divergent chain.  Request their full chain
        # so that _handle_closures can compare weights and reorg if
        # the peer's branch is heavier.
        our_closure = self.blockchain.closures.get(closure.epoch)
        if our_closure and our_closure.hash != closure.hash:
            logger.info(
                "Fork detected (gossip) at epoch %s from %s — "
                "requesting full chain for comparison",
                closure.epoch,
                peer.address,
            )
            await self.network.send_to_peer(
                peer.address,
                {"type": "get_closures", "from_epoch": "a"},
            )
            return

        accepted = self.blockchain.accept_closure(closure)
        if accepted:
            await self._on_closure_accepted(closure, msg, peer.address)
        else:
            # Buffer the closure — pixels may still be in flight.
            # Only buffer for the current epoch (don't buffer stale ones).
            if closure.epoch == self.blockchain.current_epoch:
                # Use pixel_hashes to identify exactly what we're missing
                state = self.blockchain.epoch_states.get(closure.epoch)
                missing = []
                if state and closure.pixel_hashes:
                    missing = [h for h in closure.pixel_hashes if h not in state.pixels]
                logger.info(
                    "Buffering closure for epoch %s from %s "
                    "(%d/%d winning pixels missing, %d epoch pixels so far)",
                    closure.epoch,
                    peer.address,
                    len(missing),
                    len(closure.pixel_hashes),
                    state.pixel_count if state else 0,
                )
                self._pending_closures[closure.epoch] = (closure, msg, peer.address)
                # Request all epoch pixels (includes missing winners + their parents)
                await self.network.send_to_peer(
                    peer.address,
                    {
                        "type": "get_epoch_pixels",
                        "epoch": closure.epoch,
                    },
                )
            elif epoch_to_index(closure.epoch) > epoch_to_index(self.blockchain.current_epoch):
                # Peer is ahead — we're missing closures for earlier epochs.
                # Request the full chain so _handle_closures can catch us up.
                logger.info(
                    "Peer %s is ahead (closure epoch %s, we are at %s) "
                    "— requesting full chain",
                    peer.address,
                    closure.epoch,
                    self.blockchain.current_epoch,
                )
                await self.network.send_to_peer(
                    peer.address,
                    {"type": "get_closures", "from_epoch": "a"},
                )
            else:
                logger.debug(
                    "Rejected closure for epoch %s (current=%s)",
                    closure.epoch,
                    self.blockchain.current_epoch,
                )

    async def _on_closure_accepted(
        self, closure: ClosureBlock, msg: dict, exclude_addr: str
    ):
        """Common logic after a closure is successfully accepted."""
        # Cancel our own closure mining if we were mining for this epoch
        if self._closure_mining_epoch == closure.epoch:
            logger.info(
                "Peer closure for epoch %s accepted — cancelling local mining",
                closure.epoch,
            )
            self._cancel_closure_mining()
        # Remove from pending buffer
        self._pending_closures.pop(closure.epoch, None)
        # Requeue pool pixels for new epoch
        requeued = self.pool.requeue_all()
        for rid in requeued:
            await self._ws_broadcast({"type": "pool_requeued", "id": rid})
        # Relay
        await self.network.broadcast(msg, exclude=exclude_addr)
        # Save state
        self._schedule_save()

    async def _try_pending_closure(self):
        """Try to apply a pending closure after a new pixel was accepted.

        Uses the closure's *pixel_hashes* for a fast completeness check:
        we only call the (expensive) accept_closure when all winning pixels
        are present AND the epoch has enough total pixels.
        """
        epoch = self.blockchain.current_epoch
        pending = self._pending_closures.get(epoch)
        if not pending:
            return

        closure, msg, peer_addr = pending

        # Fast check: do we have all winning pixels?
        if closure.pixel_hashes:
            state = self.blockchain.epoch_states.get(epoch)
            if state:
                if state.pixel_count < PIXELS_PER_EPOCH - 1:
                    return  # not enough pixels yet
                for h in closure.pixel_hashes:
                    if h not in state.pixels:
                        return  # still missing at least one winning pixel

        accepted = self.blockchain.accept_closure(closure)
        if accepted:
            logger.info("Pending closure for epoch %s now accepted!", closure.epoch)
            await self._on_closure_accepted(closure, msg, peer_addr)

    async def _handle_get_closures(self, peer: PeerInfo, msg: dict):
        """Handle a request for closure blocks."""
        from_epoch = msg.get("from_epoch", "a")
        closures = self.blockchain.get_closures_from(from_epoch)
        await self.network.send_to_peer(
            peer.address,
            {
                "type": "closures",
                "closures": [c.to_dict() for c in closures],
                "closure_work": self.blockchain.closure_work,
            },
        )

    async def _handle_closures(self, peer: PeerInfo, msg: dict):
        """Handle received closure blocks (sync response).

        Closures are accepted in *sync_mode* (skipping pixel-count and
        Merkle-root checks) because the canvas hasn't been downloaded yet.
        After accepting, we request the canvas and current-epoch pixels.

        If a fork is detected (different closure hash for the same epoch),
        the heavier chain wins.  On a tie, the chain whose first diverging
        closure has the lexicographically lower hash wins.
        """
        closures_data = msg.get("closures", [])
        incoming: List[ClosureBlock] = []
        for cd in closures_data:
            try:
                incoming.append(ClosureBlock.from_dict(cd))
            except Exception as e:
                logger.debug("Invalid closure in sync: %s", e)

        if not incoming:
            return

        # ---- Fork detection ----
        fork_epoch = None
        for closure in incoming:
            our = self.blockchain.closures.get(closure.epoch)
            if our and our.hash != closure.hash:
                fork_epoch = closure.epoch
                break

        if fork_epoch:
            # Compare closure-only work — this is deterministic and consistent
            # even after a reset+replay (unlike total_work which includes pixel PoW).
            peer_work = msg.get("closure_work",
                               msg.get("total_work",
                                        getattr(peer, "best_work", 0)))
            our_work = self.blockchain.closure_work

            should_reorg = False
            if peer_work > our_work:
                should_reorg = True
            elif peer_work == our_work:
                # Deterministic tie-break: lower first-diverging closure hash wins
                their_closure = next(
                    c for c in incoming if c.epoch == fork_epoch
                )
                our_closure = self.blockchain.closures[fork_epoch]
                should_reorg = their_closure.hash < our_closure.hash

            if should_reorg:
                logger.info(
                    "Reorg: fork at epoch %s — switching to peer %s chain "
                    "(peer_work=%d, our_work=%d)",
                    fork_epoch,
                    peer.address,
                    peer_work,
                    our_work,
                )
                self._cancel_closure_mining()
                self._pending_closures.clear()
                self.blockchain.reset()

                for closure in incoming:
                    self.blockchain.accept_closure(closure, sync_mode=True)

                # Download canvas + current-epoch pixels
                await self.network.send_to_peer(
                    peer.address, {"type": "get_canvas"}
                )
                await self.network.send_to_peer(
                    peer.address,
                    {
                        "type": "get_epoch_pixels",
                        "epoch": self.blockchain.current_epoch,
                    },
                )
                self._schedule_save()
            else:
                logger.info(
                    "Fork at epoch %s — keeping our chain "
                    "(our_work=%d >= peer_work=%d)",
                    fork_epoch,
                    our_work,
                    peer_work,
                )
            return

        # ---- No fork: accept any new closures (peer may be ahead) ----
        accepted = 0
        for closure in incoming:
            if closure.epoch not in self.blockchain.closures:
                if self.blockchain.accept_closure(closure, sync_mode=True):
                    accepted += 1

        if accepted:
            logger.info(
                "Sync: accepted %d closures from %s, now on epoch %s",
                accepted,
                peer.address,
                self.blockchain.current_epoch,
            )
            # Download the canvas that corresponds to the tip of the chain
            await self.network.send_to_peer(peer.address, {"type": "get_canvas"})
            # Then request current-epoch pixels
            await self.network.send_to_peer(
                peer.address,
                {
                    "type": "get_epoch_pixels",
                    "epoch": self.blockchain.current_epoch,
                },
            )

    async def _handle_get_canvas(self, peer: PeerInfo, msg: dict):
        """Handle a canvas download request."""
        await self.network.send_to_peer(
            peer.address,
            {
                "type": "canvas",
                "epoch": self.blockchain.current_epoch,
                "pixels": self.blockchain.canvas.to_base64(),
            },
        )

    async def _handle_canvas(self, peer: PeerInfo, msg: dict):
        """Handle received canvas data."""
        from .canvas import Canvas

        try:
            canvas = Canvas.from_base64(msg.get("pixels", ""))
            self.blockchain.canvas = canvas
            logger.info("Canvas loaded from peer %s", peer.address)
            # Notify WS clients about the new canvas
            await self._ws_broadcast(
                {"type": "canvas_init", "canvas": self.blockchain.canvas.to_list()}
            )
            # Check if we need to mine a closure for the current epoch
            self._check_epoch_closure()
        except Exception as e:
            logger.debug("Invalid canvas from %s: %s", peer.address, e)

    async def _handle_get_epoch_pixels(self, peer: PeerInfo, msg: dict):
        """Handle request for all pixels in an epoch."""
        epoch = msg.get("epoch", "")
        pixels = self.blockchain.get_epoch_pixels(epoch)
        await self.network.send_to_peer(
            peer.address,
            {
                "type": "epoch_pixels",
                "epoch": epoch,
                "pixels": [p.to_dict() for p in pixels],
            },
        )

    async def _handle_epoch_pixels(self, peer: PeerInfo, msg: dict):
        """Handle received epoch pixels (sync)."""
        pixels_data = msg.get("pixels", [])
        accepted = 0
        for pd in pixels_data:
            try:
                pixel = Pixel.from_dict(pd)
                if self.blockchain.accept_pixel(pixel):
                    accepted += 1
            except Exception as e:
                logger.debug("Invalid pixel in epoch sync: %s", e)
        if accepted:
            logger.info("Epoch pixel sync: accepted %d pixels", accepted)
            # Some of these pixels may unblock a pending closure
            await self._try_pending_closure()

    async def _handle_get_epoch_sync(self, peer: PeerInfo, msg: dict):
        """Handle sync request — send pixels not in known_hashes."""
        epoch = msg.get("epoch", "")
        known = msg.get("known_hashes", [])
        missing = self.blockchain.get_missing_pixels(epoch, known)
        await self.network.send_to_peer(
            peer.address,
            {
                "type": "epoch_sync",
                "epoch": epoch,
                "missing_pixels": [p.to_dict() for p in missing],
            },
        )

    async def _handle_epoch_sync(self, peer: PeerInfo, msg: dict):
        """Handle sync response — accept missing pixels."""
        for pd in msg.get("missing_pixels", []):
            try:
                pixel = Pixel.from_dict(pd)
                self.blockchain.accept_pixel(pixel)
            except Exception:
                pass

    async def _handle_get_peers(self, peer: PeerInfo, msg: dict):
        """Handle peer discovery request."""
        addresses = self.network.get_peer_addresses()
        await self.network.send_to_peer(
            peer.address,
            {
                "type": "peers",
                "peers": addresses,
            },
        )

    async def _handle_peers(self, peer: PeerInfo, msg: dict):
        """Handle received peer list."""
        for addr in msg.get("peers", []):
            if addr and isinstance(addr, str):
                self.network.known_addresses.add(addr)

    async def _handle_reject(self, peer: PeerInfo, msg: dict):
        """Handle a reject message (informational only)."""
        logger.debug(
            "Reject from %s: reason=%s hash=%s",
            peer.address,
            msg.get("reason"),
            msg.get("hash"),
        )

    # -------------------------------------------------------------------
    # Peer discovery
    # -------------------------------------------------------------------

    async def _discover_peers(self):
        """Attempt to find more peers when running low."""
        # Ask connected peers for their peer lists
        for addr in list(self.network.peers.keys()):
            await self.network.send_to_peer(addr, {"type": "get_peers"})

        # Try bootstrap peers
        for addr in self.config.bootstrap_peers:
            if addr not in self.network.peers:
                asyncio.create_task(self.network.connect_to_peer(addr))

    # -------------------------------------------------------------------
    # WebSocket interface (browser)
    # -------------------------------------------------------------------

    def register_ws_client(self, ws):
        self._ws_clients.add(ws)

    def unregister_ws_client(self, ws):
        self._ws_clients.discard(ws)

    async def _ws_broadcast(self, msg: dict):
        """Send a message to all connected WebSocket clients."""
        if not self._ws_clients:
            return
        data = json.dumps(msg)
        disconnected = set()
        for ws in list(self._ws_clients):
            try:
                await ws.send(data)
            except Exception:
                disconnected.add(ws)
        self._ws_clients -= disconnected

    async def handle_ws_message(self, ws, msg_str: str):
        """Handle a message from the browser UI."""
        try:
            msg = json.loads(msg_str)
        except json.JSONDecodeError:
            return

        msg_type = msg.get("type")

        if msg_type == "add_pixel":
            pixel_id = msg.get("id")
            x = msg.get("x", 0)
            y = msg.get("y", 0)
            color = msg.get("color", "000000")
            r = int(color[0:2], 16)
            g = int(color[2:4], 16)
            b = int(color[4:6], 16)
            # Skip if the canvas already has this exact colour — no point mining.
            cr, cg, cb = self.blockchain.canvas.get_pixel(x, y)
            if (cr, cg, cb) == (r, g, b):
                await self._ws_broadcast({"type": "pool_confirmed", "id": pixel_id})
                return
            self.pool.add_pixel(pixel_id, x, y, r, g, b)

        elif msg_type == "cancel_pixel":
            pixel_id = msg.get("id")
            self.pool.cancel_pixel(pixel_id)

        elif msg_type == "clear_pool":
            self.pool.clear_all()

    async def send_ws_init(self, ws):
        """Send initial state to a newly connected WebSocket client."""
        # Canvas
        await ws.send(
            json.dumps(
                {
                    "type": "canvas_init",
                    "pixels": self.blockchain.canvas.to_base64(),
                }
            )
        )

        # Pool
        await ws.send(
            json.dumps(
                {
                    "type": "pool_init",
                    "pixels": self.pool.get_pool_state(),
                }
            )
        )

        # Current epoch info
        state = self.blockchain.get_current_state()
        await ws.send(
            json.dumps(
                {
                    "type": "epoch_update",
                    "epoch": state["epoch"],
                    "difficulty": state["difficulty"],
                    "peers": len(self.network.peers),
                }
            )
        )

    # -------------------------------------------------------------------
    # UI notification helpers
    # -------------------------------------------------------------------

    def _notify_canvas_update(self, x: int, y: int, r: int, g: int, b: int):
        color = f"{r:02X}{g:02X}{b:02X}"
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                lambda: self._loop.create_task(
                    self._ws_broadcast(
                        {
                            "type": "canvas_update",
                            "x": x,
                            "y": y,
                            "color": color,
                        }
                    )
                )
            )

    def _notify_epoch_change(self, epoch: str, difficulty: float):
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                lambda: self._loop.create_task(
                    self._ws_broadcast(
                        {
                            "type": "epoch_update",
                            "epoch": epoch,
                            "difficulty": difficulty,
                            "peers": len(self.network.peers),
                        }
                    )
                )
            )

    def _notify_peer_count(self, count: int):
        """Called by the network layer when a peer connects or disconnects."""
        if self._loop is not None:
            state = self.blockchain.get_current_state()
            self._loop.call_soon_threadsafe(
                lambda: self._loop.create_task(
                    self._ws_broadcast(
                        {
                            "type": "epoch_update",
                            "epoch": state["epoch"],
                            "difficulty": state["difficulty"],
                            "peers": count,
                        }
                    )
                )
            )

    def get_hello_msg(self) -> dict:
        """Build a hello message with current state."""
        return {
            "type": "hello",
            "version": PROTOCOL_VERSION,
            "best_closure": self.blockchain.current_epoch,
            "best_work": self.blockchain.closure_work,
            "listen_port": self.config.tcp_port,
        }
