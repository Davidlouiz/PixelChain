"""Node — central orchestrator for the PixelChain peer.

Ties together the blockchain, P2P network, mining pool, and WebSocket UI.
Handles all protocol messages and bootstrap/sync logic.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Set

from .blockchain import Blockchain, GENESIS_CLOSURE_HASH
from .config import Config, PIXELS_PER_EPOCH, PROTOCOL_VERSION
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
            num_workers=2,
        )

        # WebSocket connections (browser clients)
        self._ws_clients: Set = set()

        # Track pixel pool ids -> pixel hashes for confirmation
        self._pool_pixel_map: Dict[str, str] = {}  # pool_id -> hash_hex

        # Event loop reference (set in start())
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._save_handle = None  # debounced save timer

        # Set up callbacks
        self.blockchain._on_canvas_update = self._notify_canvas_update
        self.blockchain._on_epoch_change = self._notify_epoch_change
        self.blockchain._on_pixel_confirmed = self._on_blockchain_pixel_confirmed

        self.network.set_message_handler(self._handle_peer_message)
        self.network.set_need_peers_handler(self._discover_peers)
        self.network.set_handshake_info_provider(self._get_handshake_info)

    def _get_handshake_info(self):
        """Return (current_epoch, total_work) for P2P handshake."""
        return self.blockchain.current_epoch, self.blockchain.total_work

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
        """Process a mined pixel on the event loop (thread-safe)."""
        accepted = self.blockchain.accept_pixel(pixel)
        if accepted:
            hash_hex = pixel.hash.hex()
            self._pool_pixel_map[pool_id] = hash_hex
            entry.status = "confirmed"
            logger.info("Local pixel %s confirmed: hash=%s", pool_id, hash_hex[:16])
            # Broadcast to peers and notify browser
            await self.network.broadcast(pixel.to_dict())
            await self._ws_broadcast({"type": "pool_confirmed", "id": pool_id})
        else:
            # Requeue for mining (maybe epoch changed or conflict lost)
            entry.status = "pending"
            entry.cancel_event.clear()
            logger.debug("Local pixel %s rejected, requeuing", pool_id)

    def _on_blockchain_pixel_confirmed(self, pixel: Pixel):
        """Called when any pixel is confirmed on the blockchain."""
        # Check if it's one of our pool pixels
        hash_hex = pixel.hash.hex()
        for pool_id, phash in list(self._pool_pixel_map.items()):
            if phash == hash_hex:
                self.pool.confirm_pixel(pool_id)
        # Schedule periodic save
        self._schedule_save()

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
        my_work = self.blockchain.total_work
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
        # Request current epoch pixels from peer
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

        accepted = self.blockchain.accept_closure(closure)
        if accepted:
            # Requeue pool pixels for new epoch
            requeued = self.pool.requeue_all()
            for rid in requeued:
                await self._ws_broadcast({"type": "pool_requeued", "id": rid})
            # Relay
            await self.network.broadcast(msg, exclude=peer.address)
        else:
            peer.trust_score -= 1

    async def _handle_get_closures(self, peer: PeerInfo, msg: dict):
        """Handle a request for closure blocks."""
        from_epoch = msg.get("from_epoch", "a")
        closures = self.blockchain.get_closures_from(from_epoch)
        await self.network.send_to_peer(
            peer.address,
            {
                "type": "closures",
                "closures": [c.to_dict() for c in closures],
            },
        )

    async def _handle_closures(self, peer: PeerInfo, msg: dict):
        """Handle received closure blocks (sync response)."""
        closures_data = msg.get("closures", [])
        for cd in closures_data:
            try:
                closure = ClosureBlock.from_dict(cd)
                # For sync, we accept closures more leniently
                # In full implementation, we'd verify the entire chain
                if closure.epoch not in self.blockchain.closures:
                    self.blockchain.accept_closure(closure)
            except Exception as e:
                logger.debug("Invalid closure in sync: %s", e)

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
            # Verify Merkle root against last known closure
            # For now, accept if we're bootstrapping
            self.blockchain.canvas = canvas
            logger.info("Canvas loaded from peer %s", peer.address)
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
        for pd in pixels_data:
            try:
                pixel = Pixel.from_dict(pd)
                self.blockchain.accept_pixel(pixel)
            except Exception as e:
                logger.debug("Invalid pixel in epoch sync: %s", e)

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
        for ws in self._ws_clients:
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
            "best_work": self.blockchain.total_work,
            "listen_port": self.config.tcp_port,
        }
