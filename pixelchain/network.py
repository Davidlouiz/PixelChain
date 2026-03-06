"""Peer-to-peer network layer — JSON over TCP with gossip protocol."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional, Set, Tuple

from .config import (
    BAN_DURATIONS,
    DEFAULT_MAX_OUTBOUND,
    GOSSIP_WINDOW_SIZE,
    INITIAL_TRUST_SCORE,
    MAX_CONSECUTIVE_INVALID,
    MAX_MESSAGE_SIZE,
    MIN_PEERS,
    PROTOCOL_VERSION,
    RECONNECT_INTERVAL,
    TRUST_BAN_THRESHOLD,
    TRUST_PENALTY,
    TRUST_RECOVERY_PER_MINUTE,
)

logger = logging.getLogger(__name__)


@dataclass
class PeerInfo:
    """Tracking info for a connected peer."""

    address: str  # "ip:port"
    reader: Optional[asyncio.StreamReader] = None
    writer: Optional[asyncio.StreamWriter] = None
    trust_score: int = INITIAL_TRUST_SCORE
    consecutive_invalid: int = 0
    ban_count: int = 0
    banned_until: float = 0.0
    last_seen: float = field(default_factory=time.time)
    best_closure: str = ""
    best_work: int = 0
    handshake_done: bool = False
    inbound: bool = False


class P2PNetwork:
    """Manages TCP connections, gossip, and peer lifecycle."""

    def __init__(
        self,
        bind_address: str,
        port: int,
        max_outbound: int = DEFAULT_MAX_OUTBOUND,
    ):
        self.bind_address = bind_address
        self.port = port
        self.max_outbound = max_outbound

        # Connected peers
        self.peers: Dict[str, PeerInfo] = {}
        # Known peer addresses (for reconnection)
        self.known_addresses: Set[str] = set()
        # Recently seen message hashes (sliding window for dedup)
        self._seen_hashes: Deque[str] = deque(maxlen=GOSSIP_WINDOW_SIZE)
        self._seen_set: Set[str] = set()

        # Message handler callback: async (peer_info, message_dict) -> None
        self._message_handler: Optional[Callable] = None
        # Callback when peer count drops below minimum
        self._on_need_peers: Optional[Callable] = None
        # Callback to get current chain state for handshake
        self._get_handshake_info: Optional[Callable] = None

        self._server: Optional[asyncio.AbstractServer] = None
        self._tasks: List[asyncio.Task] = []
        self._running = False

    def set_message_handler(self, handler: Callable):
        self._message_handler = handler

    def set_need_peers_handler(self, handler: Callable):
        self._on_need_peers = handler

    def set_handshake_info_provider(self, provider: Callable):
        """Set callback that returns (current_epoch, total_work) for handshake."""
        self._get_handshake_info = provider

    # -------------------------------------------------------------------
    # Server lifecycle
    # -------------------------------------------------------------------

    async def start(self):
        """Start listening for inbound connections."""
        self._running = True
        self._server = await asyncio.start_server(
            self._handle_inbound,
            self.bind_address,
            self.port,
        )
        addr = self._server.sockets[0].getsockname()
        logger.info("P2P server listening on %s:%d", addr[0], addr[1])
        # Start maintenance loop
        self._tasks.append(asyncio.create_task(self._maintenance_loop()))

    async def stop(self):
        """Shut down the P2P layer."""
        self._running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for task in self._tasks:
            task.cancel()
        # Close all peer connections
        for peer in list(self.peers.values()):
            await self._disconnect_peer(peer)
        logger.info("P2P server stopped")

    # -------------------------------------------------------------------
    # Connection management
    # -------------------------------------------------------------------

    async def connect_to_peer(self, address: str) -> bool:
        """Initiate an outbound connection to a peer."""
        if address in self.peers:
            return True  # already connected
        if self._is_banned(address):
            return False

        self.known_addresses.add(address)

        try:
            host, port_str = address.rsplit(":", 1)
            port = int(port_str)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=10.0,
            )
        except Exception as e:
            logger.debug("Failed to connect to %s: %s", address, e)
            return False

        peer = PeerInfo(address=address, reader=reader, writer=writer, inbound=False)
        self.peers[address] = peer
        logger.info("Connected to peer %s", address)

        # Send handshake
        await self._send_handshake(peer)
        # Start reading from this peer
        self._tasks.append(asyncio.create_task(self._read_loop(peer)))
        return True

    async def _handle_inbound(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        """Handle a new inbound connection."""
        peername = writer.get_extra_info("peername")
        address = f"{peername[0]}:{peername[1]}" if peername else "unknown"
        logger.info("Inbound connection from %s", address)

        if self._is_banned(address):
            writer.close()
            return

        peer = PeerInfo(address=address, reader=reader, writer=writer, inbound=True)
        self.peers[address] = peer
        self.known_addresses.add(address)

        await self._send_handshake(peer)
        await self._read_loop(peer)

    async def _disconnect_peer(self, peer: PeerInfo, ban: bool = False):
        """Disconnect from a peer."""
        address = peer.address
        if peer.writer:
            try:
                peer.writer.close()
                await peer.writer.wait_closed()
            except Exception:
                pass
        self.peers.pop(address, None)
        if ban:
            dur = BAN_DURATIONS[min(peer.ban_count, len(BAN_DURATIONS) - 1)]
            peer.banned_until = time.time() + dur
            peer.ban_count += 1
            logger.warning("Banned peer %s for %ds", address, dur)
        else:
            logger.info("Disconnected from %s", address)

    def _is_banned(self, address: str) -> bool:
        # Check if address (or its base IP) is banned
        # We store ban info in known peers — simplified approach
        return False  # TODO: implement a proper ban registry

    # -------------------------------------------------------------------
    # Handshake
    # -------------------------------------------------------------------

    async def _send_handshake(self, peer: PeerInfo):
        """Send hello message to peer."""
        best_closure = ""
        best_work = 0
        if self._get_handshake_info:
            best_closure, best_work = self._get_handshake_info()
        msg = {
            "type": "hello",
            "version": PROTOCOL_VERSION,
            "best_closure": best_closure,
            "best_work": best_work,
            "listen_port": self.port,
        }
        await self._send_message(peer, msg)

    # -------------------------------------------------------------------
    # Message I/O
    # -------------------------------------------------------------------

    async def _read_loop(self, peer: PeerInfo):
        """Read messages from a peer until disconnection."""
        try:
            while self._running and peer.reader:
                try:
                    length_data = await asyncio.wait_for(
                        peer.reader.readexactly(4),
                        timeout=120.0,
                    )
                except asyncio.TimeoutError:
                    # Send a ping or just continue
                    continue
                except (asyncio.IncompleteReadError, ConnectionError):
                    break

                length = int.from_bytes(length_data, "big")
                if length > MAX_MESSAGE_SIZE:
                    logger.warning(
                        "Message too large from %s: %d bytes", peer.address, length
                    )
                    peer.consecutive_invalid += 1
                    if peer.consecutive_invalid >= MAX_CONSECUTIVE_INVALID:
                        await self._disconnect_peer(peer, ban=True)
                        return
                    continue

                try:
                    msg_data = await asyncio.wait_for(
                        peer.reader.readexactly(length),
                        timeout=30.0,
                    )
                except (
                    asyncio.IncompleteReadError,
                    asyncio.TimeoutError,
                    ConnectionError,
                ):
                    break

                try:
                    msg = json.loads(msg_data.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    peer.consecutive_invalid += 1
                    if peer.consecutive_invalid >= MAX_CONSECUTIVE_INVALID:
                        await self._disconnect_peer(peer, ban=True)
                        return
                    continue

                peer.last_seen = time.time()
                peer.consecutive_invalid = 0  # reset on valid JSON

                if self._message_handler:
                    try:
                        await self._message_handler(peer, msg)
                    except Exception as e:
                        logger.error(
                            "Handler error for message from %s: %s", peer.address, e
                        )

        except Exception as e:
            logger.debug("Read loop error for %s: %s", peer.address, e)
        finally:
            await self._disconnect_peer(peer)
            # Check if we need more peers
            outbound = sum(1 for p in self.peers.values() if not p.inbound)
            if outbound < MIN_PEERS and self._on_need_peers:
                asyncio.create_task(self._on_need_peers())

    async def _send_message(self, peer: PeerInfo, msg: dict):
        """Send a JSON message to a peer (length-prefixed)."""
        if peer.writer is None:
            return
        try:
            data = json.dumps(msg, separators=(",", ":")).encode("utf-8")
            length = len(data)
            peer.writer.write(length.to_bytes(4, "big") + data)
            await peer.writer.drain()
        except Exception as e:
            logger.debug("Send error to %s: %s", peer.address, e)
            await self._disconnect_peer(peer)

    async def broadcast(self, msg: dict, exclude: Optional[str] = None):
        """Broadcast a message to all connected peers (gossip)."""
        # Dedup via the hash field if present
        msg_hash = msg.get("hash", "")
        if msg_hash and msg_hash in self._seen_set:
            return
        if msg_hash:
            self._mark_seen(msg_hash)

        tasks = []
        for addr, peer in list(self.peers.items()):
            if addr == exclude:
                continue
            tasks.append(self._send_message(peer, msg))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def send_to_peer(self, address: str, msg: dict):
        """Send a message to a specific peer."""
        peer = self.peers.get(address)
        if peer:
            await self._send_message(peer, msg)

    def _mark_seen(self, hash_hex: str):
        """Add a hash to the seen window."""
        if hash_hex not in self._seen_set:
            if len(self._seen_hashes) >= GOSSIP_WINDOW_SIZE:
                old = self._seen_hashes[0]
                self._seen_set.discard(old)
            self._seen_hashes.append(hash_hex)
            self._seen_set.add(hash_hex)

    def is_seen(self, hash_hex: str) -> bool:
        return hash_hex in self._seen_set

    # -------------------------------------------------------------------
    # Maintenance
    # -------------------------------------------------------------------

    async def _maintenance_loop(self):
        """Periodic maintenance: reconnect, trust recovery, etc."""
        while self._running:
            await asyncio.sleep(RECONNECT_INTERVAL)

            # Recover trust scores over time
            for peer in self.peers.values():
                if peer.trust_score < INITIAL_TRUST_SCORE:
                    peer.trust_score = min(
                        INITIAL_TRUST_SCORE,
                        peer.trust_score + TRUST_RECOVERY_PER_MINUTE,
                    )

            # Try to maintain outbound connections
            outbound = sum(1 for p in self.peers.values() if not p.inbound)
            if outbound < self.max_outbound:
                needed = self.max_outbound - outbound
                candidates = [
                    addr
                    for addr in self.known_addresses
                    if addr not in self.peers and not self._is_banned(addr)
                ]
                for addr in candidates[:needed]:
                    asyncio.create_task(self.connect_to_peer(addr))

    def get_peer_addresses(self) -> List[str]:
        """Return addresses of all connected peers."""
        return list(self.peers.keys())

    def get_known_addresses(self) -> List[str]:
        """Return all known peer addresses."""
        return list(self.known_addresses)
