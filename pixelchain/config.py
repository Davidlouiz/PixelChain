"""Global configuration constants and defaults."""

from dataclasses import dataclass, field
from typing import List

# Canvas
CANVAS_WIDTH = 1000
CANVAS_HEIGHT = 1000
CANVAS_PIXELS = CANVAS_WIDTH * CANVAS_HEIGHT  # 1,000,000
CANVAS_BYTES = CANVAS_PIXELS * 3  # 3 MB (RGB24)

# Epoch
PIXELS_PER_EPOCH = 65536
TARGET_PIXELS_PER_SECOND = 10.0
TARGET_EPOCH_DURATION = PIXELS_PER_EPOCH / TARGET_PIXELS_PER_SECOND  # 6553.6 s
MAX_DIFFICULTY_ADJUSTMENT = 4.0  # ×4 or ÷4 cap per epoch

# PoW
INITIAL_DIFFICULTY_BITS = 1  # minimal difficulty for epoch "a"
HASH_SIZE = 32  # SHA-256 = 32 bytes

# Network
DEFAULT_TCP_PORT = 8333
DEFAULT_WS_PORT = 8080
DEFAULT_MAX_OUTBOUND = 20
MIN_PEERS = 8
RECONNECT_INTERVAL = 30.0  # seconds
GOSSIP_WINDOW_SIZE = 10000  # number of recent hashes kept for dedup

# Trust
INITIAL_TRUST_SCORE = 100
TRUST_PENALTY = 10
TRUST_RECOVERY_PER_MINUTE = 1
TRUST_BAN_THRESHOLD = 20
MAX_CONSECUTIVE_INVALID = 10
BAN_DURATIONS = [30, 300, 3600, 86400]  # escalating ban in seconds

# Protocol
PROTOCOL_VERSION = "1.0"
MAX_MESSAGE_SIZE = 16 * 1024 * 1024  # 16 MB


@dataclass
class Config:
    """Runtime configuration for a peer node."""

    tcp_port: int = DEFAULT_TCP_PORT
    ws_port: int = DEFAULT_WS_PORT
    max_outbound: int = DEFAULT_MAX_OUTBOUND
    bootstrap_peers: List[str] = field(default_factory=list)
    data_dir: str = "./data"
    bind_address: str = "0.0.0.0"
