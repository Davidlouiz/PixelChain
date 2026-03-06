"""Main entry point for the PixelChain peer node."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from .config import Config
from .node import Node
from .ws_server import WebSocketServer

logger = logging.getLogger('pixelchain')


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        prog='pixelchain',
        description='PixelChain — Decentralised pixel canvas with proof-of-work',
    )
    parser.add_argument('--port', type=int, default=8333,
                        help='TCP port for P2P communication (default: 8333)')
    parser.add_argument('--ws-port', type=int, default=8080,
                        help='HTTP + WebSocket port for browser UI (default: 8080)')
    parser.add_argument('--max-peers', type=int, default=20,
                        help='Maximum outbound peers (default: 20)')
    parser.add_argument('--bootstrap', type=str, default='',
                        help='Comma-separated bootstrap peer addresses (ip:port)')
    parser.add_argument('--data-dir', type=str, default='./data',
                        help='Data directory (default: ./data)')
    parser.add_argument('--bind', type=str, default='0.0.0.0',
                        help='Bind address (default: 0.0.0.0)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Log level (default: INFO)')

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%H:%M:%S',
    )

    bootstrap = [s.strip() for s in args.bootstrap.split(',') if s.strip()]

    return Config(
        tcp_port=args.port,
        ws_port=args.ws_port,
        max_outbound=args.max_peers,
        bootstrap_peers=bootstrap,
        data_dir=args.data_dir,
        bind_address=args.bind,
    )


async def run(config: Config):
    """Main async entry point."""
    node = Node(config)
    ws_server = WebSocketServer(node, host=config.bind_address, port=config.ws_port)

    # Start node and combined HTTP+WS server
    await node.start()
    await ws_server.start()

    logger.info("=" * 60)
    logger.info("  PixelChain node running")
    logger.info("  P2P:      tcp://%s:%d", config.bind_address, config.tcp_port)
    logger.info("  Frontend: http://localhost:%d", config.ws_port)
    logger.info("=" * 60)

    # Wait forever (until signal)
    stop_event = asyncio.Event()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    await stop_event.wait()

    logger.info("Shutting down...")
    await ws_server.stop()
    await node.stop()


def main():
    config = parse_args()
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
