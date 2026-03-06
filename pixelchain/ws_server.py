"""WebSocket + HTTP server for browser UI communication."""

from __future__ import annotations

import asyncio
import logging
import mimetypes
from pathlib import Path
from typing import TYPE_CHECKING

import websockets
from websockets import Headers, Request, Response

if TYPE_CHECKING:
    from .node import Node

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


def _serve_static(connection, request: Request) -> Response | None:
    """Intercept HTTP requests to serve static files.

    Returns a Response for static files, or None to let the WebSocket
    handshake proceed for upgrade requests.
    """
    # If this is a WebSocket upgrade request, let it through
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return None

    # Serve static files
    path = request.path
    if path == "/":
        path = "/index.html"

    file_path = STATIC_DIR / path.lstrip("/")
    # Security: prevent path traversal
    try:
        file_path = file_path.resolve()
        if not str(file_path).startswith(str(STATIC_DIR.resolve())):
            return Response(403, "Forbidden", Headers())
    except Exception:
        return Response(403, "Forbidden", Headers())

    if file_path.is_file():
        content_type, _ = mimetypes.guess_type(str(file_path))
        if content_type is None:
            content_type = "application/octet-stream"
        body = file_path.read_bytes()
        headers = Headers(
            {
                "Content-Type": content_type,
                "Content-Length": str(len(body)),
                "Cache-Control": "no-cache",
            }
        )
        return Response(200, "OK", headers, body)
    else:
        return Response(404, "Not Found", Headers(), b"Not Found")


class WebSocketServer:
    """Combined WebSocket + HTTP server that serves the frontend and bridges the Node."""

    def __init__(self, node: "Node", host: str = "0.0.0.0", port: int = 8080):
        self.node = node
        self.host = host
        self.port = port
        self._server = None

    async def start(self):
        """Start the combined WebSocket + HTTP server."""
        self._server = await websockets.serve(
            self._ws_handler,
            self.host,
            self.port,
            process_request=_serve_static,
            max_size=16 * 1024 * 1024,  # 16 MB — canvas_init is ~4 MB
        )
        logger.info(
            "Server listening on http://%s:%d (WS + HTTP)", self.host, self.port
        )

    async def stop(self):
        """Stop the server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        logger.info("Server stopped")

    async def _ws_handler(self, websocket):
        """Handle a single WebSocket connection."""
        logger.info("Browser connected from %s", websocket.remote_address)
        self.node.register_ws_client(websocket)

        try:
            # Send initial state
            await self.node.send_ws_init(websocket)

            # Listen for messages
            async for message in websocket:
                await self.node.handle_ws_message(websocket, message)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logger.error("WebSocket handler error: %s", e)
        finally:
            self.node.unregister_ws_client(websocket)
            logger.info("Browser disconnected")
