# PixelChain

A decentralized pixel canvas (1000×1000 RGB) regulated by SHA-256 proof-of-work, targeting 10 pixels/second network-wide.

## Architecture

- **Peers**: Python scripts communicating via JSON over TCP
- **UI**: Web page communicating with local peer via WebSocket
- **Bootstrap**: Centralized directories listing known peer IP:port

## Quick Start

```bash
pip install -r requirements.txt
python -m pixelchain --port 8333 --ws-port 8080
```

Open `http://localhost:8080` in your browser.

## Configuration

```bash
python -m pixelchain --help
```

Key options:
- `--port`: TCP port for P2P communication (default: 8333)
- `--ws-port`: WebSocket port for browser UI (default: 8080)
- `--max-peers`: Maximum outbound peer connections (default: 20)
- `--bootstrap`: Comma-separated list of bootstrap peer addresses
- `--data-dir`: Directory for blockchain data storage (default: ./data)
