#!/usr/bin/env python3
"""Test convergence between two nodes."""
import asyncio
import base64
import json
import random
import time

import websockets


async def submit_pixels(ws_url, count, x_start, label):
    """Submit pixels via WebSocket and return confirmation count."""
    confirmed = 0
    async with websockets.connect(ws_url, max_size=64 * 1024 * 1024) as ws:
        # Drain init messages
        for _ in range(3):
            try:
                await asyncio.wait_for(ws.recv(), timeout=3)
            except Exception:
                break

        # Submit pixels
        ids = []
        for i in range(count):
            pid = f"{label}-{i}"
            x = (x_start + i) % 1000
            y = (i * 7) % 1000
            r, g, b = random.randint(1, 255), random.randint(1, 255), random.randint(1, 255)
            color = f"{r:02X}{g:02X}{b:02X}"
            msg = json.dumps({"type": "add_pixel", "id": pid, "x": x, "y": y, "color": color})
            await ws.send(msg)
            ids.append(pid)

        # Wait for confirmations
        pending = set(ids)
        deadline = time.time() + 90
        while pending and time.time() < deadline:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=2)
                msg = json.loads(raw)
                if msg.get("type") == "pool_confirmed":
                    pid = msg.get("id")
                    pending.discard(pid)
                    confirmed += 1
            except asyncio.TimeoutError:
                continue
            except Exception:
                break

        print(f"  {label}: {confirmed}/{count} confirmed, {len(pending)} still pending")
        return confirmed


async def get_canvas(ws_url):
    """Get the full canvas from a node."""
    async with websockets.connect(ws_url, max_size=64 * 1024 * 1024) as ws:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        msg = json.loads(raw)
        if msg.get("type") == "canvas_init":
            return msg["pixels"]
    return None


async def main():
    print("=== Test: Submit 15 pixels to each node (30 total = 3 epochs) ===\n")

    c1 = await submit_pixels("ws://localhost:8080", 15, 0, "Node1")
    c2 = await submit_pixels("ws://localhost:8082", 15, 500, "Node2")
    total = c1 + c2
    print(f"\n  Total confirmed: {total}/30")

    print("\n  Waiting 10s for sync...")
    await asyncio.sleep(10)

    # Compare chain files
    print("\n=== Chain comparison ===")
    chains = {}
    for path, label in [("data/chain.jsonl", "Node1"), ("data2/chain.jsonl", "Node2")]:
        closures = []
        pixels = 0
        try:
            with open(path) as f:
                for line in f:
                    d = json.loads(line.strip())
                    if d.get("type") == "closure":
                        closures.append(d["hash"])
                    elif d.get("type") == "pixel":
                        pixels += 1
            chains[label] = closures
            print(f"  {label}: {len(closures)} closures, {pixels} pixels")
        except FileNotFoundError:
            print(f"  {label}: no chain file!")
            chains[label] = []

    c1h, c2h = chains.get("Node1", []), chains.get("Node2", [])
    shared = min(len(c1h), len(c2h))
    diverged = sum(1 for i in range(shared) if c1h[i] != c2h[i])
    ahead = abs(len(c1h) - len(c2h))

    print(f"\n  Shared: {shared}  Diverged: {diverged}  Ahead: {ahead}")
    if diverged == 0 and ahead == 0:
        print("  ✓ CHAINS MATCH")
    else:
        print("  ✗ CHAINS DIFFER")

    # Compare canvas via WebSocket
    print("\n=== Canvas comparison ===")
    cv1 = await get_canvas("ws://localhost:8080")
    cv2 = await get_canvas("ws://localhost:8082")

    if cv1 and cv2:
        d1 = base64.b64decode(cv1)
        d2 = base64.b64decode(cv2)
        nb1 = sum(1 for i in range(0, len(d1), 3) if d1[i] or d1[i + 1] or d1[i + 2])
        nb2 = sum(1 for i in range(0, len(d2), 3) if d2[i] or d2[i + 1] or d2[i + 2])
        print(f"  Node1: {nb1} non-black pixels")
        print(f"  Node2: {nb2} non-black pixels")

        if cv1 == cv2:
            print("  ✓ CANVAS MATCH")
        else:
            byte_diffs = sum(1 for i in range(len(d1)) if d1[i] != d2[i])
            pixel_diffs = byte_diffs // 3
            print(f"  ✗ CANVAS DIFFER: ~{pixel_diffs} pixel differences ({byte_diffs} bytes)")
    else:
        print("  ✗ Could not retrieve canvas from one or both nodes")

    # Test restart convergence
    print("\n=== Restart test: check canvas.b64 saved ===")
    import os
    for d, label in [("data", "Node1"), ("data2", "Node2")]:
        b64_path = os.path.join(d, "canvas.b64")
        if os.path.exists(b64_path):
            size = os.path.getsize(b64_path)
            print(f"  {label}: canvas.b64 exists ({size} bytes)")
        else:
            print(f"  {label}: canvas.b64 NOT FOUND")


if __name__ == "__main__":
    asyncio.run(main())
