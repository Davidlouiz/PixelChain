#!/usr/bin/env python3
"""Live convergence test: submit pixels to both nodes and verify they agree."""

import asyncio
import json
import random
import time
import sys
import os

import websockets

WS1 = "ws://127.0.0.1:8080"
WS2 = "ws://127.0.0.1:8082"


async def get_epoch(uri):
    """Get current epoch from a node."""
    async with websockets.connect(uri, max_size=10 * 1024 * 1024) as ws:
        # canvas_init
        await ws.recv()
        # pool_init
        await ws.recv()
        # epoch_update
        msg = json.loads(await ws.recv())
        return msg.get("epoch", "a")


async def add_pixel(uri, x, y, color_hex):
    """Queue a pixel in a node's mining pool."""
    async with websockets.connect(uri, max_size=10 * 1024 * 1024) as ws:
        await ws.recv()  # canvas_init
        await ws.recv()  # pool_init
        await ws.recv()  # epoch_update
        pixel_id = f"test-{random.randint(0, 2**32)}"
        await ws.send(json.dumps({
            "type": "add_pixel",
            "id": pixel_id,
            "x": x,
            "y": y,
            "color": color_hex,
        }))
        return pixel_id


def compare_chains():
    """Compare closure chains from both data files."""
    def load_closures(path):
        closures = {}
        if not os.path.exists(path):
            return closures
        with open(path) as f:
            for line in f:
                obj = json.loads(line)
                if "epoch" in obj and "hash" in obj and "merkle_root" in obj:
                    closures[obj["epoch"]] = obj["hash"]
        return closures

    c1 = load_closures("data/chain.jsonl")
    c2 = load_closures("data2/chain.jsonl")

    if not c1 and not c2:
        print("  No closures in either chain yet")
        return True, 0

    epochs = sorted(set(list(c1.keys()) + list(c2.keys())))
    matched = 0
    diverged = 0
    for e in epochs:
        h1 = c1.get(e, "MISSING")[:16]
        h2 = c2.get(e, "MISSING")[:16]
        status = "OK" if h1 == h2 else "DIVERGED"
        if status == "OK":
            matched += 1
        else:
            diverged += 1
        print(f"  Epoch {e}: {status} (n1={h1} n2={h2})")

    print(f"\n  Matched: {matched}, Diverged: {diverged}")
    return diverged == 0, matched


async def main():
    target_epochs = 5
    colors = ["FF0000", "00FF00", "0000FF", "FFFF00", "FF00FF", "00FFFF",
              "FF8800", "8800FF", "008800", "880000"]

    print(f"Target: {target_epochs} completed epochs")
    print("Submitting pixels to both nodes via mining pool...\n")

    submitted = 0
    for i in range(200):
        x = random.randint(0, 999)
        y = random.randint(0, 999)
        color = random.choice(colors)

        # Alternate between nodes
        uri = WS1 if i % 2 == 0 else WS2
        try:
            await add_pixel(uri, x, y, color)
            submitted += 1
        except Exception as e:
            if "too big" not in str(e):
                print(f"  Error: {e}")

        if i % 20 == 19:
            e1 = await get_epoch(WS1)
            e2 = await get_epoch(WS2)
            print(f"  [{submitted} pixels] Node1 epoch={e1}, Node2 epoch={e2}")

            if ord(e1) - ord('a') >= target_epochs and ord(e2) - ord('a') >= target_epochs:
                print(f"  Both nodes past target — stopping submissions")
                break

        await asyncio.sleep(0.3)

    print(f"\nSubmitted {submitted} pixels. Waiting 60s for mining + propagation...")

    for wait in range(12):
        await asyncio.sleep(5)
        e1 = await get_epoch(WS1)
        e2 = await get_epoch(WS2)
        print(f"  [{5*(wait+1)}s] Node1={e1} Node2={e2}")
        if (ord(e1) - ord('a') >= target_epochs and
            ord(e2) - ord('a') >= target_epochs):
            # Give a bit more time for final sync
            await asyncio.sleep(10)
            break

    print("\n=== Chain Comparison ===")
    converged, matched = compare_chains()

    if converged and matched >= target_epochs:
        print(f"\n*** CONVERGENCE TEST PASSED ({matched} epochs matched) ***")
    elif converged:
        print(f"\n*** Chains agree but only {matched} epochs completed (target: {target_epochs}) ***")
        print("*** Consider running longer ***")
    else:
        print("\n*** CONVERGENCE TEST FAILED — chains diverged ***")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
