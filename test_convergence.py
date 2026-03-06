"""Test that _apply_pixel produces identical state regardless of pixel arrival order."""

import itertools
import random
from pixelchain.blockchain import Blockchain, GENESIS_CLOSURE_HASH
from pixelchain.models import Pixel
from pixelchain.config import INITIAL_DIFFICULTY_BITS


def make_pixel(x, y, r, g, b, prev_hash=None, nonce_start=0):
    """Mine a valid pixel at the given coord."""
    p = Pixel(
        x=x, y=y, r=r, g=g, b=b,
        epoch="a",
        closure_hash=GENESIS_CLOSURE_HASH,
        prev_pixel_hash=prev_hash,
        nonce=nonce_start,
    )
    p.mine(INITIAL_DIFFICULTY_BITS)
    return p


def apply_pixels_fresh(pixels_in_order):
    """Create a fresh blockchain and apply pixels in the given order.
    Returns (canvas bytes for the coords, coord_best mapping)."""
    bc = Blockchain()
    for p in pixels_in_order:
        bc.accept_pixel(p)
    return bc


def test_two_independent_pixels():
    """Two independent pixels at the same coord — order shouldn't matter."""
    p1 = make_pixel(5, 5, 255, 0, 0)        # red
    p2 = make_pixel(5, 5, 0, 255, 0, nonce_start=999999)  # green

    bc_ab = apply_pixels_fresh([p1, p2])
    bc_ba = apply_pixels_fresh([p2, p1])

    c_ab = bc_ab.canvas.get_pixel(5, 5)
    c_ba = bc_ba.canvas.get_pixel(5, 5)
    assert c_ab == c_ba, f"Two independent pixels: {c_ab} != {c_ba}"
    print(f"  ✓ Two independent pixels → winner = {c_ab}")


def test_chained_pixels():
    """Pixel with prev_pixel_hash and one without — order shouldn't matter."""
    p1 = make_pixel(10, 10, 255, 0, 0)   # red, no prev
    p2 = make_pixel(10, 10, 0, 255, 0, prev_hash=p1.hash)  # green, chained

    bc_ab = apply_pixels_fresh([p1, p2])
    bc_ba = apply_pixels_fresh([p2, p1])

    c_ab = bc_ab.canvas.get_pixel(10, 10)
    c_ba = bc_ba.canvas.get_pixel(10, 10)
    assert c_ab == c_ba, f"Chained pixels: {c_ab} != {c_ba}"
    print(f"  ✓ Chained pixels → winner = {c_ab}")


def test_many_pixels_all_permutations():
    """5 pixels on the same coord — all 120 permutations must produce the same canvas."""
    pixels = []
    prev = None
    for i in range(5):
        r, g, b = (i * 50) % 256, (i * 80 + 30) % 256, (i * 110 + 60) % 256
        p = make_pixel(42, 42, r, g, b, prev_hash=prev, nonce_start=i * 1000000)
        pixels.append(p)
        prev = p.hash  # chain them

    # Get reference result
    ref = apply_pixels_fresh(pixels)
    ref_color = ref.canvas.get_pixel(42, 42)
    ref_best_hash = ref.epoch_states["a"].coord_best[(42, 42)].hash.hex()[:16]
    ref_work = ref.epoch_states["a"].coord_work[(42, 42)]

    ok = 0
    fail = 0
    for perm in itertools.permutations(pixels):
        bc = apply_pixels_fresh(perm)
        color = bc.canvas.get_pixel(42, 42)
        if color != ref_color:
            fail += 1
            best = bc.epoch_states["a"].coord_best[(42, 42)]
            print(f"    FAIL: got {color} (hash={best.hash.hex()[:16]}, work={best.pow_work()}) "
                  f"expected {ref_color}")
        else:
            ok += 1

    assert fail == 0, f"{fail}/{ok + fail} permutations diverged!"
    print(f"  ✓ 5 chained pixels: all {ok} permutations agree → {ref_color} (hash={ref_best_hash}, work={ref_work})")


def test_many_coords_shuffled():
    """50 pixels across 10 coords — forward, reverse, and 10 random shuffles must agree."""
    pixels = []
    for coord_i in range(10):
        prev = None
        for j in range(5):
            r = (coord_i * 30 + j * 50) % 256
            g = (coord_i * 70 + j * 80) % 256
            b = (coord_i * 110 + j * 30) % 256
            p = make_pixel(coord_i, 0, r, g, b, prev_hash=prev,
                           nonce_start=(coord_i * 5 + j) * 1000000)
            pixels.append(p)
            prev = p.hash

    ref = apply_pixels_fresh(pixels)
    ref_data = ref.canvas.to_base64()

    orders = [("forward", pixels)]
    orders.append(("reverse", list(reversed(pixels))))
    rng = random.Random(12345)
    for i in range(10):
        shuffled = list(pixels)
        rng.shuffle(shuffled)
        orders.append((f"shuffle_{i}", shuffled))

    for name, order in orders:
        bc = apply_pixels_fresh(order)
        data = bc.canvas.to_base64()
        assert data == ref_data, f"Order '{name}' diverged from reference!"

    print(f"  ✓ 50 pixels across 10 coords: all {len(orders)} orderings agree")


def test_realistic_conflict():
    """Simulate the exact scenario that caused the bug:
    Multiple independent miners sending pixels at same coord with prev_pixel_hash set."""
    # First pixel at coord
    p1 = make_pixel(167, 14, 0, 255, 136)  # green, no prev
    # High-work pixel extending it
    p2 = make_pixel(167, 14, 255, 58, 0, prev_hash=p1.hash, nonce_start=5000000)
    # Several low-work pixels extending
    p3 = make_pixel(167, 14, 0, 255, 136, prev_hash=p2.hash, nonce_start=10000000)
    p4 = make_pixel(167, 14, 0, 255, 136, prev_hash=p3.hash, nonce_start=15000000)
    p5 = make_pixel(167, 14, 255, 58, 0, prev_hash=p4.hash, nonce_start=20000000)

    all_pixels = [p1, p2, p3, p4, p5]

    # Show individual works
    for i, p in enumerate(all_pixels):
        print(f"    p{i+1}: color={p.color_hex} work={p.pow_work()} hash={p.hash.hex()[:16]}")

    # Test all permutations
    ref = apply_pixels_fresh(all_pixels)
    ref_color = ref.canvas.get_pixel(167, 14)

    fail = 0
    for perm in itertools.permutations(all_pixels):
        bc = apply_pixels_fresh(perm)
        color = bc.canvas.get_pixel(167, 14)
        if color != ref_color:
            fail += 1

    assert fail == 0, f"{fail}/120 permutations diverged for realistic conflict!"
    print(f"  ✓ Realistic conflict: all 120 permutations → {ref_color}")


def test_orphan_child_before_parent():
    """Child pixel arrives before its parent → buffered as orphan, then released."""
    p1 = make_pixel(20, 20, 255, 0, 0)                    # parent (no prev)
    p2 = make_pixel(20, 20, 0, 255, 0, prev_hash=p1.hash) # child

    bc = Blockchain()

    # Child first — should be buffered
    assert bc.accept_pixel(p2) is True, "Child should be accepted (buffered)"
    assert p2.hash.hex() in bc._seen_hashes, "Child should be in seen_hashes"
    assert bc._orphan_count == 1, "Should have 1 orphan"
    # Canvas should NOT show the child yet
    assert bc.canvas.get_pixel(20, 20) == (0, 0, 0), "Canvas should still be black"

    # Parent arrives — should release child
    assert bc.accept_pixel(p1) is True, "Parent should be accepted"
    assert bc._orphan_count == 0, "Orphan buffer should be empty"
    # Both pixels applied — winner is highest individual work
    state = bc.epoch_states["a"]
    assert state.pixel_count == 2, f"Expected 2 pixels, got {state.pixel_count}"
    print(f"  ✓ Child buffered then released, canvas = {bc.canvas.get_pixel(20, 20)}")


def test_orphan_cascade():
    """A→B→C chain: C arrives first, then B, then A — all released in cascade."""
    pA = make_pixel(30, 30, 255, 0, 0)                      # root
    pB = make_pixel(30, 30, 0, 255, 0, prev_hash=pA.hash, nonce_start=1000000)
    pC = make_pixel(30, 30, 0, 0, 255, prev_hash=pB.hash, nonce_start=2000000)

    bc = Blockchain()

    # C first → orphan (waiting for B)
    assert bc.accept_pixel(pC) is True
    assert bc._orphan_count == 1

    # B next → orphan (waiting for A), but C is still waiting for B
    assert bc.accept_pixel(pB) is True
    assert bc._orphan_count == 2

    # A arrives → accepts A, releases B (whose parent A is now known),
    #             then releases C (whose parent B is now known)
    assert bc.accept_pixel(pA) is True
    assert bc._orphan_count == 0, f"Expected 0 orphans, got {bc._orphan_count}"
    state = bc.epoch_states["a"]
    assert state.pixel_count == 3, f"Expected 3 pixels, got {state.pixel_count}"
    print(f"  ✓ Cascade A→B→C released correctly, pixel_count = {state.pixel_count}")


def test_invalid_prev_pixel_hash():
    """Pixel with a fabricated prev_pixel_hash should stay in orphan buffer forever."""
    fake_parent = b"\xde\xad\xbe\xef" * 8  # 32 bytes of garbage
    p_bad = make_pixel(40, 40, 255, 0, 0, prev_hash=fake_parent)

    bc = Blockchain()
    assert bc.accept_pixel(p_bad) is True, "Accepted into orphan buffer"
    assert bc._orphan_count == 1

    # It's in seen_hashes (won't be re-buffered) but NOT in state.pixels
    state = bc.epoch_states["a"]
    assert state.pixel_count == 0, "Should NOT be applied to state"
    assert bc.canvas.get_pixel(40, 40) == (0, 0, 0), "Should NOT affect canvas"

    # Add a legitimate pixel — orphan stays
    p_good = make_pixel(40, 40, 0, 255, 0, nonce_start=5000000)
    bc.accept_pixel(p_good)
    assert bc._orphan_count == 1, "Orphan still waiting"
    assert state.pixel_count == 1, "Only good pixel applied"
    print(f"  ✓ Fake prev_pixel_hash stays buffered, never applied")


def test_orphan_buffer_limit():
    """When orphan buffer is full, new orphans are rejected."""
    bc = Blockchain()

    # Temporarily lower the limit
    old_max = Blockchain._MAX_ORPHANS
    Blockchain._MAX_ORPHANS = 5

    try:
        fake_parent = b"\x01" * 32
        orphans = []
        for i in range(7):
            p = make_pixel(50 + i, 50, 255, 0, 0, prev_hash=fake_parent,
                           nonce_start=i * 1000000)
            orphans.append(p)

        results = [bc.accept_pixel(p) for p in orphans]
        accepted = sum(results)
        rejected = len(results) - accepted
        assert accepted == 5, f"Expected 5 accepted, got {accepted}"
        assert rejected == 2, f"Expected 2 rejected, got {rejected}"
        assert bc._orphan_count == 5
        print(f"  ✓ Buffer limit enforced: {accepted} accepted, {rejected} rejected")
    finally:
        Blockchain._MAX_ORPHANS = old_max


if __name__ == "__main__":
    print("=== Convergence Tests ===\n")

    print("1. Two independent pixels at same coord:")
    test_two_independent_pixels()

    print("\n2. Chained pixels (prev_pixel_hash):")
    test_chained_pixels()

    print("\n3. Five chained pixels — all 120 permutations:")
    test_many_pixels_all_permutations()

    print("\n4. 50 pixels across 10 coords — shuffled orderings:")
    test_many_coords_shuffled()

    print("\n5. Realistic conflict scenario (bug repro):")
    test_realistic_conflict()

    print("\n6. Orphan buffer — child before parent:")
    test_orphan_child_before_parent()

    print("\n7. Orphan cascade — grandchild before all:")
    test_orphan_cascade()

    print("\n8. Invalid prev_pixel_hash — rejected properly:")
    test_invalid_prev_pixel_hash()

    print("\n9. Orphan buffer limit (DoS protection):")
    test_orphan_buffer_limit()

    print("\n✓ All convergence tests passed!")
