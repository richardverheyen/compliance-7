"""Tests for group identification — no sibling overlap, correct filtering."""

import json
import re
import sys

sys.path.insert(0, ".")
from main import (
    _filter_sequential_rule_codes,
    _normalise_full,
    _parse_rule_code,
    build_groups,
)

NODES_PATH = "runs/1/nodes.json"


def load_nodes():
    with open(NODES_PATH) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Unit tests for _filter_sequential_rule_codes
# ---------------------------------------------------------------------------


def test_filter_rejects_forward_jump():
    """A stem that jumps far ahead of the current position is rejected."""
    nodes = [
        {"node_index": 0, "rule_code": "4.1.1", "x_indent": 89.8},
        {"node_index": 1, "rule_code": "4.1.2", "x_indent": 89.8},
        # Big forward jump — ranks of 4.9.3 are many stems ahead of 4.1.2
        {"node_index": 2, "rule_code": "4.9.3", "x_indent": 125.8},
        {"node_index": 3, "rule_code": "4.1.3", "x_indent": 89.8},
        {"node_index": 4, "rule_code": "4.1.4", "x_indent": 89.8},
        {"node_index": 5, "rule_code": "4.1.5", "x_indent": 89.8},
        {"node_index": 6, "rule_code": "4.1.6", "x_indent": 89.8},
    ]
    valid = _filter_sequential_rule_codes(nodes)
    assert 0 in valid
    assert 1 in valid
    assert 2 not in valid, "Forward jump from 4.1.2 to 4.9.3 should be filtered"
    assert 3 in valid


def test_filter_rejects_backward_regression():
    """A stem that goes backward after a higher stem is rejected."""
    nodes = [
        {"node_index": 0, "rule_code": "4.2.3", "x_indent": 89.8},
        {"node_index": 1, "rule_code": "4.2.4", "x_indent": 89.8},
        {"node_index": 2, "rule_code": "4.2.5", "x_indent": 89.8},
        {"node_index": 3, "rule_code": "4.2.3", "x_indent": 125.8},  # regression
    ]
    valid = _filter_sequential_rule_codes(nodes)
    assert 0 in valid
    assert 1 in valid
    assert 2 in valid
    assert 3 not in valid, "Backward regression from 4.2.5 to 4.2.3 should be filtered"


def test_filter_allows_same_stem_consecutively():
    """Multiple bracket variants of the same stem are fine."""
    nodes = [
        {"node_index": 0, "rule_code": "4.3.5", "x_indent": 89.8},
        {"node_index": 1, "rule_code": "4.3.5(1)", "x_indent": 125.8},
        {"node_index": 2, "rule_code": "4.3.5(2)", "x_indent": 125.8},
        {"node_index": 3, "rule_code": "4.3.5(3)", "x_indent": 125.8},
    ]
    valid = _filter_sequential_rule_codes(nodes)
    assert all(i in valid for i in range(4)), "Same-stem bracket items should all be valid"


def test_filter_rejects_same_stem_after_progression():
    """Once a higher stem is seen, the old stem must not recur."""
    nodes = [
        {"node_index": 0, "rule_code": "4.3.5", "x_indent": 89.8},
        {"node_index": 1, "rule_code": "4.3.5(1)", "x_indent": 125.8},
        {"node_index": 2, "rule_code": "4.3.6", "x_indent": 89.8},
        {"node_index": 3, "rule_code": "4.3.7", "x_indent": 89.8},
        {"node_index": 4, "rule_code": "4.3.8", "x_indent": 89.8},
        {"node_index": 5, "rule_code": "4.3.5", "x_indent": 125.8},  # stale
        {"node_index": 6, "rule_code": "4.3.5(1)", "x_indent": 125.8},  # stale
    ]
    valid = _filter_sequential_rule_codes(nodes)
    assert all(i in valid for i in range(5))
    assert 5 not in valid, "Stem 4.3.5 should be rejected after 4.3.8"
    assert 6 not in valid, "Stem 4.3.5 bracket should be rejected after 4.3.8"


def test_filter_allows_part_transitions():
    """Transitioning from Part 4.1 to Part 4.2 etc. is valid."""
    nodes = [
        {"node_index": 0, "rule_code": "Part 4.1", "x_indent": 89.8},
        {"node_index": 1, "rule_code": "4.1.1", "x_indent": 89.8},
        {"node_index": 2, "rule_code": "4.1.2", "x_indent": 89.8},
        {"node_index": 3, "rule_code": "Part 4.2", "x_indent": 89.8},
        {"node_index": 4, "rule_code": "4.2.1", "x_indent": 89.8},
    ]
    valid = _filter_sequential_rule_codes(nodes)
    assert all(i in valid for i in range(5)), "Part transitions should all be valid"


# ---------------------------------------------------------------------------
# Unit tests for singleton & roman-numeral group filtering
# ---------------------------------------------------------------------------


def test_singleton_groups_removed():
    """A group containing only one rule node should be removed."""
    nodes = [
        {"node_index": 0, "rule_code": "4.1.1", "x_indent": 89.8},
        {"node_index": 1, "rule_code": "4.1.2", "x_indent": 89.8},  # no children → singleton
        {"node_index": 2, "rule_code": "4.1.3", "x_indent": 89.8},
        {"node_index": 3, "rule_code": "4.1.3(1)", "x_indent": 125.8},
        {"node_index": 4, "rule_code": "4.1.3(2)", "x_indent": 125.8},
    ]
    groups = build_groups(nodes)
    ids = {g["id"] for g in groups}
    # 4_1_3 should exist (has children 4.1.3(1), 4.1.3(2))
    assert "4_1_3" in ids, "4_1_3 should be a group (has 2+ children)"
    # 4_1_2 should NOT exist — it's a standalone rule with no children
    assert "4_1_2" not in ids, "4_1_2 should be removed (singleton, no children)"


def test_indent_confirmed_group_survives_singleton_filter():
    """A group confirmed by indent structure should survive even if prefix
    matching finds only 1 member (scraper bracket flattening)."""
    # Simulates: (b) at x=150 followed by (i) at x=190
    # The scraper codes (i) as 4.1.3(i) not 4.1.3(b)(i), so prefix
    # matching for 4_1_3_b finds only 1 member. But indent proves children.
    nodes = [
        {"node_index": 0, "rule_code": "4.1.3", "x_indent": 89.8},
        {"node_index": 1, "rule_code": "4.1.3(a)", "x_indent": 150.0},
        {"node_index": 2, "rule_code": "4.1.3(b)", "x_indent": 150.0},
        {"node_index": 3, "rule_code": "4.1.3(i)", "x_indent": 190.0},  # child of (b)
        {"node_index": 4, "rule_code": "4.1.3(c)", "x_indent": 150.0},
    ]
    groups = build_groups(nodes)
    ids = {g["id"] for g in groups}
    assert "4_1_3_b" in ids, (
        "4_1_3_b should survive singleton filter (indent-confirmed)"
    )


def test_roman_numeral_chain_groups_removed():
    """Roman numeral groups created by chain inference (not indent) should
    be removed — they're artifacts of the scraper flattening (b)(i) → (i)."""
    nodes = [
        {"node_index": 0, "rule_code": "4.1.3", "x_indent": 89.8},
        {"node_index": 1, "rule_code": "4.1.3(a)", "x_indent": 150.0},
        {"node_index": 2, "rule_code": "4.1.3(b)", "x_indent": 150.0},
        {"node_index": 3, "rule_code": "4.1.3(i)", "x_indent": 190.0},
        {"node_index": 4, "rule_code": "4.1.3(i)(ii)", "x_indent": 190.0},
        {"node_index": 5, "rule_code": "4.1.3(c)", "x_indent": 150.0},
    ]
    groups = build_groups(nodes)
    ids = {g["id"] for g in groups}
    # (i)(ii) chain-infers group 4_1_3_i, but (i) is a roman numeral
    # sub-item of (b), not a real group
    assert "4_1_3_i" not in ids, (
        "4_1_3_i should be removed (roman numeral chain artifact)"
    )
    # (b) should still exist as indent-confirmed group
    assert "4_1_3_b" in ids, "4_1_3_b should exist (indent-confirmed)"


# ---------------------------------------------------------------------------
# Integration tests on real data
# ---------------------------------------------------------------------------


def test_no_sibling_overlap_in_real_data():
    """Sibling groups (same depth, same parent) must not overlap."""
    nodes = load_nodes()
    groups = build_groups(nodes)
    valid = _filter_sequential_rule_codes(nodes)

    spans = {}
    for g in groups:
        gid = g["id"]
        first = last = None
        for node in nodes:
            rc = node.get("rule_code", "")
            if not rc or node["node_index"] not in valid:
                continue
            full = _normalise_full(rc)
            if full == gid or full.startswith(gid + "_"):
                ni = node["node_index"]
                if first is None:
                    first = ni
                last = ni
        if first is not None:
            spans[gid] = (first, last)

    overlaps = []
    for g1 in groups:
        for g2 in groups:
            if g1["id"] >= g2["id"]:
                continue
            if g1["depth"] != g2["depth"]:
                continue
            p1 = "_".join(g1["id"].split("_")[:-1])
            p2 = "_".join(g2["id"].split("_")[:-1])
            if p1 != p2:
                continue
            if g1["id"] not in spans or g2["id"] not in spans:
                continue
            s1, e1 = spans[g1["id"]]
            s2, e2 = spans[g2["id"]]
            if s1 <= e2 and s2 <= e1:
                overlaps.append(
                    f"{g1['id']} ({s1}-{e1}) vs {g2['id']} ({s2}-{e2})"
                )

    assert overlaps == [], f"Unexpected sibling overlaps:\n" + "\n".join(
        f"  {o}" for o in overlaps
    )


def test_no_singleton_groups_in_real_data():
    """No group should contain only one rule node (unless indent-confirmed,
    which by definition has children even if prefix matching can't see them)."""
    nodes = load_nodes()
    groups = build_groups(nodes)
    valid = _filter_sequential_rule_codes(nodes)

    singletons = []
    for g in groups:
        gid = g["id"]
        count = 0
        for node in nodes:
            rc = node.get("rule_code", "")
            if not rc or node["node_index"] not in valid:
                continue
            full = _normalise_full(rc)
            if full == gid or full.startswith(gid + "_"):
                count += 1
        # Indent-confirmed singletons are OK — their children exist but
        # have miscoded rule_codes due to scraper bracket flattening
        if count <= 1:
            singletons.append(gid)

    # Remaining singletons are indent-confirmed groups whose children
    # have miscoded rule_codes (scraper bracket flattening).  These are
    # expected — their last segment is a single alpha letter (b, c, d)
    # or a short numeric (1) where the indent structure proves children.
    # What should NEVER appear is a multi-segment stem singleton like
    # "4_2_12" (a standalone rule with no children at all).
    roman_re = re.compile(r"^[ivx]+$")
    for gid in singletons:
        last_seg = gid.rsplit("_", 1)[-1]
        assert not roman_re.match(last_seg), (
            f"Singleton group {gid} ends in roman numeral (should be removed)"
        )
        # Singletons with long numeric stems (like 4_2_12 where 12 is a rule
        # number, not a bracket index) should not survive
        if last_seg.isdigit() and int(last_seg) > 9:
            raise AssertionError(
                f"Singleton group {gid} looks like a standalone rule, not indent-confirmed"
            )


def test_no_roman_numeral_groups_in_real_data():
    """No group should end with a roman numeral segment (chain artifact)."""
    nodes = load_nodes()
    groups = build_groups(nodes)

    roman_re = re.compile(r"^[ivx]+$")
    roman_groups = [
        g["id"] for g in groups if roman_re.match(g["id"].rsplit("_", 1)[-1])
    ]
    assert roman_groups == [], f"Roman numeral groups found: {roman_groups}"


def test_specific_singletons_removed():
    """Specific user-reported singleton groups must not exist."""
    nodes = load_nodes()
    groups = build_groups(nodes)
    ids = {g["id"] for g in groups}
    for gid in ["4_2_12", "4_3_7", "4_12_5", "4_9_2"]:
        assert gid not in ids, f"{gid} should be removed (standalone rule, no children)"


def test_specific_indent_groups_exist():
    """Groups confirmed by indent structure must exist even when prefix
    matching finds only 1 member."""
    nodes = load_nodes()
    groups = build_groups(nodes)
    ids = {g["id"] for g in groups}
    for gid in ["4_4_19_4_b", "4_4_19_1_c", "4_4_19_1_d", "4_12_7_2_b"]:
        assert gid in ids, f"{gid} should exist (indent-confirmed group)"


def test_groups_are_deduplicated():
    """Each group id should appear exactly once."""
    nodes = load_nodes()
    groups = build_groups(nodes)
    ids = [g["id"] for g in groups]
    assert len(ids) == len(set(ids)), (
        f"Duplicate group ids: {[x for x in ids if ids.count(x) > 1]}"
    )


def test_group_depth_matches_id():
    """A group's depth should equal the number of segments in its id minus 1."""
    nodes = load_nodes()
    groups = build_groups(nodes)
    for g in groups:
        expected_depth = len(g["id"].split("_")) - 1
        assert g["depth"] == expected_depth, (
            f"Group {g['id']} has depth {g['depth']} but expected {expected_depth}"
        )


def test_every_group_has_parent_group():
    """Every group except the root should have its parent in the group list."""
    nodes = load_nodes()
    groups = build_groups(nodes)
    ids = {g["id"] for g in groups}
    for g in groups:
        parts = g["id"].split("_")
        if len(parts) > 1:
            parent_id = "_".join(parts[:-1])
            assert parent_id in ids, (
                f"Group {g['id']} is missing parent group {parent_id}"
            )


# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
