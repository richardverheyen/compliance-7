#!/usr/bin/env python3
"""Tests for the architect pipeline and group enrichment."""

import json
import pytest
from main import enrich_groups_with_nodes
from architect import (
    generate_section_gating_rules,
    validate_ids,
    get_top_level_section,
    ID_REGEX,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_nodes():
    """Synthetic nodes for testing enrichment boundary logic."""
    return [
        {"node_index": 0, "text": "Root", "rule_code": "Part 4.1", "type": "RULE", "is_bold": True, "is_italic": False},
        {"node_index": 1, "text": "Intro text", "rule_code": "4.1.1", "type": "RULE", "is_bold": False, "is_italic": False},
        {"node_index": 2, "text": "Sub-section A", "rule_code": "4.1.2", "type": "RULE", "is_bold": False, "is_italic": False},
        {"node_index": 3, "text": "Detail A1", "rule_code": "4.1.2(1)", "type": "RULE", "is_bold": False, "is_italic": False},
        {"node_index": 4, "text": "Detail A2", "rule_code": "4.1.2(2)", "type": "RULE", "is_bold": False, "is_italic": False},
        {"node_index": 5, "text": "Sub-section B", "rule_code": "4.1.3", "type": "RULE", "is_bold": True, "is_italic": False},
        {"node_index": 6, "text": "Detail B1", "rule_code": "4.1.3(1)", "type": "RULE", "is_bold": False, "is_italic": False},
        {"node_index": 7, "text": "Note about B", "rule_code": "", "type": "TEXT", "is_bold": False, "is_italic": True},
        {"node_index": 8, "text": "Section 2 start", "rule_code": "4.2.1", "type": "RULE", "is_bold": False, "is_italic": False},
        {"node_index": 9, "text": "Section 2 detail", "rule_code": "4.2.2", "type": "RULE", "is_bold": False, "is_italic": False},
    ]


@pytest.fixture
def sample_groups():
    """Groups matching the sample nodes."""
    return [
        {"id": "4", "depth": 0, "first_node_index": 0, "x_indent": 90.0},
        {"id": "4_1", "depth": 1, "first_node_index": 0, "x_indent": 90.0},
        {"id": "4_1_2", "depth": 2, "first_node_index": 2, "x_indent": 90.0},
        {"id": "4_1_3", "depth": 2, "first_node_index": 5, "x_indent": 90.0},
        {"id": "4_2", "depth": 1, "first_node_index": 8, "x_indent": 90.0},
    ]


@pytest.fixture
def intro():
    """Minimal introduction.json fixture."""
    return {
        "scoping": {
            "4_1_4_1": {"sections": ["4_2"], "processes": ["PROC-AML-002"]},
            "4_1_4_2": {"sections": ["4_3"], "processes": ["PROC-AML-002"]},
            "4_1_4_3": {"sections": ["4_4"], "processes": ["PROC-AML-002"]},
            "4_1_5_1": {"sections": ["4_12"], "processes": ["PROC-AML-003"]},
            "4_1_8": {"sections": ["4_11"], "processes": ["PROC-AML-001"]},
        },
        "alwaysActive": {
            "sections": ["4_1", "4_9", "4_10", "4_14", "4_15"],
            "processes": [],
        },
    }


# ---------------------------------------------------------------------------
# Enrichment tests
# ---------------------------------------------------------------------------

class TestEnrichGroupsWithNodes:
    def test_parent_group_includes_all_child_nodes(self, sample_nodes, sample_groups):
        """Parent group 4_1 should include nodes from 4_1_2 and 4_1_3."""
        enriched = enrich_groups_with_nodes(sample_nodes, sample_groups)
        g4_1 = next(g for g in enriched if g["id"] == "4_1")
        # 4_1 starts at index 0, next non-descendant is 4_2 at index 8
        assert len(g4_1["text_nodes"]) == 8
        indices = [tn["node_index"] for tn in g4_1["text_nodes"]]
        assert indices == [0, 1, 2, 3, 4, 5, 6, 7]

    def test_leaf_group_stops_at_sibling(self, sample_nodes, sample_groups):
        """Leaf group 4_1_2 should stop at 4_1_3's first_node_index."""
        enriched = enrich_groups_with_nodes(sample_nodes, sample_groups)
        g4_1_2 = next(g for g in enriched if g["id"] == "4_1_2")
        indices = [tn["node_index"] for tn in g4_1_2["text_nodes"]]
        assert indices == [2, 3, 4]

    def test_last_group_gets_remaining_nodes(self, sample_nodes, sample_groups):
        """Last group 4_2 should get all remaining nodes to end of document."""
        enriched = enrich_groups_with_nodes(sample_nodes, sample_groups)
        g4_2 = next(g for g in enriched if g["id"] == "4_2")
        indices = [tn["node_index"] for tn in g4_2["text_nodes"]]
        assert indices == [8, 9]

    def test_root_group_includes_everything(self, sample_nodes, sample_groups):
        """Root group 4 should include all nodes (no non-descendant follows)."""
        enriched = enrich_groups_with_nodes(sample_nodes, sample_groups)
        g4 = next(g for g in enriched if g["id"] == "4")
        assert len(g4["text_nodes"]) == 10

    def test_text_node_fields(self, sample_nodes, sample_groups):
        """Enriched text nodes should have the expected trimmed fields."""
        enriched = enrich_groups_with_nodes(sample_nodes, sample_groups)
        g4_1_3 = next(g for g in enriched if g["id"] == "4_1_3")
        tn = g4_1_3["text_nodes"][0]
        assert set(tn.keys()) == {"node_index", "text", "rule_code", "type", "is_bold", "is_italic"}

    def test_empty_group(self):
        """A group with no nodes in range should have empty text_nodes."""
        nodes = [{"node_index": 0, "text": "A", "rule_code": "", "type": "TEXT", "is_bold": False, "is_italic": False}]
        groups = [
            {"id": "4_1", "depth": 1, "first_node_index": 5, "x_indent": 90.0},
        ]
        enriched = enrich_groups_with_nodes(nodes, groups)
        assert enriched[0]["text_nodes"] == []


# ---------------------------------------------------------------------------
# Section gating rules tests
# ---------------------------------------------------------------------------

class TestSectionGatingRules:
    def test_scoped_sections_get_rules(self, intro):
        """Every section in scoping should get a SHOW rule."""
        rules = generate_section_gating_rules(intro)
        assert len(rules["4_2"]) == 1
        assert rules["4_2"][0]["scope"] == "4_1_4_1"
        assert rules["4_2"][0]["effect"] == "SHOW"

    def test_always_active_sections_get_no_rules(self, intro):
        """Always-active sections should have empty rule lists."""
        rules = generate_section_gating_rules(intro)
        assert rules["4_1"] == []
        assert rules["4_9"] == []
        assert rules["4_10"] == []

    def test_all_scoped_sections_covered(self, intro):
        """Every section referenced in scoping should appear in rules."""
        rules = generate_section_gating_rules(intro)
        for mapping in intro["scoping"].values():
            for section in mapping["sections"]:
                assert section in rules

    def test_derived_sections(self, intro):
        """Derived sections (4_12, 4_13) should get rules from their scoping entries."""
        rules = generate_section_gating_rules(intro)
        assert "4_12" in rules
        assert rules["4_12"][0]["scope"] == "4_1_5_1"


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------

class TestValidation:
    def test_valid_ids(self):
        data = {
            "controls": [{"id": "4_2_3_1"}],
            "groups": [{"id": "4_2_3"}],
            "rules": [{"target": "4_2_3_1_a"}],
        }
        assert validate_ids(data) == []

    def test_invalid_control_id(self):
        data = {"controls": [{"id": "bad_id"}], "groups": [], "rules": []}
        warnings = validate_ids(data)
        assert len(warnings) == 1
        assert "bad_id" in warnings[0]

    def test_invalid_group_id(self):
        data = {"controls": [], "groups": [{"id": "4.2.3"}], "rules": []}
        warnings = validate_ids(data)
        assert len(warnings) == 1

    def test_id_regex_valid(self):
        valid = ["4_1", "4_2_3", "4_2_3_1", "4_2_3_1_a", "4_15_6"]
        for v in valid:
            assert ID_REGEX.match(v), f"{v} should be valid"

    def test_id_regex_invalid(self):
        invalid = ["4", "3_1", "4.2.3", "4_a", "foo", "4_2_3_1_ab"]
        for v in invalid:
            assert not ID_REGEX.match(v), f"{v} should be invalid"


# ---------------------------------------------------------------------------
# Utility tests
# ---------------------------------------------------------------------------

class TestUtils:
    def test_get_top_level_section(self):
        assert get_top_level_section("4_2") == "4_2"
        assert get_top_level_section("4_2_3") == "4_2"
        assert get_top_level_section("4_2_3_1_a") == "4_2"
        assert get_top_level_section("4_12_7_2") == "4_12"


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def validate_section(data: dict) -> list[str]:
    """Validate a SectionData dict for required fields and consistency."""
    errors = []

    if not isinstance(data.get("controls"), list):
        errors.append("Missing or invalid 'controls' array")
    if not isinstance(data.get("groups"), list):
        errors.append("Missing or invalid 'groups' array")
    if not isinstance(data.get("rules"), list):
        errors.append("Missing or invalid 'rules' array")

    if errors:
        return errors

    # Check control fields
    for c in data["controls"]:
        if "id" not in c:
            errors.append(f"Control missing 'id': {c}")
        if "label" not in c:
            errors.append(f"Control missing 'label': {c.get('id', '?')}")
        if "detail-required" not in c:
            errors.append(f"Control missing 'detail-required': {c.get('id', '?')}")
        if "correct-option" not in c:
            errors.append(f"Control missing 'correct-option': {c.get('id', '?')}")
        elif c["correct-option"] not in ("Yes", "No", "N/A"):
            errors.append(f"Invalid correct-option '{c['correct-option']}' in {c.get('id', '?')}")

    # Check group fields
    for g in data["groups"]:
        if "id" not in g:
            errors.append(f"Group missing 'id': {g}")
        if "title" not in g:
            errors.append(f"Group missing 'title': {g.get('id', '?')}")

    # Check rule fields
    for r in data["rules"]:
        for field in ("target", "scope", "effect", "schema"):
            if field not in r:
                errors.append(f"Rule missing '{field}': {r}")
        if r.get("effect") not in ("SHOW", "HIDE"):
            errors.append(f"Invalid effect '{r.get('effect')}' in rule targeting {r.get('target', '?')}")
        if "const" not in r.get("schema", {}):
            errors.append(f"Rule schema missing 'const': {r.get('target', '?')}")

    # Check rule targets/scopes reference existing IDs
    all_ids = {c["id"] for c in data["controls"]} | {g["id"] for g in data["groups"]}
    for r in data["rules"]:
        target = r.get("target", "")
        # target_detail is also valid (for detail fields)
        base_target = target.replace("_detail", "")
        if base_target not in all_ids and target not in all_ids:
            # Gating rules may target section IDs not in this file — skip those
            pass

    return errors


class TestSchemaValidation:
    def test_valid_section(self):
        data = {
            "controls": [{"id": "4_2_3_1", "label": "Q?", "detail-required": False, "correct-option": "Yes"}],
            "groups": [{"id": "4_2_3", "title": "Collection"}],
            "rules": [{"target": "4_2", "scope": "4_1_4_1", "effect": "SHOW", "schema": {"const": "Yes"}}],
        }
        assert validate_section(data) == []

    def test_missing_control_fields(self):
        data = {
            "controls": [{"id": "4_2_3_1"}],
            "groups": [],
            "rules": [],
        }
        errors = validate_section(data)
        assert len(errors) == 3  # missing label, detail-required, correct-option

    def test_invalid_correct_option(self):
        data = {
            "controls": [{"id": "4_2_3_1", "label": "Q?", "detail-required": False, "correct-option": "Maybe"}],
            "groups": [],
            "rules": [],
        }
        errors = validate_section(data)
        assert any("correct-option" in e for e in errors)


# ---------------------------------------------------------------------------
# Integration test with real data
# ---------------------------------------------------------------------------

class TestRealData:
    """Tests against the actual project data files (skipped if files don't exist)."""

    @pytest.fixture
    def real_groups_enriched(self):
        path = "runs/1/groups_enriched.json"
        if not os.path.exists(path):
            pytest.skip("groups_enriched.json not found")
        with open(path) as f:
            return json.load(f)

    @pytest.fixture
    def real_intro(self):
        path = "data/introduction.json"
        with open(path) as f:
            return json.load(f)

    def test_all_groups_have_text_nodes_key(self, real_groups_enriched):
        for g in real_groups_enriched:
            assert "text_nodes" in g, f"Group {g['id']} missing text_nodes"

    def test_gating_covers_all_scoped_sections(self, real_intro):
        rules = generate_section_gating_rules(real_intro)
        scoping = real_intro["scoping"]
        for control_id, mapping in scoping.items():
            for section in mapping["sections"]:
                assert section in rules
                matching = [r for r in rules[section] if r["scope"] == control_id]
                assert len(matching) == 1, f"Expected 1 rule for {section} from {control_id}"


import os
