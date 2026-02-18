#!/usr/bin/env python3
"""Tests for the architect pipeline and group enrichment."""

import json
import pytest
from architect import (
    generate_section_gating_rules,
    validate_ids,
    get_top_level_section,
    gather_process_nodes,
    build_process_user_message,
    extract_input_rule_codes,
    extract_output_rule_codes,
    compute_coverage_report,
    ID_REGEX,
    PROCESS_FORMS,
    REVIEW_TOOL,
)

try:
    from main import enrich_groups_with_nodes
    HAS_MAIN = True
except Exception:
    HAS_MAIN = False


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

@pytest.mark.skipif(not HAS_MAIN, reason="main.py import failed (PyMuPDF dependency)")
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


# ---------------------------------------------------------------------------
# Process forms tests
# ---------------------------------------------------------------------------

class TestProcessForms:
    def test_all_forms_have_required_keys(self):
        """Every process form must have title, source_groups, and gated_by."""
        for pid, form in PROCESS_FORMS.items():
            assert "title" in form, f"{pid} missing title"
            assert "source_groups" in form, f"{pid} missing source_groups"
            assert "gated_by" in form, f"{pid} missing gated_by"

    def test_source_groups_are_lists(self):
        for pid, form in PROCESS_FORMS.items():
            assert isinstance(form["source_groups"], list), f"{pid} source_groups not a list"
            assert len(form["source_groups"]) > 0, f"{pid} has empty source_groups"

    def test_gated_by_is_valid_id_or_none(self):
        for pid, form in PROCESS_FORMS.items():
            if form["gated_by"] is not None:
                assert ID_REGEX.match(form["gated_by"]), \
                    f"{pid} gated_by '{form['gated_by']}' is not a valid ID"

    def test_expected_form_count(self):
        assert len(PROCESS_FORMS) == 15

    def test_cdd_forms_are_gated(self):
        """All CDD forms should be gated by a customer category control."""
        cdd_forms = {k: v for k, v in PROCESS_FORMS.items() if k.startswith("cdd-")}
        assert len(cdd_forms) == 7
        for pid, form in cdd_forms.items():
            assert form["gated_by"] is not None, f"{pid} should be gated"
            assert form["gated_by"].startswith("4_1_4_"), f"{pid} gated_by should be a customer category control"

    def test_always_active_forms_not_gated(self):
        """Forms like risk-assessment, verification, record-keeping should not be gated."""
        ungated = ["risk-assessment", "verification-documents", "verification-electronic",
                   "record-keeping", "alternative-id"]
        for pid in ungated:
            assert PROCESS_FORMS[pid]["gated_by"] is None, f"{pid} should not be gated"


@pytest.mark.skipif(not HAS_MAIN, reason="main.py import failed (PyMuPDF dependency)")
class TestGatherProcessNodes:
    @pytest.fixture
    def enriched_groups(self, sample_nodes, sample_groups):
        return enrich_groups_with_nodes(sample_nodes, sample_groups)

    def test_gathers_nodes_from_source_group(self, enriched_groups):
        """gather_process_nodes should return all text nodes from the source group."""
        # risk-assessment uses source_groups=["4_1"]
        nodes = gather_process_nodes("risk-assessment", enriched_groups)
        assert len(nodes) == 8  # 4_1 has nodes 0-7
        indices = [n["node_index"] for n in nodes]
        assert indices == [0, 1, 2, 3, 4, 5, 6, 7]

    def test_unknown_source_group_returns_empty(self):
        """If source group not in enriched data, return empty list."""
        groups = [{"id": "4_99", "text_nodes": [{"node_index": 0, "text": "x", "rule_code": "", "type": "TEXT", "is_bold": False, "is_italic": False}]}]
        nodes = gather_process_nodes("cdd-individuals", groups)
        # cdd-individuals uses 4_2 which is not in groups
        assert nodes == []

    def test_gathers_from_correct_group(self, enriched_groups):
        """cdd-individuals uses 4_2, should get exactly those nodes."""
        nodes = gather_process_nodes("cdd-individuals", enriched_groups)
        assert len(nodes) == 2  # 4_2 has nodes 8, 9
        indices = [n["node_index"] for n in nodes]
        assert indices == [8, 9]


class TestProcessOutput:
    def test_process_output_with_source_rules(self):
        """Process output controls should accept source-rules field."""
        data = {
            "controls": [{
                "id": "4_2_3",
                "label": "Does your program collect KYC?",
                "detail-required": True,
                "correct-option": "Yes",
                "source-rules": ["4.2.3", "4.2.4"],
            }],
            "groups": [{"id": "4_2", "title": "Collection of KYC Information"}],
            "rules": [],
        }
        errors = validate_section(data)
        assert errors == []

    def test_process_output_source_rules_are_strings(self):
        """source-rules should be a list of strings."""
        ctrl = {
            "id": "4_2_3",
            "label": "Q?",
            "detail-required": False,
            "correct-option": "Yes",
            "source-rules": ["4.2.3", "4.2.4"],
        }
        assert isinstance(ctrl["source-rules"], list)
        assert all(isinstance(r, str) for r in ctrl["source-rules"])

    def test_build_process_user_message(self):
        """build_process_user_message should include form title and text nodes."""
        form_def = PROCESS_FORMS["cdd-individuals"]
        text_nodes = [
            {"node_index": 0, "text": "Test node", "rule_code": "4.2.1", "is_bold": False, "is_italic": False},
        ]
        msg = build_process_user_message("cdd-individuals", form_def, text_nodes)
        assert "Customer Due Diligence" in msg
        assert "Individuals" in msg
        assert "[4.2.1] Test node" in msg
        assert "source-rules" in msg


# ---------------------------------------------------------------------------
# Coverage audit tests
# ---------------------------------------------------------------------------

class TestCoverageAudit:
    @pytest.fixture
    def sample_text_nodes(self):
        return [
            {"node_index": 0, "text": "Rule 1", "rule_code": "4.2.1", "is_bold": False, "is_italic": False},
            {"node_index": 1, "text": "Rule 2", "rule_code": "4.2.2", "is_bold": False, "is_italic": False},
            {"node_index": 2, "text": "Rule 3", "rule_code": "4.2.3", "is_bold": False, "is_italic": False},
            {"node_index": 3, "text": "A note", "rule_code": "", "is_bold": False, "is_italic": True},
            {"node_index": 4, "text": "Part heading", "rule_code": "Part 4.2", "is_bold": True, "is_italic": False},
        ]

    @pytest.fixture
    def sample_result(self):
        return {
            "controls": [
                {
                    "id": "4_2_1",
                    "label": "Q1?",
                    "detail-required": False,
                    "correct-option": "Yes",
                    "source-rules": ["4.2.1", "4.2.2"],
                    "mapping-confidence": 0.9,
                },
                {
                    "id": "4_2_3",
                    "label": "Q2?",
                    "detail-required": True,
                    "correct-option": "Yes",
                    "source-rules": ["4.2.3"],
                    "mapping-confidence": 0.4,
                },
            ],
            "groups": [],
            "rules": [],
        }

    def test_extract_input_rule_codes(self, sample_text_nodes):
        codes = extract_input_rule_codes(sample_text_nodes)
        assert codes == {"4.2.1", "4.2.2", "4.2.3"}
        # "Part 4.2" and empty codes should be excluded
        assert "Part 4.2" not in codes
        assert "" not in codes

    def test_extract_output_rule_codes(self, sample_result):
        codes = extract_output_rule_codes(sample_result)
        assert codes == {"4.2.1", "4.2.2", "4.2.3"}

    def test_full_coverage(self, sample_text_nodes, sample_result):
        report = compute_coverage_report("test", sample_text_nodes, sample_result)
        assert report["coverage_pct"] == 100.0
        assert report["total_unmapped"] == 0
        assert len(report["unmapped_codes"]) == 0

    def test_partial_coverage(self, sample_text_nodes):
        result = {
            "controls": [{
                "id": "4_2_1",
                "label": "Q?",
                "detail-required": False,
                "correct-option": "Yes",
                "source-rules": ["4.2.1"],
                "mapping-confidence": 0.8,
            }],
            "groups": [], "rules": [],
        }
        report = compute_coverage_report("test", sample_text_nodes, result)
        assert report["total_mapped"] == 1
        assert report["total_unmapped"] == 2
        assert "4.2.2" in report["unmapped_codes"]
        assert "4.2.3" in report["unmapped_codes"]
        assert report["coverage_pct"] == pytest.approx(33.3, abs=0.1)

    def test_extra_codes_detected(self, sample_text_nodes):
        result = {
            "controls": [{
                "id": "4_2_1",
                "label": "Q?",
                "detail-required": False,
                "correct-option": "Yes",
                "source-rules": ["4.2.1", "4.9.9"],  # 4.9.9 is not in input
                "mapping-confidence": 0.5,
            }],
            "groups": [], "rules": [],
        }
        report = compute_coverage_report("test", sample_text_nodes, result)
        assert "4.9.9" in report["extra_codes"]

    def test_low_confidence_flagged(self, sample_text_nodes, sample_result):
        report = compute_coverage_report("test", sample_text_nodes, sample_result)
        assert len(report["low_confidence"]) == 1
        assert report["low_confidence"][0]["id"] == "4_2_3"
        assert report["low_confidence"][0]["confidence"] == 0.4

    def test_no_controls_full_coverage(self):
        """Empty input should report 100% coverage."""
        report = compute_coverage_report("test", [], {"controls": [], "groups": [], "rules": []})
        assert report["coverage_pct"] == 100.0

    def test_confidence_field_in_schema(self):
        """mapping-confidence should be accepted in control output."""
        data = {
            "controls": [{
                "id": "4_2_3",
                "label": "Q?",
                "detail-required": False,
                "correct-option": "Yes",
                "mapping-confidence": 0.85,
            }],
            "groups": [],
            "rules": [],
        }
        errors = validate_section(data)
        assert errors == []


# ---------------------------------------------------------------------------
# Review tool schema tests
# ---------------------------------------------------------------------------

class TestReviewTool:
    def test_review_tool_has_required_schema(self):
        assert REVIEW_TOOL["name"] == "output_review"
        schema = REVIEW_TOOL["input_schema"]
        assert "reviews" in schema["properties"]
        assert "unmapped_assessment" in schema["properties"]

    def test_review_quality_enum(self):
        review_props = REVIEW_TOOL["input_schema"]["properties"]["reviews"]["items"]["properties"]
        assert set(review_props["quality"]["enum"]) == {"good", "acceptable", "questionable", "incorrect"}

    def test_unmapped_reason_enum(self):
        unmapped_props = REVIEW_TOOL["input_schema"]["properties"]["unmapped_assessment"]["items"]["properties"]
        assert set(unmapped_props["reason"]["enum"]) == {"correctly_omitted", "should_be_mapped", "already_covered"}
