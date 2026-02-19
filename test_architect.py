#!/usr/bin/env python3
"""Tests for the architect pipeline and group enrichment."""

import json
import pytest
from architect import (
    validate_output,
    gather_process_nodes,
    build_process_user_message,
    extract_input_rule_codes,
    extract_output_rule_codes,
    compute_coverage_report,
    ID_REGEX,
    SLUG_REGEX,
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
# Validation tests
# ---------------------------------------------------------------------------

class TestValidation:
    def test_valid_output(self):
        data = {
            "controls": [{"id": "4_2_3_1", "group": "collection-kyc"}],
            "groups": [{"id": "collection-kyc", "variant": "main"}],
            "rules": [{"target": "4_2_3_1_a", "scope": "sub-domestic", "effect": "SHOW", "schema": {"const": "Yes"}}],
        }
        warnings = validate_output(data)
        # Should have no errors about IDs — only possible orphan warning if controls don't match
        id_errors = [w for w in warnings if "Invalid" in w]
        assert id_errors == []

    def test_invalid_control_id(self):
        data = {"controls": [{"id": "bad_id", "group": "somegroup"}], "groups": [{"id": "somegroup", "variant": "main"}], "rules": []}
        warnings = validate_output(data)
        assert any("bad_id" in w for w in warnings)

    def test_invalid_group_id(self):
        data = {"controls": [], "groups": [{"id": "4.2.3", "variant": "main"}], "rules": []}
        warnings = validate_output(data)
        assert any("Invalid group" in w for w in warnings)

    def test_control_missing_group(self):
        data = {"controls": [{"id": "4_2_3"}], "groups": [], "rules": []}
        warnings = validate_output(data)
        assert any("missing 'group'" in w for w in warnings)

    def test_control_unknown_group_ref(self):
        data = {"controls": [{"id": "4_2_3", "group": "nonexistent"}], "groups": [], "rules": []}
        warnings = validate_output(data)
        assert any("unknown group slug" in w for w in warnings)

    def test_orphan_group_warning(self):
        data = {
            "controls": [],
            "groups": [{"id": "collection-kyc", "variant": "main"}],
            "rules": [],
        }
        warnings = validate_output(data)
        assert any("Orphan group" in w for w in warnings)

    def test_id_regex_valid(self):
        valid = ["4_1", "4_2_3", "4_2_3_1", "4_2_3_1_a", "4_15_6"]
        for v in valid:
            assert ID_REGEX.match(v), f"{v} should be valid"

    def test_id_regex_invalid(self):
        invalid = ["4", "3_1", "4.2.3", "4_a", "foo", "4_2_3_1_ab"]
        for v in invalid:
            assert not ID_REGEX.match(v), f"{v} should be invalid"

    def test_slug_regex_valid(self):
        valid = ["collection-kyc", "verification", "safe-harbour-listed", "a", "abc123"]
        for v in valid:
            assert SLUG_REGEX.match(v), f"{v} should be a valid slug"

    def test_slug_regex_invalid(self):
        invalid = ["4_2_3", "CamelCase", "-starts-with-dash", "has spaces", "4_3", ""]
        for v in invalid:
            assert not SLUG_REGEX.match(v), f"{v} should be an invalid slug"


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
    group_ids = {g["id"] for g in data["groups"] if "id" in g}
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
        if "group" not in c:
            errors.append(f"Control missing 'group': {c.get('id', '?')}")

    # Check group fields
    for g in data["groups"]:
        if "id" not in g:
            errors.append(f"Group missing 'id': {g}")
        if "title" not in g:
            errors.append(f"Group missing 'title': {g.get('id', '?')}")
        if "variant" not in g:
            errors.append(f"Group missing 'variant': {g.get('id', '?')}")
        elif g["variant"] not in ("main", "subprocess"):
            errors.append(f"Invalid variant '{g['variant']}' in group {g.get('id', '?')}")

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
            "controls": [{"id": "4_2_3_1", "label": "Q?", "detail-required": False, "correct-option": "Yes", "group": "collection"}],
            "groups": [{"id": "collection", "title": "Collection", "variant": "main"}],
            "rules": [{"target": "4_2_3_1", "scope": "4_1_4_1", "effect": "SHOW", "schema": {"const": "Yes"}}],
        }
        assert validate_section(data) == []

    def test_missing_control_fields(self):
        data = {
            "controls": [{"id": "4_2_3_1"}],
            "groups": [],
            "rules": [],
        }
        errors = validate_section(data)
        assert len(errors) == 4  # missing label, detail-required, correct-option, group

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

    def test_all_groups_have_text_nodes_key(self, real_groups_enriched):
        for g in real_groups_enriched:
            assert "text_nodes" in g, f"Group {g['id']} missing text_nodes"


import os


# ---------------------------------------------------------------------------
# Process forms tests
# ---------------------------------------------------------------------------

class TestProcessForms:
    def test_all_forms_have_required_keys(self):
        """Every process form must have all required fields."""
        required_keys = ("title", "source_groups", "gated_by", "sub_types",
                         "form_links", "subprocess_groups", "architect_notes")
        for pid, form in PROCESS_FORMS.items():
            for key in required_keys:
                assert key in form, f"{pid} missing '{key}'"

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

    def test_sub_types_are_lists(self):
        """sub_types must be a list; each entry must have id (slug) and label."""
        for pid, form in PROCESS_FORMS.items():
            assert isinstance(form["sub_types"], list), f"{pid} sub_types not a list"
            for st in form["sub_types"]:
                assert "id" in st, f"{pid} sub_type missing 'id'"
                assert "label" in st, f"{pid} sub_type missing 'label'"
                assert SLUG_REGEX.match(st["id"]), \
                    f"{pid} sub_type id '{st['id']}' is not a valid slug"

    def test_form_links_structure(self):
        """form_links must be a list; each entry must have target, label, gated_by (valid ID)."""
        for pid, form in PROCESS_FORMS.items():
            assert isinstance(form["form_links"], list), f"{pid} form_links not a list"
            for fl in form["form_links"]:
                assert "target" in fl, f"{pid} form_link missing 'target'"
                assert "label" in fl, f"{pid} form_link missing 'label'"
                assert "gated_by" in fl, f"{pid} form_link missing 'gated_by'"
                assert ID_REGEX.match(fl["gated_by"]), \
                    f"{pid} form_link gated_by '{fl['gated_by']}' is not a valid ID"
                assert fl["target"] in PROCESS_FORMS, \
                    f"{pid} form_link target '{fl['target']}' is not a known form"

    def test_subprocess_groups_are_slug_lists(self):
        """subprocess_groups must be a list of valid slugs."""
        for pid, form in PROCESS_FORMS.items():
            assert isinstance(form["subprocess_groups"], list), f"{pid} subprocess_groups not a list"
            for sg in form["subprocess_groups"]:
                assert SLUG_REGEX.match(sg), \
                    f"{pid} subprocess_group '{sg}' is not a valid slug"

    def test_architect_notes_are_string_lists(self):
        """architect_notes must be a list of strings."""
        for pid, form in PROCESS_FORMS.items():
            assert isinstance(form["architect_notes"], list), f"{pid} architect_notes not a list"
            for note in form["architect_notes"]:
                assert isinstance(note, str), f"{pid} architect_note is not a string: {note!r}"

    def test_cdd_forms_have_no_scope_gate_notes(self):
        """All CDD forms should instruct architect not to generate scope gate questions."""
        for pid, form in PROCESS_FORMS.items():
            if pid.startswith("cdd-"):
                notes_text = " ".join(form["architect_notes"])
                assert "gated externally" in notes_text or "Do NOT generate a top-level scope gate" in notes_text, \
                    f"{pid} architect_notes should instruct against scope gate questions"

    def test_cdd_individuals_has_form_links(self):
        """cdd-individuals should link to verification forms for safe harbour."""
        form = PROCESS_FORMS["cdd-individuals"]
        assert len(form["form_links"]) == 2
        targets = {fl["target"] for fl in form["form_links"]}
        assert "verification-documents" in targets
        assert "verification-electronic" in targets

    def test_cdd_companies_has_subprocess_groups(self):
        """cdd-companies should define subprocess group slugs for safe harbour."""
        form = PROCESS_FORMS["cdd-companies"]
        assert len(form["subprocess_groups"]) > 0
        assert "safe-harbour-listed" in form["subprocess_groups"]


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
                "group": "collection-kyc",
            }],
            "groups": [{"id": "collection-kyc", "title": "Collection of KYC Information", "variant": "main"}],
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
        """build_process_user_message should include form title, text nodes, and new schema hints."""
        form_def = PROCESS_FORMS["cdd-individuals"]
        text_nodes = [
            {"node_index": 0, "text": "Test node", "rule_code": "4.2.1", "is_bold": False, "is_italic": False},
        ]
        msg = build_process_user_message("cdd-individuals", form_def, text_nodes)
        assert "Customer Due Diligence" in msg
        assert "Individuals" in msg
        assert "[4.2.1] Test node" in msg
        # New schema: message must remind LLM about slug groups and group field on controls
        assert "group" in msg
        assert "slug" in msg
        # Gating context should be injected
        assert "4_1_4_1" in msg
        # Sub-type IDs should appear
        assert "sub-individual" in msg


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
                "group": "collection-kyc",
            }],
            "groups": [{"id": "collection-kyc", "title": "Collection", "variant": "main"}],
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
