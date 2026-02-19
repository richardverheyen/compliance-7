#!/usr/bin/env python3
"""
Rules Architect Agent — LLM-powered pipeline that processes enriched groups
and produces compliance form data files organized by business process.

Usage:
    python architect.py runs/1                                    # All process forms
    python architect.py runs/1 --process cdd-individuals          # Single process
    python architect.py runs/1 --dry-run                          # Print prompts only
    python architect.py runs/1 --model claude-sonnet-4-5-20250929 # Override model
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict

import anthropic
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ID_REGEX = re.compile(r"^4(_\d+)+(_[a-z])?$")
SLUG_REGEX = re.compile(r"^[a-z][a-z0-9-]*$")

PROCESSES = {
    "PROC-AML-001": "Agent Management Program",
    "PROC-AML-002": "Customer Identification Procedure (General)",
    "PROC-AML-002a": "Collection of KYC Information",
    "PROC-AML-002b": "Verification of Identity",
    "PROC-AML-002c": "ML/TF Risk Assessment",
    "PROC-AML-003": "Enhanced Due Diligence",
    "PROC-AML-003a": "Beneficial Ownership Identification",
    "PROC-AML-003b": "PEP Screening",
    "PROC-AML-003c": "Ongoing Customer Due Diligence",
    "PROC-AML-004": "Record Keeping",
    "PROC-AML-004a": "Record Retention",
    "PROC-AML-004b": "Record Accessibility",
}

MODEL_SMALL = "claude-haiku-4-5-20251001"
MODEL_LARGE = "claude-sonnet-4-5-20250929"
TEXT_NODE_THRESHOLD = 50  # groups with >= this many nodes use the large model

# ---------------------------------------------------------------------------
# Process forms map — one form per business process step
# ---------------------------------------------------------------------------

PROCESS_FORMS = {
    "cdd-individuals": {
        "title": "Customer Due Diligence — Individuals",
        "source_groups": ["4_2"],
        "gated_by": "4_1_4_1",
        "sub_types": [
            {"id": "sub-individual", "label": "Individuals"},
            {"id": "sub-sole-trader", "label": "Sole Traders"},
        ],
        "form_links": [
            {"target": "verification-documents", "label": "Documentary Safe Harbour", "gated_by": "4_2_10"},
            {"target": "verification-electronic", "label": "Electronic Safe Harbour", "gated_by": "4_2_12"},
        ],
        "subprocess_groups": [],
        "architect_notes": [
            "This form is already gated externally — the intro form has confirmed the entity serves individual customers. Do NOT generate a top-level scope gate question (e.g. 'Do you have individual customers?'). Begin directly with the KYC collection and verification obligations.",
            "Sub-types are pre-defined: Individuals (id: sub-individual) and Sole Traders (id: sub-sole-trader). Use these exact IDs in any gating rules. Gate the individuals-specific KYC collection control on sub-individual and the sole-trader-specific control on sub-sole-trader.",
            "Safe harbour procedures (rules 4.2.10 through 4.2.14) are handled via form-links to the verification-documents and verification-electronic forms. Do NOT generate controls for these rules — they are excluded from this form.",
            "Pre-commencement customers (rule 4.1.2) are handled externally via an onboarding diagram. Do not generate a control for 4.1.2.",
        ],
    },
    "cdd-companies": {
        "title": "Customer Due Diligence — Companies",
        "source_groups": ["4_3"],
        "gated_by": "4_1_4_2",
        "sub_types": [
            {"id": "sub-domestic", "label": "Domestic Companies"},
            {"id": "sub-reg-foreign", "label": "Registered Foreign Companies"},
            {"id": "sub-unreg-foreign", "label": "Unregistered Foreign Companies"},
        ],
        "form_links": [],
        "subprocess_groups": ["safe-harbour-listed", "foreign-listed", "disclosure-certificates"],
        "architect_notes": [
            "This form is already gated externally — the intro form has confirmed the entity serves company customers. Do NOT generate a top-level scope gate question (e.g. 'Does your entity have company customers?'). Begin directly with the KYC obligations.",
            "Sub-types are pre-defined: Domestic Companies (id: sub-domestic), Registered Foreign Companies (id: sub-reg-foreign), Unregistered Foreign Companies (id: sub-unreg-foreign). Use these exact IDs in gating rules. Gate KYC collection controls for each company type on the corresponding sub-type ID.",
            "The simplified verification procedure (4.3.8) for listed public companies and their subsidiaries should be a group with variant 'subprocess'. The foreign listed public company procedure (4.3.9) should also be a subprocess group. Disclosure certificates (4.3.11–4.3.13) should be a subprocess group.",
            "Pre-commencement customers (rule 4.1.2) are handled externally. Do not generate a control for 4.1.2.",
        ],
    },
    "cdd-trusts": {
        "title": "Customer Due Diligence — Trusts",
        "source_groups": ["4_4"],
        "gated_by": "4_1_4_3",
        "sub_types": [
            {"id": "sub-private-trust", "label": "Private Trusts"},
            {"id": "sub-asic-mis", "label": "ASIC-registered Managed Investment Schemes"},
            {"id": "sub-govt-super", "label": "Government Superannuation Funds"},
        ],
        "form_links": [],
        "subprocess_groups": ["simplified-trustee-verification", "custodians-nominees"],
        "architect_notes": [
            "This form is already gated externally. Do NOT generate a top-level scope gate question.",
            "Sub-types are pre-defined: Private Trusts (id: sub-private-trust), ASIC-registered MIS (id: sub-asic-mis), Government Superannuation Funds (id: sub-govt-super). Use these exact IDs in gating rules for simplified verification eligibility (4.4.8, 4.4.13).",
            "The simplified trustee verification procedure (4.4.8, 4.4.13) and the custodians/nominees of custodians section (4.4.18) should each be a group with variant 'subprocess'.",
            "Trustee composition distinctions (individual vs company trustees) should be addressed within controls, not as separate sub-types.",
        ],
    },
    "cdd-partnerships": {
        "title": "Customer Due Diligence — Partnerships",
        "source_groups": ["4_5"],
        "gated_by": "4_1_4_4",
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [
            "This form is already gated externally. Do NOT generate a top-level scope gate question.",
            "No sub-types are defined for partnerships. Partner composition distinctions (individual vs entity partners) should be handled within individual controls.",
            "Pre-commencement customers (rule 4.1.2) are handled externally. Do not generate a control for 4.1.2.",
        ],
    },
    "cdd-associations": {
        "title": "Customer Due Diligence — Associations",
        "source_groups": ["4_6"],
        "gated_by": "4_1_4_5",
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [
            "This form is already gated externally. Do NOT generate a top-level scope gate question.",
            "No sub-types are defined for associations. Member composition distinctions should be handled within controls.",
        ],
    },
    "cdd-cooperatives": {
        "title": "Customer Due Diligence — Co-operatives",
        "source_groups": ["4_7"],
        "gated_by": "4_1_4_6",
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [
            "This form is already gated externally. Do NOT generate a top-level scope gate question.",
            "No sub-types defined. Rules apply uniformly to all co-operatives.",
        ],
    },
    "cdd-government": {
        "title": "Customer Due Diligence — Government Bodies",
        "source_groups": ["4_8"],
        "gated_by": "4_1_4_7",
        "sub_types": [
            {"id": "sub-domestic-govt", "label": "Domestic Government Bodies"},
            {"id": "sub-foreign-govt", "label": "Foreign Government Bodies"},
        ],
        "form_links": [],
        "subprocess_groups": ["foreign-government-entities"],
        "architect_notes": [
            "This form is already gated externally. Do NOT generate a top-level scope gate question.",
            "Sub-types are pre-defined: Domestic Government Bodies (id: sub-domestic-govt) and Foreign Government Bodies (id: sub-foreign-govt). Gate beneficial ownership requirements (4.8) on sub-foreign-govt.",
            "The foreign government entities section should be a group with variant 'subprocess'.",
        ],
    },
    "risk-assessment": {
        "title": "ML/TF Risk Assessment",
        "source_groups": ["4_1"],
        "gated_by": None,
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [
            "Pre-commencement customers (rule 4.1.2) are handled externally via an onboarding diagram. Do not generate a control for 4.1.2.",
        ],
    },
    "verification-documents": {
        "title": "Verification Standards — Documents",
        "source_groups": ["4_9"],
        "gated_by": None,
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [],
    },
    "verification-electronic": {
        "title": "Verification Standards — Electronic",
        "source_groups": ["4_10"],
        "gated_by": None,
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [],
    },
    "agent-management": {
        "title": "Agent Management",
        "source_groups": ["4_11"],
        "gated_by": "4_1_8",
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [
            "This form is already gated externally. Do NOT generate a top-level scope gate question.",
        ],
    },
    "beneficial-ownership": {
        "title": "Beneficial Ownership",
        "source_groups": ["4_12"],
        "gated_by": "4_1_5_1",
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [
            "This form is already gated externally. Do NOT generate a top-level scope gate question.",
        ],
    },
    "pep-screening": {
        "title": "PEP Screening",
        "source_groups": ["4_13"],
        "gated_by": "4_1_5_2",
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [
            "This form is already gated externally. Do NOT generate a top-level scope gate question.",
        ],
    },
    "record-keeping": {
        "title": "Record Keeping",
        "source_groups": ["4_14"],
        "gated_by": None,
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [],
    },
    "alternative-id": {
        "title": "Alternative Identity Proofing",
        "source_groups": ["4_15"],
        "gated_by": None,
        "sub_types": [],
        "form_links": [],
        "subprocess_groups": [],
        "architect_notes": [],
    },
}

# ---------------------------------------------------------------------------
# Tool schema for structured output
# ---------------------------------------------------------------------------

OUTPUT_TOOL = {
    "name": "output_section_data",
    "description": "Output the structured section data (controls, groups, rules) for the current regulatory group.",
    "input_schema": {
        "type": "object",
        "properties": {
            "controls": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "description": "Hierarchical regulatory ID using underscore notation, e.g. '4_2_3_1'. Must match the pattern 4_N_N... derived from regulatory rule codes."},
                        "group": {"type": "string", "description": "Semantic slug identifying which process-step group this control belongs to, e.g. 'collection-kyc', 'verification', 'safe-harbour-listed'. Must exactly match a group id in the groups array. Use lowercase letters and hyphens only."},
                        "label": {"type": "string", "description": "The compliance question presented to the user"},
                        "detail-required": {"type": "boolean", "description": "Whether answering Yes requires supporting detail"},
                        "correct-option": {"type": "string", "enum": ["Yes", "No", "N/A"], "description": "Expected correct answer for compliance"},
                        "detail-label": {"type": "string", "description": "Custom label for the detail text input"},
                        "process-id": {"type": "string", "description": "Business process ID from PROCESSES map"},
                        "source-rules": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Regulation rule codes this control covers, e.g. ['4.3.5(1)', '4.3.5(2)']",
                        },
                        "mapping-confidence": {
                            "type": "number",
                            "description": "Confidence in this mapping (0.0-1.0). 1.0=direct unambiguous, 0.7=clear but aggregated, 0.5=reasonable interpretation, <0.5=uncertain",
                        },
                    },
                    "required": ["id", "group", "label", "detail-required", "correct-option"],
                },
            },
            "groups": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "description": "Semantic slug ID for this process-step group, e.g. 'collection-kyc', 'verification', 'safe-harbour-listed'. Use lowercase letters and hyphens only. Do NOT use 4_x numbers as group IDs."},
                        "title": {"type": "string", "description": "Display title for this process step"},
                        "description": {"type": "string", "description": "Explanatory text shown beneath the group heading"},
                        "variant": {"type": "string", "enum": ["main", "subprocess"], "description": "'main' for primary process steps. 'subprocess' for secondary or optional paths (e.g. safe harbour procedures, disclosure certificates) that are visually nested."},
                        "subprocess-label": {"type": "string", "description": "Short label shown on the subprocess visual indicator, e.g. 'Safe Harbour', 'Foreign Listed'"},
                    },
                    "required": ["id", "title", "variant"],
                },
            },
            "rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "target": {"type": "string", "description": "Control ID (4_x format) or group slug whose visibility is affected"},
                        "scope": {"type": "string", "description": "Control ID or sub-type ID whose answer determines visibility"},
                        "effect": {"type": "string", "enum": ["SHOW", "HIDE"]},
                        "schema": {
                            "type": "object",
                            "properties": {"const": {"type": "string"}},
                            "required": ["const"],
                        },
                    },
                    "required": ["target", "scope", "effect", "schema"],
                },
            },
        },
        "required": ["controls", "groups", "rules"],
    },
}

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

PROCESS_SYSTEM_PROMPT = f"""You are a compliance **process** architect. Your job is to analyse Australian AML/CTF regulatory text and produce structured form data organized by **business process step**, not by regulation sub-section.

## Output Types

### Control
A compliance control point (question) within the form:
- id: Hierarchical regulatory ID, e.g. "4_2_3_1". MUST match the pattern 4_N_N... derived from the regulatory text's rule codes.
- group: REQUIRED. Semantic slug of the process-step group this control belongs to, e.g. "collection-kyc". Must exactly match a group id in your groups array.
- label: A clear Yes/No compliance question derived from the regulatory requirement.
- detail-required: true if answering "Yes" should prompt the user to explain HOW they comply.
- correct-option: "Yes" if compliance requires this, "No" if it should not happen, "N/A" for informational questions.
- detail-label: (optional) Custom label for the detail text input.
- process-id: (optional) Business process ID from the PROCESSES map below.
- source-rules: REQUIRED array of regulation rule codes this control derives from.
- mapping-confidence: REQUIRED number 0.0-1.0 indicating confidence in the regulation-to-control mapping.

### Group
An organisational container representing a **process step**. Groups use semantic slugs — NOT regulation section numbers:
- id: Semantic slug, e.g. "collection-kyc", "verification", "safe-harbour-listed". Lowercase letters and hyphens only. NEVER use 4_x numbers as group IDs.
- title: Display title for this process step.
- description: (optional) Explanatory text.
- variant: REQUIRED. "main" for primary sequential steps. "subprocess" for optional or secondary paths (safe harbour, disclosure certificates, foreign entity procedures). Subprocess groups render visually nested with a left border and tinted background.
- subprocess-label: (optional) Short label for the subprocess indicator, e.g. "Safe Harbour", "Foreign Listed".

### Rule
Conditional visibility logic:
- target: The control ID (4_x format) or group slug whose visibility is affected.
- scope: The control ID or sub-type ID whose answer determines visibility.
- effect: "SHOW" or "HIDE".
- schema: {{ "const": "Yes" }} or similar.

## Sub-Type Gating

When sub-types are provided (e.g. Domestic / Registered Foreign / Unregistered Foreign companies), generate SHOW rules that gate sub-type-specific controls on the corresponding sub-type ID. Sub-type IDs use the format provided in the prompt (e.g. "sub-domestic"). A SHOW rule with scope "sub-domestic" will show the control only when the user has selected Domestic Companies in the sub-scoping panel.

## Process Step Organisation

Organise controls into groups representing **process steps**, NOT regulation sub-sections. For CDD forms, use this pattern:

1. **General CDD Obligation** (variant: main) — top-level risk-based obligation
2. **Collection of KYC Information** (variant: main) — minimum + additional collection
3. **Verification of Information** (variant: main) — how to verify what was collected
4. **Additional KYC Assessment** (variant: main) — risk-based decisions on extra collection/verification
5. **Safe Harbour Procedures** (variant: subprocess) — only if NOT handled via form-links
6. **Discrepancy Handling** (variant: main) — responding to verification issues

For non-CDD forms, organise by logical workflow steps.

## Control Creation Guidelines

1. **Aggregate** related procedural requirements into single controls where possible.
2. **Don't create 1:1 controls** for every text node. Group related requirements.
3. Use **detail-required: true** for "how" questions where the user should explain their process.
4. Use **detail-required: false** for simple yes/no compliance checks.
5. **Skip** italic/note text — use it as group descriptions instead.
6. Every control MUST include **source-rules** and **mapping-confidence**.
7. Every control MUST include a **group** field matching a group slug in your output.

## Critical Rules

### Control IDs — REQUIRED format
Control IDs MUST match: ^4(_\\d+)+(_[a-z])?$
- Derive from regulatory rule codes (e.g. rule 4.3.5(1) → id "4_3_5_1")
- NEVER invent descriptive suffixes
- Valid: 4_2, 4_2_3, 4_2_3_1, 4_2_3_1_a
- Invalid: 4_3_8_simplified, 4_2_collection

### Group IDs — semantic slugs ONLY
Group IDs MUST match: ^[a-z][a-z0-9-]*$
- Use descriptive slugs: "collection-kyc", "verification", "safe-harbour-listed"
- NEVER use 4_x numbers as group IDs
- Valid: "collection-kyc", "verification", "discrepancy-handling"
- Invalid: "4_3", "4_3_3", "4_2_5"

## Process IDs

{json.dumps(PROCESSES, indent=2)}
"""


def gather_process_nodes(process_id: str, groups: list[dict]) -> list[dict]:
    """Gather all text nodes for a process form from its source groups."""
    form_def = PROCESS_FORMS[process_id]
    group_map = {g["id"]: g for g in groups}
    all_nodes = []

    for prefix in form_def["source_groups"]:
        if prefix in group_map:
            nodes = group_map[prefix].get("text_nodes", [])
            all_nodes.extend(nodes)

    return all_nodes


def build_process_user_message(process_id: str, form_def: dict, text_nodes: list[dict]) -> str:
    """Build the user message for a process-mode LLM call."""
    # Format text nodes
    nodes_text = ""
    for tn in text_nodes:
        prefix = f"[{tn['rule_code']}] " if tn["rule_code"] else ""
        style = ""
        if tn["is_bold"]:
            style = " **BOLD**"
        elif tn["is_italic"]:
            style = " *ITALIC/NOTE*"
        nodes_text += f"  {prefix}{tn['text']}{style}\n"

    # Gating context
    gating_section = ""
    if form_def.get("gated_by"):
        gating_section = f"""
## Gating Context
This form is gated by intro answer `{form_def['gated_by']}`. The user has already confirmed they serve the relevant customer type via the introduction form. Do NOT generate a scope gate question — the form's visibility is managed externally.
"""

    # Sub-types
    sub_types_section = ""
    if form_def.get("sub_types"):
        sub_types_json = json.dumps(form_def["sub_types"], indent=2)
        sub_type_ids = [st["id"] for st in form_def["sub_types"]]
        sub_types_section = f"""
## Sub-Type Definitions (Pre-defined — do not modify)
Generate sub_scoping entries that match these exactly. Use the id values in SHOW rules to gate sub-type-specific controls:
{sub_types_json}

Sub-type IDs to use in rules: {sub_type_ids}
When a control applies only to one sub-type, add a SHOW rule with scope = <sub-type-id> and schema.const = "Yes".
"""

    # Subprocess groups
    subprocess_section = ""
    if form_def.get("subprocess_groups"):
        subprocess_section = f"""
## Subprocess Group Hints
The following process steps should be marked with variant "subprocess" (visual nesting with left border):
{json.dumps(form_def['subprocess_groups'], indent=2)}
Use these as guidance for which groups to mark as subprocess. You may use different slug names if more appropriate, but keep the same semantic intent.
"""

    # Form links (excluded rules)
    form_links_section = ""
    if form_def.get("form_links"):
        excluded_rules = []
        for fl in form_def["form_links"]:
            excluded_rules.append(f"  - {fl['label']} (linked form: {fl['target']}, gated by: {fl.get('gated_by', 'none')})")
        form_links_section = f"""
## Form Links (Excluded from this form)
The following sub-processes are handled via links to separate forms. Do NOT generate controls for the regulatory text that covers these:
{chr(10).join(excluded_rules)}
"""

    # Architect notes (human feedback)
    notes_section = ""
    if form_def.get("architect_notes"):
        notes_lines = "\n".join(f"- {note}" for note in form_def["architect_notes"])
        notes_section = f"""
## Architect Notes (Follow these precisely)
{notes_lines}
"""

    return f"""## Process Form: {form_def['title']}
Process ID: {process_id}
{gating_section}{sub_types_section}{subprocess_section}{form_links_section}{notes_section}
## Regulatory Text ({len(text_nodes)} text nodes)
{nodes_text}
Analyse the regulatory text above and produce the controls, groups, and rules for the "{form_def['title']}" process form. Remember:
- Every control MUST have a "group" field matching a slug in your groups array
- Group IDs MUST be semantic slugs (e.g. "collection-kyc"), NEVER 4_x numbers
- Use variant "subprocess" for optional/secondary process paths
"""


# ---------------------------------------------------------------------------
# Coverage audit — post-processing diff of input vs output rule codes
# ---------------------------------------------------------------------------


def extract_input_rule_codes(text_nodes: list[dict]) -> set[str]:
    """Extract unique rule codes from input text nodes."""
    codes = set()
    for tn in text_nodes:
        code = tn.get("rule_code", "").strip()
        if code and not code.startswith("Part "):
            codes.add(code)
    return codes


def extract_output_rule_codes(result: dict) -> set[str]:
    """Extract unique rule codes from output controls' source-rules."""
    codes = set()
    for ctrl in result.get("controls", []):
        for code in ctrl.get("source-rules", []):
            codes.add(code)
    return codes


def compute_coverage_report(process_id: str, text_nodes: list[dict], result: dict) -> dict:
    """Compare input rule codes against output source-rules to find coverage gaps."""
    input_codes = extract_input_rule_codes(text_nodes)
    output_codes = extract_output_rule_codes(result)

    mapped = input_codes & output_codes
    unmapped = input_codes - output_codes
    extra = output_codes - input_codes

    coverage_pct = (len(mapped) / len(input_codes) * 100) if input_codes else 100.0

    low_confidence = []
    for ctrl in result.get("controls", []):
        conf = ctrl.get("mapping-confidence")
        if conf is not None and conf < 0.5:
            low_confidence.append({
                "id": ctrl["id"],
                "label": ctrl["label"],
                "confidence": conf,
                "source-rules": ctrl.get("source-rules", []),
            })

    return {
        "process_id": process_id,
        "input_codes": sorted(input_codes),
        "mapped_codes": sorted(mapped),
        "unmapped_codes": sorted(unmapped),
        "extra_codes": sorted(extra),
        "coverage_pct": round(coverage_pct, 1),
        "total_input": len(input_codes),
        "total_mapped": len(mapped),
        "total_unmapped": len(unmapped),
        "total_controls": len(result.get("controls", [])),
        "low_confidence": low_confidence,
    }


def log_coverage_report(report: dict):
    """Log a coverage report summary."""
    pid = report["process_id"]
    pct = report["coverage_pct"]
    unmapped = report["total_unmapped"]
    extra = len(report["extra_codes"])
    low_conf = len(report["low_confidence"])

    level = logging.INFO if pct >= 90 and unmapped == 0 else logging.WARNING
    logger.log(level,
        f"  Coverage: {pct}% ({report['total_mapped']}/{report['total_input']} rules mapped)"
    )
    if unmapped > 0:
        logger.warning(f"  UNMAPPED ({unmapped}): {', '.join(report['unmapped_codes'])}")
    if extra > 0:
        logger.warning(f"  EXTRA ({extra}): {', '.join(report['extra_codes'])}")
    if low_conf > 0:
        logger.warning(f"  LOW CONFIDENCE ({low_conf}):")
        for lc in report["low_confidence"]:
            logger.warning(f"    {lc['id']} (conf={lc['confidence']}): {lc['label']}")


def call_process_architect(
    client: anthropic.Anthropic,
    process_id: str,
    form_def: dict,
    text_nodes: list[dict],
    model: str,
    dry_run: bool = False,
) -> dict | None:
    """Call the LLM for a process form and return parsed SectionData."""
    user_msg = build_process_user_message(process_id, form_def, text_nodes)

    if dry_run:
        print(f"\n{'='*60}")
        print(f"DRY RUN — Process: {process_id} | Model: {model}")
        print(f"{'='*60}")
        print(f"SYSTEM PROMPT: ({len(PROCESS_SYSTEM_PROMPT)} chars)")
        print(f"USER MESSAGE:\n{user_msg}")
        return None

    logger.info(f"Calling API for process {process_id} with model {model}...")

    response = client.messages.create(
        model=model,
        max_tokens=8192,
        system=PROCESS_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
        tools=[OUTPUT_TOOL],
        tool_choice={"type": "tool", "name": "output_section_data"},
    )

    for block in response.content:
        if block.type == "tool_use" and block.name == "output_section_data":
            data = block.input
            warnings = validate_output(data)
            for w in warnings:
                logger.warning(f"  {process_id}: {w}")
            data = strip_invalid_items(data)
            return data

    logger.error(f"No tool_use block in response for process {process_id}")
    return None


def inject_static_fields(result: dict, form_def: dict) -> dict:
    """Inject statically-defined sub_scoping and form_links into the result."""
    # Sub-scoping: always comes from PROCESS_FORMS, not from LLM
    result["sub_scoping"] = form_def.get("sub_types", [])

    # Form links: always comes from PROCESS_FORMS, not from LLM
    result["form_links"] = form_def.get("form_links", [])

    return result


def run_process_architect(run_dir: str, single_process: str | None = None,
                          dry_run: bool = False, model_override: str | None = None,
                          run_review: bool = False):
    """Process-mode pipeline: one LLM call per process form."""

    # Load data
    enriched_path = os.path.join(run_dir, "groups_enriched.json")
    if not os.path.exists(enriched_path):
        logger.error(f"groups_enriched.json not found in {run_dir}. Run 'python main.py enrich' first.")
        sys.exit(1)

    with open(enriched_path) as f:
        groups = json.load(f)

    # Determine which processes to run
    if single_process:
        if single_process not in PROCESS_FORMS:
            logger.error(f"Unknown process: {single_process}. Available: {', '.join(PROCESS_FORMS.keys())}")
            sys.exit(1)
        processes_to_run = {single_process: PROCESS_FORMS[single_process]}
    else:
        processes_to_run = PROCESS_FORMS

    # Create API client (unless dry run)
    client = None
    if not dry_run:
        client = anthropic.Anthropic()

    # Output directory
    processes_dir = os.path.join(run_dir, "processes")
    if not dry_run:
        os.makedirs(processes_dir, exist_ok=True)

    # Coverage reports accumulator
    coverage_reports: dict[str, dict] = {}

    total = len(processes_to_run)
    for i, (process_id, form_def) in enumerate(processes_to_run.items(), 1):
        # Gather text nodes
        text_nodes = gather_process_nodes(process_id, groups)

        if not text_nodes:
            logger.info(f"[{i}/{total}] Skipping {process_id} (no text nodes)")
            continue

        # Select model
        if model_override:
            model = model_override
        else:
            model = MODEL_LARGE if len(text_nodes) >= TEXT_NODE_THRESHOLD else MODEL_SMALL

        logger.info(f"[{i}/{total}] Processing {process_id} ({len(text_nodes)} nodes, model={model.split('-')[1] if '-' in model else model})")

        result = call_process_architect(
            client, process_id, form_def, text_nodes, model, dry_run,
        )

        if result is None:
            continue

        # Inject static fields (sub_scoping, form_links)
        result = inject_static_fields(result, form_def)

        # Coverage audit
        report = compute_coverage_report(process_id, text_nodes, result)
        coverage_reports[process_id] = report
        log_coverage_report(report)

        # Add gating rule if this process is gated
        if form_def["gated_by"]:
            target_section = form_def["source_groups"][0]
            gating_rule = {
                "target": target_section,
                "scope": form_def["gated_by"],
                "effect": "SHOW",
                "schema": {"const": "Yes"},
            }
            result["rules"].insert(0, gating_rule)

        # Write output
        if not dry_run:
            output_path = os.path.join(processes_dir, f"{process_id}.json")
            with open(output_path, "w") as f:
                json.dump(result, f, indent=2)
            logger.info(
                f"Wrote {process_id}.json — "
                f"{len(result['controls'])} controls, "
                f"{len(result['groups'])} groups, "
                f"{len(result['rules'])} rules, "
                f"{len(result['sub_scoping'])} sub-types, "
                f"{len(result['form_links'])} form-links"
            )

        # Rate limiting
        if not dry_run:
            time.sleep(0.5)

    if not dry_run:
        # Write coverage audit report
        if coverage_reports:
            audit_path = os.path.join(processes_dir, "_coverage_audit.json")
            total_input = sum(r["total_input"] for r in coverage_reports.values())
            total_mapped = sum(r["total_mapped"] for r in coverage_reports.values())
            total_unmapped = sum(r["total_unmapped"] for r in coverage_reports.values())
            total_low_conf = sum(len(r["low_confidence"]) for r in coverage_reports.values())
            overall_pct = round(total_mapped / total_input * 100, 1) if total_input else 100.0

            audit_data = {
                "summary": {
                    "overall_coverage_pct": overall_pct,
                    "total_input_rules": total_input,
                    "total_mapped_rules": total_mapped,
                    "total_unmapped_rules": total_unmapped,
                    "total_low_confidence_controls": total_low_conf,
                    "processes_audited": len(coverage_reports),
                },
                "processes": coverage_reports,
            }
            with open(audit_path, "w") as f:
                json.dump(audit_data, f, indent=2)
            logger.info(f"Coverage audit → {audit_path}")

            print(f"\nCoverage: {overall_pct}% ({total_mapped}/{total_input} rules)")
            if total_unmapped > 0:
                print(f"  Unmapped rules: {total_unmapped}")
            if total_low_conf > 0:
                print(f"  Low confidence controls: {total_low_conf}")

        if run_review and coverage_reports:
            logger.info("Starting second-pass review...")
            review_results = run_review_pass(client, run_dir, groups, coverage_reports)
            review_path = os.path.join(processes_dir, "_review_results.json")
            with open(review_path, "w") as f:
                json.dump(review_results, f, indent=2)
            logger.info(f"Review results → {review_path}")

        print(f"\nDone! Process files written to {processes_dir}/")
        print(f"  Process forms: {total}")


# ---------------------------------------------------------------------------
# Second-pass review — independent LLM validation of mappings
# ---------------------------------------------------------------------------

REVIEW_TOOL = {
    "name": "output_review",
    "description": "Output the review results for each control mapping.",
    "input_schema": {
        "type": "object",
        "properties": {
            "reviews": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "control_id": {"type": "string"},
                        "quality": {
                            "type": "string",
                            "enum": ["good", "acceptable", "questionable", "incorrect"],
                        },
                        "confidence": {"type": "number"},
                        "issues": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                    },
                    "required": ["control_id", "quality", "confidence"],
                },
            },
            "unmapped_assessment": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "rule_code": {"type": "string"},
                        "reason": {
                            "type": "string",
                            "enum": ["correctly_omitted", "should_be_mapped", "already_covered"],
                        },
                        "explanation": {"type": "string"},
                    },
                    "required": ["rule_code", "reason"],
                },
            },
        },
        "required": ["reviews", "unmapped_assessment"],
    },
}

REVIEW_SYSTEM_PROMPT = """You are an independent compliance mapping reviewer. You are reviewing mappings produced by another LLM that converted regulatory text into compliance form controls.

Your job is to assess the quality of each control's mapping to the source regulation text.

For each control, evaluate:
1. Does the control's label accurately reflect the regulation text cited in source-rules?
2. Are the source-rules correct — do they actually relate to this control?
3. Is the control's correct-option appropriate?
4. Were any important requirements from the source rules missed or misrepresented?

Quality ratings:
- "good": Direct, accurate mapping. Source rules match. No issues.
- "acceptable": Reasonable mapping with minor imprecision.
- "questionable": Mapping has issues — wrong source rules, misleading label, or missed requirements.
- "incorrect": Fundamentally wrong mapping.

For unmapped rules, assess whether:
- "correctly_omitted": The rule is a heading, note, or doesn't warrant a control
- "should_be_mapped": The rule contains a substantive requirement that should have a control
- "already_covered": The rule's substance is covered by another control
"""


def run_review_pass(
    client: anthropic.Anthropic,
    run_dir: str,
    groups: list[dict],
    coverage_reports: dict[str, dict],
) -> dict:
    """Run second-pass review on process forms."""
    processes_dir = os.path.join(run_dir, "processes")
    all_reviews = {}

    for process_id, report in coverage_reports.items():
        process_path = os.path.join(processes_dir, f"{process_id}.json")
        if not os.path.exists(process_path):
            continue

        with open(process_path) as f:
            result = json.load(f)

        text_nodes = gather_process_nodes(process_id, groups)

        nodes_text = ""
        for tn in text_nodes:
            prefix = f"[{tn['rule_code']}] " if tn["rule_code"] else ""
            nodes_text += f"  {prefix}{tn['text']}\n"

        controls_text = ""
        for ctrl in result.get("controls", []):
            src = ", ".join(ctrl.get("source-rules", []))
            conf = ctrl.get("mapping-confidence", "N/A")
            controls_text += f"  {ctrl['id']} (group: {ctrl.get('group', '?')}): {ctrl['label']}\n"
            controls_text += f"    source-rules: [{src}]\n"
            controls_text += f"    mapping-confidence: {conf}\n"
            controls_text += f"    correct-option: {ctrl.get('correct-option', '?')}\n\n"

        unmapped_text = ""
        if report["unmapped_codes"]:
            unmapped_text = f"\n## Unmapped Rules ({len(report['unmapped_codes'])})\n"
            for code in report["unmapped_codes"]:
                matching = [tn for tn in text_nodes if tn.get("rule_code") == code]
                text = matching[0]["text"] if matching else "(text not found)"
                unmapped_text += f"  [{code}] {text}\n"

        user_msg = f"""## Review: {PROCESS_FORMS[process_id]['title']}

## Original Regulatory Text ({len(text_nodes)} nodes)
{nodes_text}

## Controls Produced ({len(result.get('controls', []))})
{controls_text}
{unmapped_text}

Review each control mapping and assess the unmapped rules.
"""

        logger.info(f"  Reviewing {process_id}...")

        response = client.messages.create(
            model=MODEL_SMALL,
            max_tokens=4096,
            system=REVIEW_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
            tools=[REVIEW_TOOL],
            tool_choice={"type": "tool", "name": "output_review"},
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == "output_review":
                review_data = block.input
                all_reviews[process_id] = review_data

                reviews = review_data.get("reviews", [])
                quality_counts = defaultdict(int)
                for r in reviews:
                    quality_counts[r["quality"]] += 1
                logger.info(
                    f"  Review: {quality_counts.get('good', 0)} good, "
                    f"{quality_counts.get('acceptable', 0)} acceptable, "
                    f"{quality_counts.get('questionable', 0)} questionable, "
                    f"{quality_counts.get('incorrect', 0)} incorrect"
                )

                unmapped = review_data.get("unmapped_assessment", [])
                should_map = [u for u in unmapped if u["reason"] == "should_be_mapped"]
                if should_map:
                    logger.warning(f"  {len(should_map)} unmapped rules SHOULD have been mapped:")
                    for u in should_map:
                        logger.warning(f"    {u['rule_code']}: {u.get('explanation', '')}")
                break

        time.sleep(0.5)

    return all_reviews


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_output(data: dict) -> list[str]:
    """Validate controls, groups, and rules. Returns list of warnings."""
    warnings = []

    # Collect valid group slugs
    valid_group_slugs = set()
    for group in data.get("groups", []):
        gid = group.get("id", "")
        if not SLUG_REGEX.match(gid):
            warnings.append(f"Invalid group ID (must be slug): '{gid}'")
        else:
            valid_group_slugs.add(gid)
        if group.get("variant") not in ("main", "subprocess"):
            warnings.append(f"Group '{gid}' missing valid variant ('main' or 'subprocess')")

    # Validate controls
    control_ids = set()
    for control in data.get("controls", []):
        cid = control.get("id", "")
        if not ID_REGEX.match(cid):
            warnings.append(f"Invalid control ID: '{cid}'")
        else:
            control_ids.add(cid)

        group_ref = control.get("group", "")
        if not group_ref:
            warnings.append(f"Control '{cid}' missing 'group' field")
        elif group_ref not in valid_group_slugs:
            warnings.append(f"Control '{cid}' references unknown group slug: '{group_ref}'")

    # Check for orphan groups (groups with no controls)
    referenced_groups = {c.get("group") for c in data.get("controls", []) if c.get("group")}
    for gid in valid_group_slugs:
        if gid not in referenced_groups:
            warnings.append(f"Orphan group: '{gid}' has no controls referencing it")

    # Validate rules
    for rule in data.get("rules", []):
        target = rule.get("target", "")
        # Target can be either a control ID or a group slug
        if not ID_REGEX.match(target) and not SLUG_REGEX.match(target):
            warnings.append(f"Invalid rule target: '{target}'")

    return warnings


def strip_invalid_items(data: dict) -> dict:
    """Remove controls with invalid IDs or missing group refs, groups with invalid slugs."""
    # Collect valid group slugs first
    valid_groups = [g for g in data.get("groups", []) if SLUG_REGEX.match(g.get("id", ""))]
    valid_group_slugs = {g["id"] for g in valid_groups}

    # Filter controls: must have valid ID and valid group reference
    valid_controls = []
    for c in data.get("controls", []):
        if not ID_REGEX.match(c.get("id", "")):
            continue
        if c.get("group", "") not in valid_group_slugs:
            continue
        valid_controls.append(c)

    # Filter rules: target must be valid (control ID or group slug)
    valid_rules = []
    for rule in data.get("rules", []):
        target = rule.get("target", "")
        if ID_REGEX.match(target) or SLUG_REGEX.match(target):
            valid_rules.append(rule)

    stripped_controls = len(data.get("controls", [])) - len(valid_controls)
    stripped_groups = len(data.get("groups", [])) - len(valid_groups)
    stripped_rules = len(data.get("rules", [])) - len(valid_rules)
    if stripped_controls or stripped_groups or stripped_rules:
        logger.warning(
            f"  Stripped invalid items: {stripped_controls} controls, "
            f"{stripped_groups} groups, {stripped_rules} rules"
        )

    return {"controls": valid_controls, "groups": valid_groups, "rules": valid_rules}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rules Architect — LLM pipeline for compliance form generation"
    )
    parser.add_argument("run_dir", help="Path to run directory (e.g. runs/1)")
    parser.add_argument("--process", help="Process a single process form")
    parser.add_argument("--dry-run", action="store_true", help="Print prompts without API calls")
    parser.add_argument("--model", help="Override model for all groups")
    parser.add_argument("--review", action="store_true",
                        help="Run second-pass review after generation")

    args = parser.parse_args()

    run_process_architect(args.run_dir, args.process, args.dry_run, args.model, args.review)
