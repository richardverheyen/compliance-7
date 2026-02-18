#!/usr/bin/env python3
"""
Rules Architect Agent — LLM-powered pipeline that processes enriched groups
and produces compliance form data files.

Usage:
    # Section mode (default) — breadth-first by regulation group
    python architect.py runs/1                                    # Full pipeline
    python architect.py runs/1 --group 4_2                        # Single group
    python architect.py runs/1 --dry-run                          # Print prompts only
    python architect.py runs/1 --model claude-sonnet-4-5-20250929 # Override model

    # Process mode — one form per business process
    python architect.py runs/1 --mode process                     # All process forms
    python architect.py runs/1 --mode process --process cdd-individuals  # Single process
    python architect.py runs/1 --mode process --dry-run           # Print prompts only
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
    # CDD forms — one per customer type (collection + verification + discrepancies combined)
    "cdd-individuals": {
        "title": "Customer Due Diligence — Individuals",
        "source_groups": ["4_2"],
        "gated_by": "4_1_4_1",
    },
    "cdd-companies": {
        "title": "Customer Due Diligence — Companies",
        "source_groups": ["4_3"],
        "gated_by": "4_1_4_2",
    },
    "cdd-trusts": {
        "title": "Customer Due Diligence — Trusts",
        "source_groups": ["4_4"],
        "gated_by": "4_1_4_3",
    },
    "cdd-partnerships": {
        "title": "Customer Due Diligence — Partnerships",
        "source_groups": ["4_5"],
        "gated_by": "4_1_4_4",
    },
    "cdd-associations": {
        "title": "Customer Due Diligence — Associations",
        "source_groups": ["4_6"],
        "gated_by": "4_1_4_5",
    },
    "cdd-cooperatives": {
        "title": "Customer Due Diligence — Co-operatives",
        "source_groups": ["4_7"],
        "gated_by": "4_1_4_6",
    },
    "cdd-government": {
        "title": "Customer Due Diligence — Government Bodies",
        "source_groups": ["4_8"],
        "gated_by": "4_1_4_7",
    },
    # Cross-cutting forms
    "risk-assessment": {
        "title": "ML/TF Risk Assessment",
        "source_groups": ["4_1"],
        "gated_by": None,
    },
    "verification-documents": {
        "title": "Verification Standards — Documents",
        "source_groups": ["4_9"],
        "gated_by": None,
    },
    "verification-electronic": {
        "title": "Verification Standards — Electronic",
        "source_groups": ["4_10"],
        "gated_by": None,
    },
    "agent-management": {
        "title": "Agent Management",
        "source_groups": ["4_11"],
        "gated_by": "4_1_8",
    },
    "beneficial-ownership": {
        "title": "Beneficial Ownership",
        "source_groups": ["4_12"],
        "gated_by": "4_1_5_1",
    },
    "pep-screening": {
        "title": "PEP Screening",
        "source_groups": ["4_13"],
        "gated_by": "4_1_5_2",
    },
    "record-keeping": {
        "title": "Record Keeping",
        "source_groups": ["4_14"],
        "gated_by": None,
    },
    "alternative-id": {
        "title": "Alternative Identity Proofing",
        "source_groups": ["4_15"],
        "gated_by": None,
    },
}

# ---------------------------------------------------------------------------
# Section gating rules (deterministic, no LLM needed)
# ---------------------------------------------------------------------------


def generate_section_gating_rules(intro: dict) -> dict[str, list[dict]]:
    """Compute deterministic section gating rules from the introduction's scoping map.

    Returns a dict mapping section_id -> list of Rule dicts.
    Always-active sections get an empty list.
    """
    scoping = intro.get("scoping", {})
    always_active = set(intro.get("alwaysActive", {}).get("sections", []))

    # Build reverse map: section -> scope control
    section_rules: dict[str, list[dict]] = defaultdict(list)

    for control_id, mapping in scoping.items():
        for section_id in mapping.get("sections", []):
            section_rules[section_id].append({
                "target": section_id,
                "scope": control_id,
                "effect": "SHOW",
                "schema": {"const": "Yes"},
            })

    # Ensure always-active sections have empty rule lists
    for section_id in always_active:
        if section_id not in section_rules:
            section_rules[section_id] = []

    return dict(section_rules)


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
                        "id": {"type": "string", "description": "Hierarchical ID using underscore notation, e.g. '4_2_3_1'"},
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
                    "required": ["id", "label", "detail-required", "correct-option"],
                },
            },
            "groups": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string", "description": "Hierarchical ID matching parent prefix of children"},
                        "title": {"type": "string", "description": "Display title for this section"},
                        "description": {"type": "string", "description": "Explanatory text shown beneath the group heading"},
                    },
                    "required": ["id", "title"],
                },
            },
            "rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "target": {"type": "string", "description": "Control or group ID whose visibility is affected"},
                        "scope": {"type": "string", "description": "Control ID whose answer determines visibility"},
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

SYSTEM_PROMPT = f"""You are a compliance form architect. Your job is to analyse Australian AML/CTF regulatory text and produce structured form data.

## Output Types

### Control
A compliance control point (question) within the form:
- id: Hierarchical underscore ID, e.g. "4_2_3_1". MUST match the pattern 4_N_N... derived from the regulatory text's rule codes.
- label: A clear Yes/No compliance question derived from the regulatory requirement.
- detail-required: true if answering "Yes" should prompt the user to explain HOW they comply.
- correct-option: "Yes" if compliance requires this, "No" if it should not happen, "N/A" for scope-gate questions.
- detail-label: (optional) Custom label for the detail text input.
- process-id: (optional) Business process ID from the PROCESSES map below.

### Group
An organisational container:
- id: Hierarchical ID matching the parent prefix of its children.
- title: Display title derived from bold text headings in the regulatory text.
- description: (optional) Explanatory text, often from italic notes.

### Rule
Conditional visibility logic:
- target: The control or group ID whose visibility is affected.
- scope: The control ID whose answer determines visibility.
- effect: "SHOW" or "HIDE".
- schema: {{ "const": "Yes" }} or similar — the value the scope control must equal.

## Control Creation Guidelines

1. **Aggregate** related procedural requirements into single controls. For example, if the text lists "must collect name, DOB, address, occupation", create ONE control: "Does your program include a procedure to collect identifying information for [customer type]?" with detail-required: true.
2. **Don't create 1:1 controls** for every text node. Group related requirements.
3. Use **detail-required: true** for "how" questions where the user should explain their process.
4. Use **detail-required: false** for simple yes/no compliance checks.
5. **Skip** italic/note text — use it as group descriptions instead.
6. Use **bold text** as group titles.
7. Control IDs MUST follow the pattern derived from the regulatory section numbering.

## Process IDs

Available process IDs to assign to controls:
{json.dumps(PROCESSES, indent=2)}

## ID Format — CRITICAL

All IDs MUST match the regex: ^4(_\\d+)+(_[a-z])?$
- ONLY use numbers and single lowercase letters as segments
- Derive IDs directly from the regulatory rule codes (e.g. rule 4.3.5(1) → id "4_3_5_1")
- NEVER invent descriptive suffixes like "_simplified", "_methods", "_discrepancies"
- NEVER add words to IDs — only digits and single letters from the rule numbering

Valid: 4_2, 4_2_3, 4_2_3_1, 4_2_3_1_a, 4_3_5_3_b
Invalid: 4_3_8_simplified, 4_3_10_methods, 4_2_collection
"""


def build_user_message(
    group: dict,
    depth: int,
    parent_id: str | None,
    parent_controls: list[dict],
    intro: dict,
) -> str:
    """Build the per-group user message for the LLM."""
    text_nodes = group.get("text_nodes", [])
    gid = group["id"]

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

    # Parent controls context
    parent_ctx = ""
    if parent_controls:
        parent_ctx = "\n## Parent Section Controls (already defined — you may reference these in rules)\n"
        for c in parent_controls:
            parent_ctx += f"  - {c['id']}: {c['label']}\n"

    # Depth-specific instructions
    if depth == 1:
        depth_instructions = """## Depth-1 Instructions
- Create controls for the core compliance questions in this top-level section.
- Create sub-groups where the text has clear sub-sections (look for bold headings).
- Section gating rules (showing/hiding this section based on introduction answers) are pre-computed — focus on controls and groups.
- For section 4_1 specifically: focus on Risk Assessment content (4_1_2, 4_1_3, etc.), knowing customer categories and agents are handled by the introduction form."""
    else:
        depth_instructions = f"""## Depth-{depth} Instructions
- Create controls for the detailed requirements in this sub-section.
- Create rules that conditionally show this group or its controls based on parent controls where appropriate.
- Reference parent controls (listed above) in rule scopes if this sub-section should only appear when a parent control is answered a certain way."""

    # Introduction context
    intro_ctx = ""
    if intro:
        scoping_keys = list(intro.get("scoping", {}).keys())
        always_active = intro.get("alwaysActive", {}).get("sections", [])
        intro_ctx = f"""
## Introduction Form Context
The introduction form handles customer category selection and agent selection.
Scoping controls: {', '.join(scoping_keys)}
Always-active sections: {', '.join(always_active)}
Do NOT duplicate controls that exist in the introduction form."""

    return f"""## Group: {gid}
Depth: {depth}
Parent: {parent_id or 'None (top-level section)'}
Text nodes: {len(text_nodes)}

## Regulatory Text
{nodes_text}
{parent_ctx}
{depth_instructions}
{intro_ctx}

Analyse the regulatory text above and produce the controls, groups, and rules for group {gid}.
"""


# ---------------------------------------------------------------------------
# Process mode — system prompt, helpers, pipeline
# ---------------------------------------------------------------------------

PROCESS_SYSTEM_PROMPT = f"""You are a compliance **process** architect. Your job is to analyse Australian AML/CTF regulatory text and produce structured form data organized by **business process step**, not by regulation sub-section.

## Output Types

### Control
A compliance control point (question) within the form:
- id: Hierarchical underscore ID, e.g. "4_2_3_1". MUST match the pattern 4_N_N... derived from the regulatory text's rule codes.
- label: A clear Yes/No compliance question derived from the regulatory requirement.
- detail-required: true if answering "Yes" should prompt the user to explain HOW they comply.
- correct-option: "Yes" if compliance requires this, "No" if it should not happen, "N/A" for scope-gate questions.
- detail-label: (optional) Custom label for the detail text input.
- process-id: (optional) Business process ID from the PROCESSES map below.
- source-rules: REQUIRED array of regulation rule codes this control derives from, e.g. ["4.3.5(1)", "4.3.5(2)"]. Every control MUST include this.
- mapping-confidence: REQUIRED number 0.0-1.0 indicating confidence in the regulation-to-control mapping:
  - 1.0 = direct, unambiguous 1:1 mapping from a single rule
  - 0.7 = clear mapping but aggregated from multiple related rules
  - 0.5 = reasonable interpretation, some judgement applied
  - <0.5 = uncertain mapping, rule text is ambiguous or tangentially related

### Group
An organisational container representing a **process step**:
- id: Hierarchical ID matching the parent prefix of its children.
- title: Display title for this process step.
- description: (optional) Explanatory text.

### Rule
Conditional visibility logic:
- target: The control or group ID whose visibility is affected.
- scope: The control ID whose answer determines visibility.
- effect: "SHOW" or "HIDE".
- schema: {{ "const": "Yes" }} or similar — the value the scope control must equal.

## Process Step Organization

Organize controls into groups representing **process steps**, NOT regulation sub-sections. For CDD forms, use this pattern:

1. **General CDD Obligation** — top-level "does your program cover this customer type"
2. **Collection of KYC Information** — what minimum info to collect
3. **Verification of Information** — how to verify what was collected
4. **Additional KYC Assessment** — risk-based decisions on extra collection/verification
5. **Safe Harbour Procedures** (if applicable) — simplified verification options
6. **Discrepancy Handling** — responding to verification issues

For non-CDD forms, organize by logical workflow steps appropriate to the topic.

## Control Creation Guidelines

1. **Aggregate** related procedural requirements into single controls. For example, if the text lists "must collect name, DOB, address, occupation", create ONE control: "Does your program include a procedure to collect identifying information for [customer type]?" with detail-required: true.
2. **Don't create 1:1 controls** for every text node. Group related requirements.
3. Use **detail-required: true** for "how" questions where the user should explain their process.
4. Use **detail-required: false** for simple yes/no compliance checks.
5. **Skip** italic/note text — use it as group descriptions instead.
6. Every control MUST include **source-rules** listing the specific regulation codes it derives from.
7. Every control MUST include **mapping-confidence** (0.0-1.0) indicating how confident the mapping is.
8. Control IDs MUST follow the pattern derived from the regulatory section numbering.

## Process IDs

Available process IDs to assign to controls:
{json.dumps(PROCESSES, indent=2)}

## ID Format — CRITICAL

All IDs MUST match the regex: ^4(_\\d+)+(_[a-z])?$
- ONLY use numbers and single lowercase letters as segments
- Derive IDs directly from the regulatory rule codes (e.g. rule 4.3.5(1) → id "4_3_5_1")
- NEVER invent descriptive suffixes like "_simplified", "_methods", "_discrepancies"
- NEVER add words to IDs — only digits and single letters from the rule numbering

Valid: 4_2, 4_2_3, 4_2_3_1, 4_2_3_1_a, 4_3_5_3_b
Invalid: 4_3_8_simplified, 4_3_10_methods, 4_2_collection
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

    return f"""## Process Form: {form_def['title']}
Process ID: {process_id}

You are producing a single form that covers ALL aspects of this process.
Organize controls by process step (Collect → Verify → Handle Discrepancies), NOT by regulation sub-section.
Every control MUST include source-rules listing the regulation codes it derives from.

## Regulatory Text ({len(text_nodes)} text nodes)
{nodes_text}

Analyse ALL the regulatory text above and produce the controls, groups, and rules for the "{form_def['title']}" process form.
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
    """Compare input rule codes against output source-rules to find coverage gaps.

    Returns a report dict with:
    - input_codes: all rule codes from the input text
    - mapped_codes: rule codes that appear in at least one control's source-rules
    - unmapped_codes: input codes not referenced by any control (potential gaps)
    - extra_codes: codes in source-rules that weren't in the input (hallucinations?)
    - coverage_pct: percentage of input codes that were mapped
    - low_confidence: controls with mapping-confidence < 0.5
    """
    input_codes = extract_input_rule_codes(text_nodes)
    output_codes = extract_output_rule_codes(result)

    mapped = input_codes & output_codes
    unmapped = input_codes - output_codes
    extra = output_codes - input_codes

    coverage_pct = (len(mapped) / len(input_codes) * 100) if input_codes else 100.0

    # Find low-confidence controls
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
            warnings = validate_ids(data)
            for w in warnings:
                logger.warning(f"  {process_id}: {w}")
            data = strip_invalid_ids(data)
            return data

    logger.error(f"No tool_use block in response for process {process_id}")
    return None


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

        # Select model: large for big forms, small for smaller ones
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
                f"{len(result['rules'])} rules"
            )

        # Rate limiting
        if not dry_run:
            time.sleep(0.5)

    if not dry_run:
        # Write coverage audit report
        if coverage_reports:
            audit_path = os.path.join(processes_dir, "_coverage_audit.json")
            # Compute summary
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

        # Run second-pass review if requested
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
                            "description": "Mapping quality assessment",
                        },
                        "confidence": {
                            "type": "number",
                            "description": "Reviewer confidence in mapping (0.0-1.0)",
                        },
                        "issues": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Specific issues found with this mapping",
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
                            "description": "Why this rule was not mapped",
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

Your job is to assess the quality of each control's mapping to the source regulation text. You have access to both the original regulatory text and the controls that were produced.

For each control, evaluate:
1. Does the control's label accurately reflect the regulation text cited in source-rules?
2. Are the source-rules correct — do they actually relate to this control?
3. Is the control's correct-option appropriate?
4. Were any important requirements from the source rules missed or misrepresented?

Quality ratings:
- "good": Direct, accurate mapping. Source rules match. No issues.
- "acceptable": Reasonable mapping with minor imprecision. Aggregation is fine but might lose nuance.
- "questionable": Mapping has issues — wrong source rules, misleading label, or missed requirements.
- "incorrect": Fundamentally wrong mapping — source rules don't relate, or control contradicts the regulation.

For unmapped rules (rules from the input that no control references), assess whether:
- "correctly_omitted": The rule is a heading, note, or doesn't warrant a control
- "should_be_mapped": The rule contains a substantive requirement that should have a control
- "already_covered": The rule's substance is covered by another control, just not listed in source-rules
"""


def run_review_pass(
    client: anthropic.Anthropic,
    run_dir: str,
    groups: list[dict],
    coverage_reports: dict[str, dict],
) -> dict:
    """Run second-pass review on process forms that have coverage issues or low confidence."""
    processes_dir = os.path.join(run_dir, "processes")
    all_reviews = {}

    for process_id, report in coverage_reports.items():
        # Review all processes (not just problematic ones) for completeness
        process_path = os.path.join(processes_dir, f"{process_id}.json")
        if not os.path.exists(process_path):
            continue

        with open(process_path) as f:
            result = json.load(f)

        # Gather text nodes for context
        text_nodes = gather_process_nodes(process_id, groups)

        # Build review prompt
        nodes_text = ""
        for tn in text_nodes:
            prefix = f"[{tn['rule_code']}] " if tn["rule_code"] else ""
            nodes_text += f"  {prefix}{tn['text']}\n"

        controls_text = ""
        for ctrl in result.get("controls", []):
            src = ", ".join(ctrl.get("source-rules", []))
            conf = ctrl.get("mapping-confidence", "N/A")
            controls_text += f"  {ctrl['id']}: {ctrl['label']}\n"
            controls_text += f"    source-rules: [{src}]\n"
            controls_text += f"    mapping-confidence: {conf}\n"
            controls_text += f"    correct-option: {ctrl.get('correct-option', '?')}\n\n"

        unmapped_text = ""
        if report["unmapped_codes"]:
            unmapped_text = f"\n## Unmapped Rules ({len(report['unmapped_codes'])})\nThese rule codes from the input were NOT referenced by any control's source-rules:\n"
            for code in report["unmapped_codes"]:
                # Find the text for this code
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
            model=MODEL_SMALL,  # Use Haiku for cost efficiency
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

                # Log summary
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


def validate_ids(data: dict) -> list[str]:
    """Validate that all IDs match the expected format. Returns list of warnings."""
    warnings = []
    for control in data.get("controls", []):
        if not ID_REGEX.match(control["id"]):
            warnings.append(f"Invalid control ID: {control['id']}")
    for group in data.get("groups", []):
        if not ID_REGEX.match(group["id"]):
            warnings.append(f"Invalid group ID: {group['id']}")
    for rule in data.get("rules", []):
        if not ID_REGEX.match(rule["target"]):
            warnings.append(f"Invalid rule target ID: {rule['target']}")
    return warnings


def strip_invalid_ids(data: dict) -> dict:
    """Remove controls, groups, and rules with invalid IDs from the output."""
    valid_controls = [c for c in data.get("controls", []) if ID_REGEX.match(c["id"])]
    valid_groups = [g for g in data.get("groups", []) if ID_REGEX.match(g["id"])]

    # For rules, check both target and scope
    valid_ids = {c["id"] for c in valid_controls} | {g["id"] for g in valid_groups}
    valid_rules = []
    for rule in data.get("rules", []):
        if not ID_REGEX.match(rule["target"]):
            continue
        # Keep rules whose target references a valid control/group in this output,
        # OR whose target is a section-level ID (gating rules reference external IDs)
        valid_rules.append(rule)

    stripped_controls = len(data.get("controls", [])) - len(valid_controls)
    stripped_groups = len(data.get("groups", [])) - len(valid_groups)
    stripped_rules = len(data.get("rules", [])) - len(valid_rules)
    if stripped_controls or stripped_groups or stripped_rules:
        logger.warning(
            f"  Stripped invalid IDs: {stripped_controls} controls, "
            f"{stripped_groups} groups, {stripped_rules} rules"
        )

    return {"controls": valid_controls, "groups": valid_groups, "rules": valid_rules}


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------


def call_architect(
    client: anthropic.Anthropic,
    group: dict,
    depth: int,
    parent_id: str | None,
    parent_controls: list[dict],
    intro: dict,
    model: str,
    dry_run: bool = False,
) -> dict | None:
    """Call the LLM for a single group and return parsed SectionData."""
    user_msg = build_user_message(group, depth, parent_id, parent_controls, intro)

    if dry_run:
        print(f"\n{'='*60}")
        print(f"DRY RUN — Group: {group['id']} | Model: {model}")
        print(f"{'='*60}")
        print(f"SYSTEM PROMPT: ({len(SYSTEM_PROMPT)} chars)")
        print(f"USER MESSAGE:\n{user_msg}")
        return None

    logger.info(f"Calling API for group {group['id']} with model {model}...")

    response = client.messages.create(
        model=model,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
        tools=[OUTPUT_TOOL],
        tool_choice={"type": "tool", "name": "output_section_data"},
    )

    # Extract tool use result
    for block in response.content:
        if block.type == "tool_use" and block.name == "output_section_data":
            data = block.input
            # Validate and strip invalid IDs
            warnings = validate_ids(data)
            for w in warnings:
                logger.warning(f"  {group['id']}: {w}")
            data = strip_invalid_ids(data)
            return data

    logger.error(f"No tool_use block in response for group {group['id']}")
    return None


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


def get_top_level_section(group_id: str) -> str:
    """Extract top-level section from a group ID: '4_2_3_1' -> '4_2'."""
    parts = group_id.split("_")
    if len(parts) >= 2:
        return "_".join(parts[:2])
    return group_id


def select_model(group: dict, override: str | None) -> str:
    """Select model based on group size or override."""
    if override:
        return override
    node_count = len(group.get("text_nodes", []))
    return MODEL_LARGE if node_count >= TEXT_NODE_THRESHOLD else MODEL_SMALL


def run_architect(run_dir: str, single_group: str | None = None,
                  dry_run: bool = False, model_override: str | None = None):
    """Main pipeline: process groups breadth-first and produce section files."""

    # Load data
    enriched_path = os.path.join(run_dir, "groups_enriched.json")
    if not os.path.exists(enriched_path):
        logger.error(f"groups_enriched.json not found in {run_dir}. Run 'python main.py enrich' first.")
        sys.exit(1)

    with open(enriched_path) as f:
        groups = json.load(f)

    intro_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "introduction.json")
    with open(intro_path) as f:
        intro = json.load(f)

    # Compute section gating rules
    gating_rules = generate_section_gating_rules(intro)

    # Build group lookup
    group_map = {g["id"]: g for g in groups}

    # Filter to single group if requested
    if single_group:
        if single_group not in group_map:
            logger.error(f"Group {single_group} not found in groups_enriched.json")
            sys.exit(1)
        groups = [group_map[single_group]]

    # Sort by depth (breadth-first), skip depth 0 (root "4")
    groups_by_depth = sorted(
        [g for g in groups if g["depth"] > 0],
        key=lambda g: (g["depth"], g["id"]),
    )

    # Accumulator: completed section data
    completed_sections: dict[str, dict] = {}  # group_id -> SectionData

    # Output accumulator: section_id -> merged SectionData
    section_outputs: dict[str, dict] = defaultdict(lambda: {
        "controls": [], "groups": [], "rules": [],
    })

    # In single-group mode, load existing section files so we merge rather than overwrite
    sections_dir = os.path.join(run_dir, "sections")
    if single_group:
        if os.path.isdir(sections_dir):
            for fname in os.listdir(sections_dir):
                if fname.endswith(".json"):
                    sid = fname[:-5]
                    with open(os.path.join(sections_dir, fname)) as f:
                        section_outputs[sid] = json.load(f)
                    logger.info(f"Loaded existing {fname}")

    # Initialize gating rules for sections that don't have them yet
    for section_id, rules in gating_rules.items():
        existing_rules = section_outputs[section_id].get("rules", [])
        existing_gating = {(r["target"], r["scope"]) for r in existing_rules}
        for rule in rules:
            if (rule["target"], rule["scope"]) not in existing_gating:
                section_outputs[section_id].setdefault("rules", []).append(rule)

    # Create API client (unless dry run)
    client = None
    if not dry_run:
        client = anthropic.Anthropic()

    # Track which sections are modified in this run
    touched_sections: set[str] = set()

    # Process groups breadth-first
    total = len(groups_by_depth)
    for i, group in enumerate(groups_by_depth, 1):
        gid = group["id"]
        depth = group["depth"]

        # Skip groups with no text nodes
        if not group.get("text_nodes"):
            logger.info(f"[{i}/{total}] Skipping {gid} (no text nodes)")
            continue

        # Determine parent
        parts = gid.split("_")
        parent_id = "_".join(parts[:-1]) if len(parts) > 2 else None

        # Gather parent controls from completed sections
        parent_controls = []
        if parent_id and parent_id in completed_sections:
            parent_controls = completed_sections[parent_id].get("controls", [])

        # Select model
        model = select_model(group, model_override)

        logger.info(f"[{i}/{total}] Processing {gid} (depth={depth}, nodes={len(group['text_nodes'])}, model={model.split('-')[1] if '-' in model else model})")

        # Call the LLM
        result = call_architect(
            client, group, depth, parent_id, parent_controls,
            intro, model, dry_run,
        )

        if result is None:
            continue

        # Store in completed sections
        completed_sections[gid] = result

        # Merge into top-level section output
        section_id = get_top_level_section(gid)
        touched_sections.add(section_id)
        section = section_outputs[section_id]

        # Dedup controls and groups by ID
        existing_control_ids = {c["id"] for c in section["controls"]}
        for control in result.get("controls", []):
            if control["id"] not in existing_control_ids:
                section["controls"].append(control)
                existing_control_ids.add(control["id"])

        existing_group_ids = {g["id"] for g in section["groups"]}
        for grp in result.get("groups", []):
            if grp["id"] not in existing_group_ids:
                section["groups"].append(grp)
                existing_group_ids.add(grp["id"])

        # Append rules (no dedup — rules can stack)
        section["rules"].extend(result.get("rules", []))

        # Rate limiting
        if not dry_run:
            time.sleep(0.5)

    # Write output files
    if not dry_run:
        sections_dir = os.path.join(run_dir, "sections")
        os.makedirs(sections_dir, exist_ok=True)

        # In single-group mode, only write sections that were touched
        sections_to_write = sorted(section_outputs.items()) if not single_group else \
            [(sid, section_outputs[sid]) for sid in sorted(touched_sections)]

        for section_id, data in sections_to_write:
            output_path = os.path.join(sections_dir, f"{section_id}.json")
            with open(output_path, "w") as f:
                json.dump(data, f, indent=2)
            logger.info(
                f"Wrote {section_id}.json — "
                f"{len(data['controls'])} controls, "
                f"{len(data['groups'])} groups, "
                f"{len(data['rules'])} rules"
            )

        # Save raw responses for audit
        audit_path = os.path.join(run_dir, "architect_results.json")
        with open(audit_path, "w") as f:
            json.dump(completed_sections, f, indent=2)
        logger.info(f"Audit trail → {audit_path}")

        print(f"\nDone! Section files written to {sections_dir}/")
        print(f"  Sections: {len(section_outputs)}")
        total_controls = sum(len(d["controls"]) for d in section_outputs.values())
        total_rules = sum(len(d["rules"]) for d in section_outputs.values())
        print(f"  Total controls: {total_controls}")
        print(f"  Total rules: {total_rules}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Rules Architect — LLM pipeline for compliance form generation"
    )
    parser.add_argument("run_dir", help="Path to run directory (e.g. runs/1)")
    parser.add_argument("--mode", choices=["section", "process"], default="section",
                        help="Pipeline mode: section (default) or process")
    parser.add_argument("--group", help="Process a single group (section mode)")
    parser.add_argument("--process", help="Process a single process form (process mode)")
    parser.add_argument("--dry-run", action="store_true", help="Print prompts without API calls")
    parser.add_argument("--model", help="Override model for all groups")
    parser.add_argument("--review", action="store_true",
                        help="Run second-pass review after process mode (process mode only)")

    args = parser.parse_args()

    if args.mode == "process":
        run_process_architect(args.run_dir, args.process, args.dry_run, args.model, args.review)
    else:
        run_architect(args.run_dir, args.group, args.dry_run, args.model)
