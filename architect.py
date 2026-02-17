#!/usr/bin/env python3
"""
Rules Architect Agent — LLM-powered pipeline that processes enriched groups
breadth-first and produces per-section JSON Forms data files.

Usage:
    python architect.py runs/1                          # Full pipeline
    python architect.py runs/1 --group 4_2              # Single group
    python architect.py runs/1 --dry-run                # Print prompts only
    python architect.py runs/1 --model claude-sonnet-4-5-20250929  # Override model
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

## ID Format

All IDs must match the regex: ^4(_\\d+)+(_[a-z])?$
Examples: 4_2, 4_2_3, 4_2_3_1, 4_2_3_1_a
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
            # Validate IDs
            warnings = validate_ids(data)
            for w in warnings:
                logger.warning(f"  {group['id']}: {w}")
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

    # Initialize with gating rules
    for section_id, rules in gating_rules.items():
        section_outputs[section_id]["rules"].extend(rules)

    # Create API client (unless dry run)
    client = None
    if not dry_run:
        client = anthropic.Anthropic()

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

        for section_id, data in sorted(section_outputs.items()):
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
    parser.add_argument("--group", help="Process a single group (e.g. 4_2)")
    parser.add_argument("--dry-run", action="store_true", help="Print prompts without API calls")
    parser.add_argument("--model", help="Override model for all groups")

    args = parser.parse_args()
    run_architect(args.run_dir, args.group, args.dry_run, args.model)
