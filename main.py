#!/usr/bin/env python3
"""
PDF Scraper & Reference Linker — standalone JSON-based pipeline.

Usage:
    python main.py chapter4.pdf
    python main.py path/to/any.pdf
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict

import fitz  # PyMuPDF
import pdfplumber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

RUNS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "runs")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def next_run_id() -> int:
    """Return the next sequential run number (1, 2, 3 …)."""
    os.makedirs(RUNS_DIR, exist_ok=True)
    existing = [
        int(d) for d in os.listdir(RUNS_DIR)
        if os.path.isdir(os.path.join(RUNS_DIR, d)) and d.isdigit()
    ]
    return max(existing, default=0) + 1


def generate_uid(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:10]


# ---------------------------------------------------------------------------
# PDF Scraper
# ---------------------------------------------------------------------------

class PDFScraper:
    def __init__(self, pdf_path: str, run_dir: str):
        self.pdf_path = os.path.abspath(pdf_path)
        self.run_dir = run_dir
        self.excerpts_dir = os.path.join(run_dir, "excerpts")
        os.makedirs(self.excerpts_dir, exist_ok=True)
        self.results: list[dict] = []
        self.pdf_doc = None
        self.boilerplate: set = set()

    # --- boilerplate (header/footer) detection --------------------------------

    def build_boilerplate_map(self, sample_limit: int = 15):
        """Identifies text that appears at the same vertical position across pages."""
        y_text_map = Counter()
        with pdfplumber.open(self.pdf_path) as pdf:
            sample_pages = pdf.pages[:sample_limit]
            for page in sample_pages:
                words = page.extract_words()
                for w in words:
                    key = (w["text"].strip(), round(w["top"], 0))
                    y_text_map[key] += 1

        threshold = len(sample_pages) * 0.4
        self.boilerplate = {k for k, c in y_text_map.items() if c >= threshold}
        logger.info(f"Mapped {len(self.boilerplate)} boilerplate elements to ignore.")

    # --- rule marker detection ------------------------------------------------

    @staticmethod
    def is_rule_marker(text: str):
        patterns = {
            "part":  r"^Part\s+\d+\.\d+",
            "main":  r"^\d+\.\d+\.\d+",
            "digit": r"^\(\d+\)",
            "alpha": r"^\([a-z]\)",
            "roman": r"^\([ivx]+\)",
        }
        for level, pat in patterns.items():
            if re.match(pat, text):
                return level, pat
        return None, None

    # --- PDF excerpt generation -----------------------------------------------

    def _generate_pdf_excerpt(self, page_num_0idx: int, block_bbox: list, node_index: int):
        """Full-width PDF crop with yellow highlight over the extracted text."""
        PADDING_VERT = 80

        src_page = self.pdf_doc[page_num_0idx]
        page_width = src_page.rect.width
        page_height = src_page.rect.height

        x0, top, x1, bottom = block_bbox

        crop_top = max(0, top - PADDING_VERT)
        crop_bottom = min(page_height, bottom + PADDING_VERT)
        rect = fitz.Rect(0, crop_top, page_width, crop_bottom)

        new_doc = fitz.open()
        new_page = new_doc.new_page(width=rect.width, height=rect.height)
        new_page.show_pdf_page(new_page.rect, self.pdf_doc, page_num_0idx, clip=rect)

        highlight_rect = fitz.Rect(x0, top - crop_top, x1, bottom - crop_top)
        new_page.draw_rect(highlight_rect, color=None, fill=(1, 1, 0), fill_opacity=0.3)

        out_path = os.path.join(self.excerpts_dir, f"{node_index}.pdf")
        new_doc.save(out_path)
        new_doc.close()

    # --- main scrape ----------------------------------------------------------

    def scrape(self) -> list[dict]:
        logger.info("=== Starting PDF Scraper ===")
        logger.info(f"Input PDF: {self.pdf_path}")

        if not os.path.exists(self.pdf_path):
            raise FileNotFoundError(self.pdf_path)

        self.build_boilerplate_map()
        self.pdf_doc = fitz.open(self.pdf_path)

        # Hierarchical rule state & text-block buffer
        state = {"part": "", "main": "", "digit": "", "alpha": "", "roman": ""}
        buffer = {
            "text_parts": [],
            "bbox": None,
            "page": None,
            "rule_code": "",
            "style": {"size": 0, "bold": False, "italic": False},
        }
        node_index = 0

        def flush_buffer():
            nonlocal node_index
            if not buffer["text_parts"]:
                return

            full_text = " ".join(buffer["text_parts"]).strip()
            full_text = re.sub(r"\s+", " ", full_text)
            if not full_text:
                return

            uid = generate_uid(full_text)
            node = {
                "uid": uid,
                "node_index": node_index,
                "page": buffer["page"],
                "x_indent": round(buffer["bbox"][0], 1),
                "bbox": buffer["bbox"],
                "text": full_text,
                "rule_code": buffer["rule_code"],
                "font_size": buffer["style"]["size"],
                "is_bold": buffer["style"]["bold"],
                "is_italic": buffer["style"]["italic"],
                "type": "RULE" if buffer["rule_code"] else "TEXT",
            }
            self.results.append(node)
            node_index += 1

            try:
                self._generate_pdf_excerpt(buffer["page"] - 1, buffer["bbox"], node_index)
            except Exception as e:
                logger.warning(f"Failed to generate excerpt for UID {uid}: {e}")

            buffer["text_parts"] = []
            buffer["bbox"] = None

        with pdfplumber.open(self.pdf_path) as pdf:
            for page_num_1idx, page in enumerate(pdf.pages, start=1):
                h = page.height
                lines = page.extract_text_lines(layout=True, strip=True)

                for line in lines:
                    text = line["text"].strip()
                    if not text:
                        continue

                    # Boilerplate & margin filtering
                    if (text, round(line["top"], 0)) in self.boilerplate:
                        continue
                    if line["top"] < (h * 0.05) or line["bottom"] > (h * 0.93):
                        continue

                    marker_level, marker_pat = self.is_rule_marker(text)

                    is_note = text.lower().startswith("note:")
                    is_new_sentence_block = text[0].isupper() and (
                        not buffer["text_parts"]
                        or buffer["text_parts"][-1].endswith((".", ";", ":"))
                    )

                    if marker_level or is_note or is_new_sentence_block:
                        flush_buffer()

                        buffer["page"] = page_num_1idx
                        first_char = line["chars"][0] if line["chars"] else {}
                        buffer["style"] = {
                            "size": round(first_char.get("size", 0), 1),
                            "bold": "bold" in first_char.get("fontname", "").lower(),
                            "italic": "italic" in first_char.get("fontname", "").lower(),
                        }

                        if marker_level:
                            match = re.match(marker_pat, text)
                            m_val = match.group(0)
                            levels = ["part", "main", "digit", "alpha", "roman"]
                            start_idx = levels.index(marker_level)
                            for lvl in levels[start_idx:]:
                                state[lvl] = ""
                            state[marker_level] = m_val

                            code = state["main"] + state["digit"] + state["alpha"] + state["roman"]
                            buffer["rule_code"] = code if code else state["part"]
                            text = text[match.end():].strip()
                        else:
                            buffer["rule_code"] = ""

                    # Accumulate text & expand bounding box
                    buffer["text_parts"].append(text)
                    l_bbox = [line["x0"], line["top"], line["x1"], line["bottom"]]
                    if buffer["bbox"] is None:
                        buffer["bbox"] = l_bbox
                    else:
                        buffer["bbox"] = [
                            min(buffer["bbox"][0], l_bbox[0]),
                            min(buffer["bbox"][1], l_bbox[1]),
                            max(buffer["bbox"][2], l_bbox[2]),
                            max(buffer["bbox"][3], l_bbox[3]),
                        ]

            flush_buffer()

        self.pdf_doc.close()
        logger.info(f"Extracted {len(self.results)} text nodes.")
        return self.results


# ---------------------------------------------------------------------------
# Parent assignment (indentation-based hierarchy)
# ---------------------------------------------------------------------------

def assign_parents(nodes: list[dict]):
    """Walk nodes in order; use a stack to assign parent_uid based on x_indent."""
    stack: list[tuple[float, str]] = []  # (x_indent, uid)
    for node in nodes:
        while stack and stack[-1][0] >= node["x_indent"]:
            stack.pop()
        node["parent_uid"] = stack[-1][1] if stack else None
        stack.append((node["x_indent"], node["uid"]))


# ---------------------------------------------------------------------------
# Top-level hierarchy grouping
# ---------------------------------------------------------------------------

def assign_top_level(nodes: list[dict], top_level_indent: float = 90.0, tolerance: float = 1.0):
    """Assign top_level_uid to each node (mirrors HierarchyProcessor)."""
    current_top_uid = None
    for node in nodes:
        at_header = abs(node["x_indent"] - top_level_indent) <= tolerance
        if at_header and node["type"] != "NOTE":
            current_top_uid = node["uid"]
        node["top_level_uid"] = current_top_uid


# ---------------------------------------------------------------------------
# Reference linker
# ---------------------------------------------------------------------------

REF_PATTERN = re.compile(r"(\d+(?:\.\d+)+(?:\([a-zA-Z0-9]+\))*|\([a-zA-Z0-9]+\))")


def link_references(nodes: list[dict]):
    """Find cross-references between nodes and store them on each node."""
    # Build map: normalised rule_code -> uid
    rule_map: dict[str, str] = {}
    for n in nodes:
        if n.get("rule_code"):
            normalised = n["rule_code"].replace(".", "_").replace("(", "_").replace(")", "").strip("_")
            rule_map[normalised] = n["uid"]

    for node in nodes:
        refs = set()
        for match in REF_PATTERN.findall(node["text"]):
            clean = match.replace(".", "_").replace("(", "_").replace(")", "").strip("_")
            if clean in rule_map and rule_map[clean] != node["uid"]:
                refs.add(rule_map[clean])
        node["outgoing_references"] = list(refs)


# ---------------------------------------------------------------------------
# Group identification (JSON Forms groups)
# ---------------------------------------------------------------------------

RULE_CODE_RE = re.compile(
    r"^(?:Part\s+)?(\d+(?:\.\d+)*)(\([^)]+\))*$"
)

INDENT_TOLERANCE = 3.0  # px – x_indent values within this are treated as same level


def _normalise_group_id(dotted: str) -> str:
    """Convert '4.1.3' → '4_1_3'."""
    return dotted.replace(".", "_")


def _parent_ids(group_id: str) -> list[str]:
    """Return all ancestor group ids.  '4_1_3' → ['4', '4_1']."""
    parts = group_id.split("_")
    return ["_".join(parts[:i]) for i in range(1, len(parts))]


def _parse_rule_code(rule_code: str) -> tuple[str, list[str]]:
    """Split a rule_code into its dotted stem and bracket parts.

    '4.1.3(1)(a)' → ('4.1.3', ['1', 'a'])
    'Part 4.1'    → ('4.1',  [])
    """
    rc = rule_code.strip()
    if rc.startswith("Part "):
        rc = rc[5:]
    brackets: list[str] = []
    while rc.endswith(")"):
        i = rc.rfind("(")
        if i == -1:
            break
        brackets.insert(0, rc[i + 1 : -1])
        rc = rc[:i]
    return rc, brackets


def _indent_bucket(x: float, buckets: list[float]) -> int:
    """Return index of the closest bucket for *x*."""
    best = 0
    for i, b in enumerate(buckets):
        if abs(x - b) < abs(x - buckets[best]):
            best = i
    return best


def _normalise_full(rule_code: str) -> str:
    """Normalise a rule_code to its full underscore id.

    '4.1.3(1)(a)' → '4_1_3_1_a'
    'Part 4.1'    → '4_1'
    """
    stem, brackets = _parse_rule_code(rule_code)
    full = _normalise_group_id(stem)
    for b in brackets:
        full += "_" + b
    return full


def _filter_sequential_rule_codes(nodes: list[dict]) -> set[int]:
    """Return the set of node_index values whose rule_code is in sequence.

    The PDF scraper's state machine can produce false-positive rule_codes
    when a line of text *references* another rule (e.g. "4.9.1 to 4.9.3")
    and a subsequent line coincidentally starts with a bracket marker like
    "(1)" which gets combined with the stale state.

    We detect two kinds of anomalies:
    1. **Forward jumps** — a stem leaps far ahead of the current position.
    2. **Backward regressions** — a stem re-appears after a higher-ranked
       stem has already been seen (e.g. 4.2.3 appearing after 4.2.12).

    In a well-structured regulatory document, stems (the N.N.N part)
    should appear in monotonically non-decreasing order.  Consecutive
    nodes may share a stem (e.g. 4.3.5(1), 4.3.5(2) both have stem
    4.3.5) but once a *higher* stem appears, the lower one should not
    recur.
    """
    valid: set[int] = set()

    # Collect all unique stems
    seen_stems: set[str] = set()
    for n in nodes:
        rc = n.get("rule_code", "")
        if not rc:
            continue
        stem, _ = _parse_rule_code(rc)
        seen_stems.add(stem)

    # Build a rank map (natural sort order) for all stems
    def _stem_sort_key(s: str):
        return [int(p) for p in s.split(".")]

    sorted_stems = sorted(seen_stems, key=_stem_sort_key)
    stem_rank: dict[str, int] = {s: i for i, s in enumerate(sorted_stems)}

    # Walk nodes and accept rule_codes whose stem rank is monotonically
    # non-decreasing (with a small forward-jump tolerance).
    #
    # - hwm: highest stem rank accepted so far
    # - A node is valid if  hwm <= rank <= hwm + FWD_THRESHOLD
    #   i.e. the stem hasn't gone backward and hasn't jumped too far forward.
    hwm = -1  # high-water-mark rank
    FWD_THRESHOLD = 3  # max ranks a stem can jump *ahead*

    for n in nodes:
        rc = n.get("rule_code", "")
        if not rc:
            continue
        stem, _ = _parse_rule_code(rc)
        rank = stem_rank[stem]

        if rank >= hwm and rank <= hwm + FWD_THRESHOLD:
            valid.add(n["node_index"])
            hwm = max(hwm, rank)
        else:
            logger.debug(
                f"Skipping out-of-order rule_code {rc!r} at node {n['node_index']} "
                f"(rank {rank} vs hwm {hwm})"
            )

    return valid


def build_groups(nodes: list[dict]) -> list[dict]:
    """Infer JSON Forms groups from rule_codes and x_indent values.

    Returns a sorted list of group dicts ``{id, depth, first_node_index}``.
    No duplicate groups are created.
    """

    # --- 0. Filter out false-positive rule_codes from text references -------
    valid_nodes = _filter_sequential_rule_codes(nodes)

    # --- 1. Compute indent buckets (cluster nearby x values) ----------------
    raw_indents = sorted({n["x_indent"] for n in nodes})
    buckets: list[float] = []
    for x in raw_indents:
        if not buckets or abs(x - buckets[-1]) > INDENT_TOLERANCE:
            buckets.append(x)
        # else: absorbed into the previous bucket

    # --- 2. Walk nodes and collect group ids --------------------------------
    # group_id → {depth, first_node_index, indent_bucket, representative_x}
    groups: dict[str, dict] = {}
    indent_confirmed: set[str] = set()  # groups confirmed by indent structure

    def _ensure_group(gid: str, depth: int, first_ni: int, rep_x: float | None = None):
        if gid not in groups:
            groups[gid] = {
                "id": gid,
                "depth": depth,
                "first_node_index": first_ni,
                "x_indent": rep_x,
            }
        else:
            groups[gid]["first_node_index"] = min(groups[gid]["first_node_index"], first_ni)

    for node in nodes:
        rc = node.get("rule_code", "")
        if not rc or node["node_index"] not in valid_nodes:
            continue

        stem, brackets = _parse_rule_code(rc)
        stem_id = _normalise_group_id(stem)
        parts = stem_id.split("_")

        # The full code chain: stem parts + bracket parts
        # e.g. '4.1.3(1)(a)' → chain = ['4', '4_1', '4_1_3', '4_1_3_1', '4_1_3_1_a']
        chain: list[str] = []
        for i in range(1, len(parts) + 1):
            chain.append("_".join(parts[:i]))
        for b in brackets:
            chain.append(chain[-1] + "_" + b)

        # The leaf (last element) is a control, not a group.
        # Everything before it is a group that must exist.
        # Additionally, if the node's x_indent is deeper than its parent's,
        # the parent itself is a group.
        for depth_0, gid in enumerate(chain[:-1]):
            _ensure_group(gid, depth_0, node["node_index"], node["x_indent"])

        # Indent-based check: if there are children indented under this node,
        # this node itself acts as a group header.  We find the next *valid
        # rule* node (skipping TEXT nodes which may have unrelated indents).
        ni = node["node_index"]
        next_rule = None
        for j in range(ni + 1, min(ni + 6, len(nodes))):
            candidate = nodes[j]
            if candidate.get("rule_code") and candidate["node_index"] in valid_nodes:
                next_rule = candidate
                break
        if next_rule is not None:
            cur_bucket = _indent_bucket(node["x_indent"], buckets)
            nxt_bucket = _indent_bucket(next_rule["x_indent"], buckets)
            if nxt_bucket > cur_bucket:
                leaf_id = chain[-1] if chain else stem_id
                _ensure_group(leaf_id, len(chain) - 1, node["node_index"], node["x_indent"])
                indent_confirmed.add(leaf_id)

    # --- 3. Remove singleton groups (groups containing only one rule node) --
    # A group wrapping a single leaf rule adds no structural value.
    # Groups confirmed by indent structure are exempt — their children exist
    # but may have miscoded rule_codes (e.g. scraper flattening (b)(i) → (i))
    # so prefix-based membership counting would miss them.
    group_member_counts: dict[str, int] = {gid: 0 for gid in groups}
    for node in nodes:
        rc = node.get("rule_code", "")
        if not rc or node["node_index"] not in valid_nodes:
            continue
        full = _normalise_full(rc)
        for gid in group_member_counts:
            if full == gid or full.startswith(gid + "_"):
                group_member_counts[gid] += 1

    ROMAN_RE = re.compile(r"^[ivx]+$")
    for gid, count in list(group_member_counts.items()):
        # Remove singletons (unless indent-confirmed)
        if count <= 1 and gid not in indent_confirmed:
            del groups[gid]
            continue

        # Remove roman-numeral groups created by chain inference.
        # The scraper flattens bracket nesting (e.g. (b)(i) → (i)), so
        # chain inference creates false groups like 4_x_i.  Their members
        # actually belong to the indent-confirmed alpha sibling (4_x_b).
        last_seg = gid.rsplit("_", 1)[-1]
        if ROMAN_RE.match(last_seg) and gid not in indent_confirmed:
            del groups[gid]

    # --- 4. Sort groups by first appearance then by id ----------------------
    sorted_groups = sorted(groups.values(), key=lambda g: (g["first_node_index"], g["id"]))

    logger.info(f"Identified {len(sorted_groups)} JSON Forms groups.")
    return sorted_groups


def save_groups_json(groups: list[dict], run_dir: str) -> str:
    """Write groups.json to the run directory."""
    output_path = os.path.join(run_dir, "groups.json")
    with open(output_path, "w") as f:
        json.dump(groups, f, indent=2)
    logger.info(f"Saved {len(groups)} groups → {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# SVG visualisation of nodes & groups
# ---------------------------------------------------------------------------

SVG_LEFT_MARGIN = 10      # px before group columns
SVG_COL_WIDTH = 14        # px per group-depth column
SVG_NODE_GAP = 2          # 1px line + 1px padding
SVG_INDENT_SCALE = 1.0    # scale factor for x_indent in the node line area


def build_svg(nodes: list[dict], groups: list[dict], run_dir: str) -> str:
    """Create an SVG showing one horizontal line per node with group columns."""

    # Filter out false-positive rule_codes (same filter used by build_groups)
    valid_nodes = _filter_sequential_rule_codes(nodes)

    max_depth = max((g["depth"] for g in groups), default=0) + 1
    group_cols_width = SVG_LEFT_MARGIN + max_depth * SVG_COL_WIDTH + 10  # space for columns

    max_x = max(n["x_indent"] for n in nodes)
    node_area_width = int(max_x * SVG_INDENT_SCALE) + 40
    svg_width = group_cols_width + node_area_width

    total_height = len(nodes) * SVG_NODE_GAP + 20  # +20 for top/bottom padding

    # Pre-compute: for each group, the y-range it spans (first_node_index … last_node_index)
    group_spans: dict[str, tuple[int, int]] = {}
    # A node belongs to a group if its rule_code starts with the group's prefix
    for g in groups:
        gid = g["id"]
        first = None
        last = None
        for node in nodes:
            rc = node.get("rule_code", "")
            if not rc or node["node_index"] not in valid_nodes:
                continue
            full = _normalise_full(rc)
            if full == gid or full.startswith(gid + "_"):
                ni = node["node_index"]
                if first is None:
                    first = ni
                last = ni
        if first is not None:
            group_spans[gid] = (first, last)

    def y_for(node_index: int) -> float:
        return 10 + node_index * SVG_NODE_GAP

    # Assign colours per depth
    depth_colours = [
        "#e74c3c", "#e67e22", "#f1c40f", "#2ecc71",
        "#3498db", "#9b59b6", "#1abc9c", "#e91e63",
    ]

    lines: list[str] = []
    lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{svg_width}" height="{total_height}">')
    lines.append(f'<rect width="{svg_width}" height="{total_height}" fill="#1a1a2e"/>')

    # --- Draw group vertical lines ---
    for g in groups:
        gid = g["id"]
        if gid not in group_spans:
            continue
        first, last = group_spans[gid]
        depth = g["depth"]
        x = SVG_LEFT_MARGIN + depth * SVG_COL_WIDTH
        y1 = y_for(first)
        y2 = y_for(last) + 1
        colour = depth_colours[depth % len(depth_colours)]
        lines.append(
            f'<line x1="{x}" y1="{y1}" x2="{x}" y2="{y2}" '
            f'stroke="{colour}" stroke-width="2" opacity="0.8"/>'
        )
        # Small label at top of each group line
        lines.append(
            f'<text x="{x + 2}" y="{y1 - 1}" font-size="3" fill="{colour}" '
            f'font-family="monospace" opacity="0.9">{gid}</text>'
        )

    # --- Draw node horizontal lines ---
    for node in nodes:
        ni = node["node_index"]
        y = y_for(ni)
        x_start = group_cols_width + node["x_indent"] * SVG_INDENT_SCALE
        x_end = x_start + 60  # fixed-width line to represent the node
        colour = "#e0e0e0" if node["rule_code"] else "#666666"
        lines.append(
            f'<line x1="{x_start}" y1="{y}" x2="{x_end}" y2="{y}" '
            f'stroke="{colour}" stroke-width="1"/>'
        )

    lines.append("</svg>")
    svg_content = "\n".join(lines)

    output_path = os.path.join(run_dir, "groups.svg")
    with open(output_path, "w") as f:
        f.write(svg_content)
    logger.info(f"SVG visualisation → {output_path}")
    return output_path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_pipeline(pdf_path: str):
    run_id = next_run_id()
    run_dir = os.path.join(RUNS_DIR, str(run_id))
    os.makedirs(run_dir, exist_ok=True)
    logger.info(f"Run {run_id} → {run_dir}")

    # 1. Scrape
    scraper = PDFScraper(pdf_path, run_dir)
    nodes = scraper.scrape()

    if not nodes:
        logger.warning("No text nodes extracted — nothing to save.")
        return

    # 2. Assign parent hierarchy via indentation
    assign_parents(nodes)

    # 3. Assign top-level grouping
    assign_top_level(nodes)

    # 4. Link cross-references
    link_references(nodes)

    # 5. Save JSON (drop bbox from the main output — it's only needed for excerpts)
    output_path = os.path.join(run_dir, "nodes.json")
    output_nodes = []
    for n in nodes:
        out = dict(n)
        out.pop("bbox", None)
        output_nodes.append(out)

    with open(output_path, "w") as f:
        json.dump(output_nodes, f, indent=2)

    # 6. Identify JSON Forms groups
    groups = build_groups(output_nodes)
    save_groups_json(groups, run_dir)

    # 7. SVG visualisation
    build_svg(output_nodes, groups, run_dir)

    logger.info(f"Saved {len(output_nodes)} nodes → {output_path}")
    logger.info(f"PDF excerpts   → {os.path.join(run_dir, 'excerpts')}/")
    print(f"\nDone! Run {run_id}")
    print(f"  nodes.json : {output_path}")
    print(f"  excerpts/  : {os.path.join(run_dir, 'excerpts')}/")


def run_groups(nodes_path: str):
    """Run only the group-identification and SVG steps on an existing nodes.json."""
    with open(nodes_path) as f:
        nodes = json.load(f)
    run_dir = os.path.dirname(os.path.abspath(nodes_path))

    groups = build_groups(nodes)
    groups_path = save_groups_json(groups, run_dir)
    svg_path = build_svg(nodes, groups, run_dir)

    print(f"\nDone!")
    print(f"  groups.json : {groups_path}")
    print(f"  groups.svg  : {svg_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape a PDF and extract text nodes to JSON.")
    sub = parser.add_subparsers(dest="command")

    # Default: full pipeline
    scrape_p = sub.add_parser("scrape", help="Full PDF scrape pipeline")
    scrape_p.add_argument("pdf", nargs="?", default="chapter4.pdf", help="Path to the PDF file")

    # Groups-only on existing nodes.json
    groups_p = sub.add_parser("groups", help="Identify groups from an existing nodes.json")
    groups_p.add_argument("nodes_json", help="Path to nodes.json")

    args = parser.parse_args()
    if args.command == "groups":
        run_groups(args.nodes_json)
    else:
        run_pipeline(getattr(args, "pdf", "chapter4.pdf"))
