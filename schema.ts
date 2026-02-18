// schema.ts — Record types for the compliance form system
//
// Evolved from compliance-3's field/group/rule model.
// "field" is now "control" to better reflect that each item represents
// a compliance control point, not just a form field.

// ---------------------------------------------------------------------------
// Control — a single compliance control point (question) within the form
// ---------------------------------------------------------------------------
export interface Control {
  /** Hierarchical ID using underscore notation, e.g. "4_1_3_1_a" */
  id: string;

  /** The question or statement presented to the user */
  label: string;

  /**
   * Whether answering "Yes" requires the user to provide supporting detail.
   * When true, a free-text detail input appears conditionally.
   */
  "detail-required": boolean;

  /**
   * The expected correct answer for compliance validation:
   *  - "Yes" / "No" — a specific answer is required for compliance
   *  - "N/A"         — any answer is acceptable (scope-gate question)
   */
  "correct-option": "Yes" | "No" | "N/A";

  /** Custom label for the detail text input (defaults to "Please provide details:") */
  "detail-label"?: string;

  /**
   * Optional link to a business process that this control is relevant to.
   * References a LegislationProcess.id from the process catalog.
   */
  "process-id"?: string;

  /** Regulation rule codes this control covers (process mode), e.g. ["4.3.5(1)", "4.3.5(2)"] */
  "source-rules"?: string[];

  /**
   * LLM self-assessed confidence in the mapping from regulation to control (0.0–1.0).
   * 1.0 = direct, unambiguous mapping. 0.5 = reasonable interpretation.
   * Below 0.5 = uncertain, flagged for review.
   */
  "mapping-confidence"?: number;
}

// ---------------------------------------------------------------------------
// Group — an organisational container that holds controls and/or sub-groups
// ---------------------------------------------------------------------------
export interface Group {
  /** Hierarchical ID matching the parent prefix of its children, e.g. "4_1_3" */
  id: string;

  /** Display title for this section */
  title: string;

  /** Explanatory text shown beneath the group heading */
  description?: string;
}

// ---------------------------------------------------------------------------
// Rule — conditional visibility logic binding a target to a scope control
// ---------------------------------------------------------------------------
export interface Rule {
  /** The control or group ID whose visibility is affected */
  target: string;

  /** The control ID whose answer determines visibility */
  scope: string;

  /** The visibility effect to apply when the condition is met */
  effect: "SHOW" | "HIDE";

  /** The condition: the scope control's value must equal this constant */
  schema: { const: string };
}

// ---------------------------------------------------------------------------
// SectionData — a complete data bundle for one form section
// ---------------------------------------------------------------------------
export interface SectionData {
  controls: Control[];
  groups: Group[];
  rules: Rule[];
}

// ---------------------------------------------------------------------------
// Status evaluation (mirrors compliance-3/6 logic)
// ---------------------------------------------------------------------------
export type ControlStatus = "pending" | "success" | "warning" | "error";

export function getControlStatus(
  control: Control,
  data: Record<string, string>,
): ControlStatus {
  const answer = data[control.id];
  const detail = data[`${control.id}_detail`];

  if (answer === undefined || answer === "") return "pending";
  if (!control["correct-option"] || control["correct-option"] === "N/A")
    return "success";
  if (answer !== control["correct-option"]) return "error";
  if (control["detail-required"] && (!detail || detail.trim() === ""))
    return "warning";
  return "success";
}
