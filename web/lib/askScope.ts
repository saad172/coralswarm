// Ask v2 model + answer-option logic (ADR-0011, frontend stories 5/6), plus
// workspace classification. Pure (no React) so it can be unit-reasoned and
// shared. Chats always asks across every readable workspace, so the old
// workspace-picker scope codec is gone (see `lib/scopedChatScope.ts`).

import type { OceanKind } from "./api";

// ── Model tiers (ADR-0011 §D; two-model-world amendment, 2026-09-03) ─────────
// Capability labels (Fast/Smart/Max) survive version bumps and provider
// changes; hover copy shows the human `cap` + `bestFor` fields — provider-
// neutral by design, never a model/provider name. The wire `model` field
// carries the stable TIER keyword — the backend maps it to the allowlisted,
// env-configured id (D16: never trust a raw client string). Post-cutover:
// Fast = a cheap-tier model with reasoning off and no dial; Smart (default) =
// a capable-tier model with a four-rung `off`/`low`/`medium`/`high` dial,
// default `medium`; Max = the same capable-tier model pinned to `high`, dial
// hidden. The same tier keywords resolve server-side to DeepSeek (Fast) and
// GLM (Smart/Max). This module never names a specific model.
export type ModelTier = "fast" | "smart" | "max";
export interface ModelSpec {
  tier: ModelTier;
  label: string; // "Fast"
  model: string; // wire value (the tier keyword) — API-only, never rendered
  cap: string; // short, provider-neutral capability word
  bestFor: string; // "best for…" hover copy
}
export const MODELS: Record<ModelTier, ModelSpec> = {
  fast: {
    tier: "fast",
    label: "Fast",
    model: "fast",
    cap: "Quick",
    bestFor: "Quick lookups and short, factual answers.",
  },
  smart: {
    tier: "smart",
    label: "Smart",
    model: "smart",
    cap: "Balanced",
    bestFor: "Balanced reasoning across several sources.",
  },
  max: {
    tier: "max",
    label: "Max",
    model: "max",
    cap: "Deep",
    bestFor: "Deepest reasoning; slowest.",
  },
};
export const MODEL_TIERS: ModelTier[] = ["fast", "smart", "max"];
export const DEFAULT_MODEL: ModelTier = "smart";
export function isModelTier(s: string | null | undefined): s is ModelTier {
  return s === "fast" || s === "smart" || s === "max";
}

// ── Reasoning effort (two-model-world amendment, 2026-09-03) ────────────────
// Collapsed from the old five-rung Anthropic-only ladder (low/medium/high/
// xhigh/max) to four: off < low < medium (the default) < high. Applies to the
// Smart tier only — Fast never reasons (no dial, always off) and Max is
// pinned to `high` (dial hidden, fixed indicator instead). See
// `effortAppliesToModel`.
export type EffortLevel = "off" | "low" | "medium" | "high";
export interface EffortSpec {
  key: EffortLevel;
  label: string;
  /** One-line "what this level buys you" copy for the composer's effort dial. */
  blurb: string;
}
export const EFFORT_LEVELS: EffortSpec[] = [
  { key: "off", label: "Off", blurb: "No extra reasoning — fastest, for simple lookups." },
  { key: "low", label: "Low", blurb: "Light reasoning, best for quick lookups." },
  { key: "medium", label: "Medium", blurb: "The default — balanced reasoning for most questions." },
  { key: "high", label: "High", blurb: "As thorough as the model gets — the slowest setting." },
];
export const DEFAULT_EFFORT: EffortLevel = "medium";
export function isEffortLevel(s: string | null | undefined): s is EffortLevel {
  return EFFORT_LEVELS.some((e) => e.key === s);
}

/** Whether the reasoning-effort dial should be SHOWN for this model tier.
 *  True only for Smart: Fast never reasons (no dial, always off) and Max is
 *  pinned to `high` with the dial hidden (a fixed indicator instead). The
 *  composer collapses its effort dial when this is false, in place of
 *  showing a control that either does nothing (Fast) or is not actually
 *  adjustable (Max). */
export function effortAppliesToModel(tier: ModelTier): boolean {
  return tier === "smart";
}

/** The `effort` value to actually put on the wire for a given tier —
 *  `undefined` on Fast and Max (the backend pins both regardless of what is
 *  sent: Fast to no reasoning, Max to `high`), so sending a field known to be
 *  inert there is misleading; `send` omits it at the source instead of
 *  relying on the server to ignore it. */
export function effortForWire(tier: ModelTier, effort: EffortLevel): EffortLevel | undefined {
  return effortAppliesToModel(tier) ? effort : undefined;
}

// ── Per-ask options (ADR-0011 §F) ────────────────────────────────────────────
export type AnswerStyle = "concise" | "detailed" | "sources-only";
export type Depth = "fast" | "thorough";
export const ANSWER_STYLES: AnswerStyle[] = ["concise", "detailed", "sources-only"];

// ── Workspace classification ─────────────────────────────────────────────────
/**
 * True when `g` is reachable ONLY by its owner: the consumer-plane personal
 * primary, or — on an org host, where a personal ocean can never exist
 * (ADR-0007 D2) — this member's own ocean inside the org tenant.
 *
 * This is an ACCESS fact, not a privacy claim, and it is deliberately narrow
 * in what it may be used for: grouping and labelling in the Workspaces index,
 * where an owner-only ocean is worth telling apart from one a member's
 * coworkers can read. It must NEVER gate query scope again — that was the old
 * `sharedOceans()` filter, and it is what made an owner-only ocean unaskable
 * alongside anything else.
 */
export function isOwnerOnlyOcean(g: { is_primary: boolean; kind: OceanKind }): boolean {
  // NOTE THE ASYMMETRY, it is deliberate.
  //
  // `member`: owner-only regardless of `is_primary`. EVERY member ocean is
  // owner-only by construction — the backend's reachability predicate gates it
  // on `owner_user_id = $caller` with no reference to `is_primary`, and a
  // member may hold secondary member oceans alongside their primary.
  //
  // `personal`: still requires `is_primary`. Non-primary personal oceans on
  // the consumer plane are ordinary shareable oceans and must keep saying so.
  return g.kind === "member" || (g.is_primary && g.kind === "personal");
}

/** Ocean-kind badge shown next to a graph's name in switcher menus (Shell's
 *  header + mobile ocean dropdowns, Atlas's ocean menu) — one implementation
 *  shared by all of them so a badge fix in one place can't drift from the
 *  others. Reads the KIND only: `is_primary` used to win here and render
 *  "primary", which is internal vocabulary that told a user nothing about who
 *  can see the ocean. */
export function oceanBadge(g: { kind: OceanKind; is_primary: boolean }): string {
  if (g.kind === "member") return "member";
  if (g.kind === "team") return "team";
  if (g.kind === "org") return "org";
  return "personal";
}
