/* Graph adapter — maps the backend's semantic AtlasGraph
   (GET /v1/graphs/{id}/atlas) into a graphology graph carrying the display
   attributes Sigma renders: every node gets a colour, a size, a program type
   and a human sublabel; every edge gets a program type, width, colour and its
   review state. The backend stays style-agnostic; ALL visual mapping lives
   here, and this module is pure (no DOM, no Sigma import) so it is unit-
   tested in vitest's node environment.

   Hue taxonomy (docs/design/DESIGN_SYSTEM.md): coral = decisions/brand ·
   pink = people/entities · amber = sources/threads/candidates · teal =
   ingested/meetings · plum = systems/references. Carried over verbatim from
   the archived Atlas adapter so the graph reads with the same key as the
   rest of the app.

   Layout seeds are DETERMINISTIC: every position derives from an FNV hash of
   the node id, never Math.random, so ForceAtlas2 starts from the same
   picture on every load and converges to the same (or a near-identical)
   arrangement. The ocean hub is pinned at the origin (`fixed: true`, which
   graphology-layout-forceatlas2 honours) so the whole constellation has a
   stable centre to orbit.

   Core vs. halo. A real ocean is mostly UNLINKED: reefs and decisions that
   exist but have no confirmed current to anything else (in a 1.7k-node
   personal ocean, ~1,150 of them). Drawn like everything else, and pulled
   toward the hub by their "in ocean" spoke, they pack into a ring that hides
   the actual structure. So: spokes carry no layout weight and are not drawn;
   linked nodes are seeded in an inner core and sized by how connected they
   are; unlinked nodes are seeded in an outer halo, drawn small and faded, and
   never labelled until you zoom in. The picture then reads core = knowledge
   that connects, halo = everything else the ocean knows about. */

import { MultiDirectedGraph } from "graphology";
import type { AtlasEdgeOrigin, AtlasEdgeStatus, AtlasGraph, AtlasLink, AtlasNode, AtlasNodeType, OceanKind } from "@/lib/api";

export type HueName = "coral" | "pink" | "amber" | "teal" | "plum";

/** Resolved CSS colours. The canvas reads these from `:root` at mount
 *  (`readPalette` in GraphCanvas.tsx); tests use `DEFAULT_PALETTE`. */
export interface Palette {
  coral: string;
  pink: string;
  amber: string;
  teal: string;
  plum: string;
  ink: string;
  ink2: string;
  ink3: string;
  line: string;
  bg: string;
}

/** Fallbacks mirror `web/app/globals.css` `:root`. */
export const DEFAULT_PALETTE: Palette = {
  coral: "#f77764",
  pink: "#eb7290",
  amber: "#f4b860",
  teal: "#2a9d8f",
  plum: "#7c5295",
  ink: "#14161a",
  ink2: "#4b5058",
  ink3: "#8a9099",
  line: "#e4e6ea",
  bg: "#ffffff",
};

/** Sigma node program names registered by GraphCanvas.tsx. `bordered` is the
 *  @sigma/node-border program (a ring in `borderColor` around the fill). */
export type NodeProgram = "circle" | "bordered";
/** Sigma edge program names registered by GraphCanvas.tsx. */
export type EdgeProgram = "line" | "curve";

export interface GraphNodeAttrs {
  label: string;
  size: number;
  color: string;
  x: number;
  y: number;
  type: NodeProgram;
  /** Only meaningful when `type === "bordered"`. */
  borderColor?: string;
  /** Pinned for the layout worker: the ocean hub, and halo nodes (which are
   *  placed by `placeHalo` after the run instead). */
  fixed?: boolean;
  /** Draw order: higher on top. Sigma reads this when `zIndex: true`. */
  zIndex: number;
  /** Structural connections (typed currents, coral membership, authorship);
   *  hub spokes don't count. Drives size and core/halo placement. */
  degree: number;
  /** `degree > 0`. Unlinked reefs/decisions form the faded outer halo. */
  linked: boolean;
  nodeType: AtlasNodeType;
  hue: HueName | "ink";
  /** Human sublabel for panels: "entity reef", "decision · Jul 8", "author". */
  sub: string;
  reefKind?: string;
  count?: number;
  date?: string;
  sourceType?: string;
  /** Source glyph letter (e.g. "S" for Slack). */
  letter?: string;
  /** Person initials (e.g. "SK"); "?" when unresolved. */
  initials?: string;
  /** Parent reef id — coral only. */
  reefId?: string | null;
  /** Ocean this reef/decision was promoted from — reef/decision only. */
  promotedFrom?: string | null;
  /** Ocean-owner self reef, rendered as the "you" hub. */
  isSelf?: boolean;
}

export interface GraphEdgeAttrs {
  kind: string;
  type: EdgeProgram;
  size: number;
  color: string;
  zIndex: number;
  /** Attribution edges (authored / coral-source) are hidden unless an
   *  endpoint is the active node — the archive's EDGE_CLS 0 idiom. */
  attribution: boolean;
  /** Hub spokes ("in ocean", "member"): kept for the panels' counts, never
   *  drawn. Sigma honours this attribute natively. */
  hidden?: boolean;
  /** ForceAtlas2 attraction. 0 for spokes so the hub doesn't pull every
   *  unlinked node into a ring; typed currents pull hardest. */
  weight: number;
  predicate?: string;
  edgeId?: string;
  status?: AtlasEdgeStatus;
  origin?: AtlasEdgeOrigin;
  confidence?: number;
  /** Shared-coral count on derived currents. */
  shared?: number;
  /** 1-based decision-anchor rank. */
  anchorRank?: number;
  /** Owner→topic stated spoke — data only, never a layout edge. */
  owner?: boolean;
}

export type GraphModel = MultiDirectedGraph<GraphNodeAttrs, GraphEdgeAttrs>;

export interface BuiltGraph {
  name: string;
  oceanKind: OceanKind;
  graph: GraphModel;
  /** Per type, plus `total` and `unlinked` (reefs + decisions with no
   *  structural connection — the halo). */
  counts: Record<AtlasNodeType, number> & { total: number; unlinked: number };
  /** Backend-side sampling: the ocean has more than the newest 100 atoms. */
  truncated: boolean;
  /** Threshold the atlas actually used for `co-occurs`. */
  cooccur: number;
  selfNodeId: string | null;
}

/* ── Hue tables ──────────────────────────────────────────────────────────── */

const OCEAN_HUE: Record<OceanKind, HueName> = { personal: "coral", team: "teal", org: "plum", member: "pink" };
const REEF_HUE: Record<string, HueName> = { entity: "pink", thread: "amber", reference: "plum" };
const PERSON_HUES: HueName[] = ["coral", "pink", "amber", "teal", "plum"];

const SOURCE_META: Record<string, { hue: HueName; letter: string; name: string }> = {
  slack: { hue: "amber", letter: "S", name: "Slack" },
  zoom: { hue: "teal", letter: "Z", name: "Zoom" },
  notion: { hue: "plum", letter: "N", name: "Notion" },
  github: { hue: "pink", letter: "G", name: "GitHub" },
  linear: { hue: "plum", letter: "L", name: "Linear" },
  drive: { hue: "coral", letter: "D", name: "Drive" },
  gmail: { hue: "coral", letter: "M", name: "Gmail" },
  outlook: { hue: "teal", letter: "O", name: "Outlook" },
  teams: { hue: "teal", letter: "T", name: "Teams" },
  teams_channel: { hue: "teal", letter: "T", name: "Teams" },
  teams_chat: { hue: "teal", letter: "T", name: "Teams" },
  teams_transcript: { hue: "teal", letter: "T", name: "Teams" },
  microsoft_calendar: { hue: "teal", letter: "C", name: "Calendar" },
};

export function sourceMeta(sourceType: string): { hue: HueName; letter: string; name: string } {
  return (
    SOURCE_META[sourceType] ?? {
      hue: "plum",
      letter: (sourceType[0] ?? "?").toUpperCase(),
      name: prettifySource(sourceType),
    }
  );
}

export function prettifySource(s: string): string {
  return s
    .split(/[_\s]+/)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

export function initialsOf(name: string): string {
  const parts = name.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return "?";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

/* ── Deterministic seeds ─────────────────────────────────────────────────── */

/** FNV-1a 32-bit. Two independent 0..1 draws per id (hash and re-hash). */
function fnv(id: string): number {
  let h = 2166136261;
  for (let i = 0; i < id.length; i++) {
    h ^= id.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}
export function hash01(id: string): number {
  return (fnv(id) % 10000) / 10000;
}
function hash01b(id: string): number {
  return (fnv(id + "\u0001") % 10000) / 10000;
}

/** Seed bands (world units). Core = anything with a structural connection;
 *  halo = unlinked reefs/decisions. Halo nodes are `fixed` for the layout
 *  run (they have no edges, so ForceAtlas2 could only drift them) and are
 *  re-placed relative to the settled core by `placeHalo`; their seed band
 *  just keeps them out of the core's way while it converges. */
const CORE_R0 = 90;
const CORE_R1 = 320;
const HALO_R0 = 560;
const HALO_R1 = 820;

/** Seed position on a ring band. `r0..r1` world units, angle from the id. */
function ringSeed(id: string, r0: number, r1: number): { x: number; y: number } {
  const a = hash01(id) * Math.PI * 2;
  const r = r0 + hash01b(id) * (r1 - r0);
  return { x: Math.cos(a) * r, y: Math.sin(a) * r };
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
function fmtDate(iso?: string): string | null {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return `${MONTHS[d.getMonth()]} ${d.getDate()}`;
}

/** Reef size from how CONNECTED it is first, how many corals it holds second:
 *  a reef with 20 currents is a landmark even with few corals, while an
 *  unlinked reef stays a dot however many corals sit under it. Unlinked →
 *  3.2–5; linked: 1 current → ~5.5, 10 → ~8.5, 100+ → 18 (capped). Sigma
 *  sizes are screen pixels at zoom 1. */
export function reefSize(count: number, degree = 0): number {
  const c = Math.sqrt(Math.max(0, count));
  if (degree <= 0) return Math.min(5, 3.2 + 0.35 * c);
  return Math.min(18, 4.2 + 1.3 * Math.sqrt(degree) + 0.35 * c);
}

/** Display label: a decision's full sentence or a long reef title turns the
 *  overview into a wall of text. Panels and search still use the full
 *  `label`; the canvas reducer shows the full text for the active node. */
export const LABEL_MAX = 34;
export function shortLabel(label: string, max = LABEL_MAX): string {
  const s = label.trim();
  if (s.length <= max) return s;
  const cut = s.slice(0, max - 1);
  const sp = cut.lastIndexOf(" ");
  return (sp > max * 0.6 ? cut.slice(0, sp) : cut).trimEnd() + "…";
}

/** `#rrggbb` / `#rgb` / `#rrggbbaa` / `rgb()` / `rgba()` → the same colour
 *  with its alpha MULTIPLIED by `a` (output `#rrggbbaa` or `rgba()`, both of
 *  which Sigma parses natively). Multiplying, not replacing, is what lets
 *  the hover reducer dim an already-faded halo node below the dimmed core
 *  instead of leaving it untouched. Unknown formats pass through. */
export function fade(color: string, a: number): string {
  const k = Math.max(0, Math.min(1, a));
  const hex = (x: number) =>
    Math.round(Math.max(0, Math.min(1, x)) * 255)
      .toString(16)
      .padStart(2, "0");
  const c = color.trim();
  if (/^#[0-9a-f]{8}$/i.test(c)) return c.slice(0, 7) + hex((parseInt(c.slice(7, 9), 16) / 255) * k);
  if (/^#[0-9a-f]{6}$/i.test(c)) return c + hex(k);
  if (/^#[0-9a-f]{3}$/i.test(c)) {
    const [r, g, b] = c.slice(1).split("");
    return `#${r}${r}${g}${g}${b}${b}${hex(k)}`;
  }
  const m = c.match(/^rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*([\d.]+))?\s*\)$/i);
  if (m) {
    const existing = m[4] !== undefined ? parseFloat(m[4]) : 1;
    return `rgba(${m[1]},${m[2]},${m[3]},${+(existing * k).toFixed(4)})`;
  }
  return c;
}

/** Opacity of unlinked (halo) reefs and decisions. */
export const HALO_ALPHA = 0.58;

/** Halo ring, as multiples of the core's measured radius. */
export const HALO_INNER = 1.12;
export const HALO_OUTER = 1.55;

/** Place every unlinked reef/decision on a ring just outside the connected
 *  core. Called once the layout has settled: the halo has no edges, so
 *  ForceAtlas2 has nothing to say about where it goes, and leaving it at a
 *  fixed seed radius means the camera fits a giant donut around a thumbnail
 *  core. The core radius is the 95th percentile of linked-node distances from
 *  the pinned centre (so one flyaway node can't inflate it). Deterministic:
 *  angle from the id hash; unlinked reefs that hold corals sit nearer the
 *  core than empty ones and decisions, so the halo has some texture and the
 *  "busiest of the unlinked" are the first you meet zooming out. */
export function placeHalo(graph: GraphModel): number {
  const radii: number[] = [];
  const halo: string[] = [];
  graph.forEachNode((id, a) => {
    if (!a.linked) {
      if (!a.isSelf && (a.nodeType === "reef" || a.nodeType === "decision")) halo.push(id);
      return;
    }
    if (a.fixed) return;
    radii.push(Math.hypot(a.x, a.y));
  });
  if (halo.length === 0) return 0;
  radii.sort((p, q) => p - q);
  const core = radii.length ? Math.max(40, radii[Math.min(radii.length - 1, Math.floor(radii.length * 0.95))]) : 120;
  for (const id of halo) {
    const a = graph.getNodeAttributes(id);
    const busy = a.nodeType === "reef" && (a.count ?? 0) > 0;
    // reefs with corals: inner 70% of the band; empty reefs + decisions: outer 75%.
    const t = busy ? hash01b(id) * 0.7 : 0.25 + hash01b(id) * 0.75;
    const r = core * (HALO_INNER + (HALO_OUTER - HALO_INNER) * t);
    const ang = hash01(id) * Math.PI * 2;
    graph.mergeNodeAttributes(id, { x: Math.cos(ang) * r, y: Math.sin(ang) * r });
  }
  return core;
}

/* ── Edge styling ────────────────────────────────────────────────────────── */

/** Which edge kinds are attribution (drawn only when an endpoint is active). */
const ATTRIBUTION_KINDS = new Set(["authored", "coral-source"]);
/** Hub spokes: structural for the panels' counts, invisible on the canvas
 *  and weightless in the layout. Owner→topic stated currents join this set
 *  at the link (`owner: true`), not by kind. */
export const SPOKE_KINDS = new Set(["ocean-reef", "ocean-decision", "member"]);
/** Pair kinds that share one undirected slot — stated + derived merge. */
const PAIR_KINDS = new Set(["reef-reef", "decision-reef", "co-occurs"]);
export const COOCCUR_WEIGHT_CAP = 10;
export const ANCHOR_WEIGHT = 0.1;

export function isLayoutSpoke(l: Pick<AtlasLink, "kind" | "owner">): boolean {
  return !!l.owner || SPOKE_KINDS.has(l.kind);
}

function pairKey(a: string, b: string): string {
  return a <= b ? `${a}\0${b}` : `${b}\0${a}`;
}

function edgeStyle(
  kind: string,
  status: AtlasEdgeStatus | undefined,
  origin: AtlasEdgeOrigin | undefined,
  p: Palette,
): Pick<GraphEdgeAttrs, "type" | "size" | "color" | "zIndex" | "attribution" | "hidden" | "weight"> {
  if (kind === "co-occurs" || (origin === "derived" && kind === "decision-reef")) {
    const anchor = kind === "decision-reef";
    return {
      type: "curve",
      size: 0.7,
      color: fade(p.ink3, 0.32),
      zIndex: 2,
      attribution: false,
      weight: anchor ? ANCHOR_WEIGHT : 1,
    };
  }
  switch (kind) {
    case "reef-reef":
    case "decision-reef":
      // Confirmed = a fact (solid ink); suggested = a tentative guess (amber,
      // thinner). Sigma has no dashed edge program, so colour + width carry
      // the distinction; the page's detail panel spells it out.
      return status === "suggested"
        ? { type: "curve", size: 1.1, color: p.amber, zIndex: 2, attribution: false, weight: 0.6 }
        : { type: "curve", size: 1.2, color: fade(p.ink3, 0.45), zIndex: 3, attribution: false, weight: 1 };
    case "ocean-reef":
    case "ocean-decision":
    case "member":
      return { type: "line", size: 0.9, color: p.line, zIndex: 1, attribution: false, hidden: true, weight: 0 };
    case "supports":
      return { type: "line", size: 1.0, color: p.coral, zIndex: 2, attribution: false, weight: 0.8 };
    case "reef-coral":
      return { type: "line", size: 0.5, color: p.line, zIndex: 0, attribution: false, weight: 0.5 };
    case "authored":
    case "coral-source":
    default:
      return {
        type: "line",
        size: 0.7,
        color: p.ink3,
        zIndex: 0,
        attribution: ATTRIBUTION_KINDS.has(kind),
        weight: 0.25,
      };
  }
}

function layoutWeight(l: AtlasLink): number {
  if (isLayoutSpoke(l)) return 0;
  if (l.origin === "derived" && l.kind === "decision-reef") return ANCHOR_WEIGHT;
  if (l.kind === "co-occurs" || l.origin === "derived") {
    return Math.min(Math.max(l.weight ?? 1, 1), COOCCUR_WEIGHT_CAP);
  }
  if (l.kind === "reef-coral") return 0.5;
  if (l.kind === "supports") return 0.8;
  if (l.kind === "authored" || l.kind === "coral-source") return 0.25;
  return l.status === "suggested" ? 0.6 : 1;
}

/* ── Build ───────────────────────────────────────────────────────────────── */

export function buildGraphModel(atlas: AtlasGraph, palette: Palette = DEFAULT_PALETTE): BuiltGraph {
  const graph: GraphModel = new MultiDirectedGraph<GraphNodeAttrs, GraphEdgeAttrs>();
  const hueOf = (h: HueName | "ink") => (h === "ink" ? palette.ink : palette[h]);

  // Pass 0: structural degree per node (every link except hub spokes), so a
  // node knows whether it belongs to the core or the halo before it is
  // placed or sized.
  const known = new Set(atlas.nodes.map((n) => n.id));
  const degree = new Map<string, number>();
  const bump = (id: string) => degree.set(id, (degree.get(id) ?? 0) + 1);
  for (const l of atlas.links) {
    if (isLayoutSpoke(l) || !known.has(l.a) || !known.has(l.b)) continue;
    bump(l.a);
    bump(l.b);
  }
  const degreeOf = (id: string) => degree.get(id) ?? 0;

  // Pass 1: reefs + decisions, so corals can inherit their parent's hue and
  // seed near it. Linked nodes seed in the core band, unlinked in the halo.
  const reefHue = new Map<string, HueName>();
  const reefLabel = new Map<string, string>();
  const reefSeed = new Map<string, { x: number; y: number }>();
  for (const n of atlas.nodes) {
    if (n.type === "reef") {
      reefHue.set(n.id, REEF_HUE[n.reef_kind ?? ""] ?? "plum");
      reefLabel.set(n.id, n.label ?? "");
      reefSeed.set(
        n.id,
        n.is_self || degreeOf(n.id) > 0 ? ringSeed(n.id, CORE_R0, CORE_R1) : ringSeed(n.id, HALO_R0, HALO_R1),
      );
    } else if (n.type === "decision") {
      reefHue.set(n.id, "coral");
      reefLabel.set(n.id, n.label ?? "");
      reefSeed.set(n.id, degreeOf(n.id) > 0 ? ringSeed(n.id, CORE_R0, CORE_R1) : ringSeed(n.id, HALO_R0, HALO_R1));
    }
  }

  let personIdx = 0;
  for (const n of atlas.nodes) {
    if (graph.hasNode(n.id)) continue;
    graph.addNode(
      n.id,
      adaptNode(n, atlas.ocean.kind, palette, hueOf, reefHue, reefLabel, reefSeed, degreeOf(n.id), () => personIdx++),
    );
  }

  const pairToKey = new Map<string, string>();
  for (const l of atlas.links) {
    if (!graph.hasNode(l.a) || !graph.hasNode(l.b)) continue;
    // Deterministic keys: typed edges use their relationship edge_id so the
    // page can flip `status` in place after a confirm/reject; the rest are
    // keyed by (kind, a, b), which also dedupes any repeated wire link.
    const key = l.edge_id ?? `${l.kind}:${l.a}:${l.b}`;
    const pk = PAIR_KINDS.has(l.kind) ? pairKey(l.a, l.b) : null;
    if (pk && !isLayoutSpoke(l)) {
      const existing = pairToKey.get(pk);
      if (existing && graph.hasEdge(existing)) {
        mergePairEdge(graph, existing, l, palette);
        continue;
      }
    }
    if (graph.hasEdge(key)) continue;
    const style = isLayoutSpoke(l)
      ? { type: "line" as const, size: 0.9, color: palette.line, zIndex: 1, attribution: false, hidden: true, weight: 0 }
      : edgeStyle(l.kind, l.status, l.origin, palette);
    graph.addEdgeWithKey(key, l.a, l.b, {
      kind: l.kind,
      ...style,
      weight: isLayoutSpoke(l) ? 0 : layoutWeight(l),
      predicate: l.predicate,
      edgeId: l.edge_id,
      status: l.status,
      origin: l.origin,
      confidence: l.confidence,
      shared: l.weight,
      anchorRank: l.anchor_rank,
      owner: l.owner,
    });
    if (pk && !isLayoutSpoke(l)) pairToKey.set(pk, key);
  }

  const counts: BuiltGraph["counts"] = {
    ocean: 0,
    reef: 0,
    decision: 0,
    coral: 0,
    source: 0,
    person: 0,
    total: 0,
    unlinked: 0,
  };
  graph.forEachNode((_, a) => {
    counts[a.nodeType] += 1;
    counts.total += 1;
    if (!a.linked && (a.nodeType === "reef" || a.nodeType === "decision")) counts.unlinked += 1;
  });

  return {
    name: atlas.ocean.name,
    oceanKind: atlas.ocean.kind,
    graph,
    counts,
    truncated: atlas.truncated.atoms,
    cooccur: atlas.cooccur ?? 2,
    selfNodeId: atlas.self_node_id ?? null,
  };
}

/** Stated wins the draw; derived contributes shared-coral weight (capped). */
function mergePairEdge(graph: GraphModel, existingKey: string, incoming: AtlasLink, palette: Palette): void {
  const cur = graph.getEdgeAttributes(existingKey);
  const incomingDerived = incoming.origin === "derived";
  const currentDerived = cur.origin === "derived";
  if (incomingDerived && !currentDerived) {
    const shared = incoming.weight;
    const w = incoming.kind === "decision-reef" ? ANCHOR_WEIGHT : layoutWeight(incoming);
    graph.mergeEdgeAttributes(existingKey, { shared, weight: Math.max(cur.weight, w) });
    return;
  }
  if (!incomingDerived && currentDerived) {
    const style = edgeStyle(incoming.kind, incoming.status, incoming.origin, palette);
    graph.mergeEdgeAttributes(existingKey, {
      ...style,
      kind: incoming.kind,
      predicate: incoming.predicate,
      edgeId: incoming.edge_id,
      status: incoming.status,
      origin: incoming.origin,
      confidence: incoming.confidence,
      owner: incoming.owner,
      weight: Math.max(layoutWeight(incoming), cur.weight),
      shared: cur.shared ?? incoming.weight,
    });
  }
}

function adaptNode(
  n: AtlasNode,
  oceanKind: OceanKind,
  p: Palette,
  hueOf: (h: HueName | "ink") => string,
  reefHue: Map<string, HueName>,
  reefLabel: Map<string, string>,
  reefSeed: Map<string, { x: number; y: number }>,
  degree: number,
  nextPersonIdx: () => number,
): GraphNodeAttrs {
  const linked = degree > 0;
  switch (n.type) {
    case "ocean": {
      const hue = OCEAN_HUE[oceanKind] ?? "coral";
      return {
        label: n.label ?? "",
        // A marker for the centre, not a hub: its spokes are neither drawn
        // nor weighted, so it should not read as the biggest thing on screen.
        size: 12,
        color: hueOf(hue),
        x: 0,
        y: 0,
        fixed: true,
        type: "bordered",
        borderColor: p.bg,
        zIndex: 10,
        degree,
        linked: true,
        nodeType: "ocean",
        hue,
        sub: `${oceanKind} workspace`,
      };
    }
    case "reef": {
      const hue = reefHue.get(n.id) ?? "plum";
      const cnt = n.count ?? 0;
      const seed = reefSeed.get(n.id) ?? ringSeed(n.id, HALO_R0, HALO_R1);
      const promoted = n.promoted_from_graph_id ?? null;
      if (n.is_self) {
        return {
          label: "you",
          size: 10,
          color: hueOf("pink"),
          x: seed.x,
          y: seed.y,
          type: "bordered",
          borderColor: p.bg,
          zIndex: 8,
          degree,
          linked: true,
          nodeType: "reef",
          hue: "pink",
          sub: "you · this workspace's owner",
          reefKind: n.reef_kind,
          count: cnt,
          promotedFrom: promoted,
          isSelf: true,
        };
      }
      return {
        label: n.label ?? "",
        size: reefSize(cnt, degree),
        color: linked ? hueOf(hue) : fade(hueOf(hue), HALO_ALPHA),
        x: seed.x,
        y: seed.y,
        fixed: !linked || undefined,
        // Promoted reefs wear a ring in ink so the cross-ocean copy is
        // visible at a glance; the detail panel names the source ocean.
        type: promoted ? "bordered" : "circle",
        borderColor: promoted ? (linked ? p.ink2 : fade(p.ink2, HALO_ALPHA)) : undefined,
        zIndex: linked ? 6 : 2,
        degree,
        linked,
        nodeType: "reef",
        hue,
        sub: `${n.reef_kind ?? "entity"} topic`,
        reefKind: n.reef_kind,
        count: cnt,
        promotedFrom: promoted,
      };
    }
    case "decision": {
      const date = fmtDate(n.date);
      const seed = reefSeed.get(n.id) ?? ringSeed(n.id, HALO_R0, HALO_R1);
      const promoted = n.promoted_from_graph_id ?? null;
      return {
        label: n.label ?? "",
        size: linked ? Math.min(12, 5 + 1.1 * Math.sqrt(degree)) : 3.4,
        color: linked ? hueOf("coral") : fade(hueOf("coral"), HALO_ALPHA),
        x: seed.x,
        y: seed.y,
        fixed: !linked || undefined,
        type: promoted ? "bordered" : "circle",
        borderColor: promoted ? (linked ? p.ink2 : fade(p.ink2, HALO_ALPHA)) : undefined,
        zIndex: linked ? 7 : 2,
        degree,
        linked,
        nodeType: "decision",
        hue: "coral",
        sub: date ? `decision · ${date}` : "decision",
        count: n.count,
        date: n.date,
        promotedFrom: promoted,
      };
    }
    case "source": {
      const meta = sourceMeta(n.source_type ?? n.label ?? "");
      const seed = ringSeed(n.id, CORE_R1, CORE_R1 + 80);
      return {
        label: meta.name,
        size: 8,
        color: hueOf(meta.hue),
        x: seed.x,
        y: seed.y,
        type: "bordered",
        borderColor: p.bg,
        zIndex: 5,
        degree,
        linked: true,
        nodeType: "source",
        hue: meta.hue,
        sub: "source · platform",
        sourceType: n.source_type ?? undefined,
        letter: meta.letter,
      };
    }
    case "person": {
      // Honest contract (backend atlas.rs): `label` is `null` when
      // author_name didn't resolve; `resolved` says so directly.
      const hue = PERSON_HUES[nextPersonIdx() % PERSON_HUES.length];
      const name = n.resolved && n.label ? n.label : null;
      const seed = ringSeed(n.id, CORE_R1 - 40, CORE_R1 + 60);
      return {
        label: name ?? "Unknown author",
        size: 6,
        color: hueOf(hue),
        x: seed.x,
        y: seed.y,
        type: "circle",
        zIndex: 5,
        degree,
        linked: true,
        nodeType: "person",
        hue,
        sub: "author",
        initials: name ? initialsOf(name) : "?",
      };
    }
    case "coral":
    default: {
      const parent = n.reef_id ?? null;
      const hue = (parent && reefHue.get(parent)) || "plum";
      const sub = (parent && reefLabel.get(parent)) || prettifySource(n.source_type ?? "");
      // Seed in a tight disc around the parent reef so FA2 keeps the cloud
      // attached; orphan corals seed on the outer band.
      const base = (parent ? reefSeed.get(parent) : undefined) ?? ringSeed(n.id, HALO_R0, HALO_R1);
      const a = hash01(n.id) * Math.PI * 2;
      const r = 18 + hash01b(n.id) * 22;
      return {
        label: n.label ?? "",
        size: 2.4 + hash01(n.id) * 1.2,
        color: hueOf(hue),
        x: base.x + Math.cos(a) * r,
        y: base.y + Math.sin(a) * r,
        type: "circle",
        zIndex: 1,
        degree,
        linked: true,
        nodeType: "coral",
        hue,
        sub,
        sourceType: n.source_type ?? undefined,
        reefId: parent,
      };
    }
  }
}

/** Flip a typed edge's review state in place (after a confirm) and restyle
 *  it so the canvas reflects the new status on the next refresh. No-op for
 *  unknown keys. */
export function applyEdgeStatus(
  graph: GraphModel,
  edgeKey: string,
  status: AtlasEdgeStatus,
  palette: Palette = DEFAULT_PALETTE,
): void {
  if (!graph.hasEdge(edgeKey)) return;
  const kind = graph.getEdgeAttribute(edgeKey, "kind");
  const origin = graph.getEdgeAttribute(edgeKey, "origin");
  const style = edgeStyle(kind, status, origin, palette);
  graph.mergeEdgeAttributes(edgeKey, { status, ...style });
}

/** Graph-space bounding box of the nodes the canvas is currently showing,
 *  with a little padding. Sigma's own bbox counts hidden nodes, so "Fit"
 *  with the halo switched off would still frame the empty ring; the canvas
 *  hands this to `sigma.setCustomBBox` instead. `null` when nothing shows. */
export function visibleBBox(
  graph: GraphModel,
  visible: Record<AtlasNodeType, boolean>,
  showUnlinked: boolean,
  pad = 0.06,
): { x: [number, number]; y: [number, number] } | null {
  let x0 = Infinity;
  let y0 = Infinity;
  let x1 = -Infinity;
  let y1 = -Infinity;
  graph.forEachNode((_, a) => {
    if (a.nodeType !== "ocean" && !visible[a.nodeType]) return;
    if (!showUnlinked && !a.linked && (a.nodeType === "reef" || a.nodeType === "decision")) return;
    if (a.x < x0) x0 = a.x;
    if (a.x > x1) x1 = a.x;
    if (a.y < y0) y0 = a.y;
    if (a.y > y1) y1 = a.y;
  });
  if (!Number.isFinite(x0)) return null;
  const px = Math.max(1, (x1 - x0) * pad);
  const py = Math.max(1, (y1 - y0) * pad);
  return { x: [x0 - px, x1 + px], y: [y0 - py, y1 + py] };
}

/* ── Queries the page uses ───────────────────────────────────────────────── */

export interface Connection {
  edgeKey: string;
  other: string;
  /** True when `node` is the edge's source (reads "node <verb> other"). */
  outgoing: boolean;
  attrs: GraphEdgeAttrs;
}

/** Every edge touching `node`, with the far endpoint and direction. */
export function connectionsOf(graph: GraphModel, node: string): Connection[] {
  const out: Connection[] = [];
  if (!graph.hasNode(node)) return out;
  graph.forEachEdge(node, (edgeKey, attrs, source, target) => {
    const outgoing = source === node;
    out.push({ edgeKey, other: outgoing ? target : source, outgoing, attrs });
  });
  return out;
}

/** Node ids adjacent to `node` (either direction) plus the node itself —
 *  the "lit" set the hover/focus reducers keep at full opacity. */
export function neighborhood(graph: GraphModel, node: string): Set<string> {
  const lit = new Set<string>([node]);
  if (!graph.hasNode(node)) return lit;
  graph.forEachNeighbor(node, (nb) => lit.add(nb));
  return lit;
}
