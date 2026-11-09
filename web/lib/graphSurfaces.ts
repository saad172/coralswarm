/* Pure read models for the knowledge-graph surfaces the web app renders:
   reef digests (#1357), Jev citation verdicts in scoped Ask (#1353), fact
   supersession (#1346) and typed relationship edges (#1339).

   Every parser here is lenient by design: these fields are additive on the
   wire, absent on older stored messages and on the MCP vs HTTP shapes, and
   (for supersession) absent in prod until FACT_SUPERSESSION_MODE=live. A
   missing or malformed field degrades to "not shown"; it never rejects the
   surrounding answer or reef. The one exception is the digest body, which is
   the whole response of its route, so a malformed one is a read failure. */

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const record = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const uuid = (v: unknown): v is string => typeof v === "string" && UUID.test(v);
const instant = (v: unknown): v is string => typeof v === "string" && Number.isFinite(Date.parse(v));

// ── Jev citation verdicts ───────────────────────────────────────────────────

/** `jev_verdict` on a cited passage or legacy chunk. `unverified` means the
 *  check did not run for that cite (fail-open), not that it failed. */
export type JevVerdict = "verified" | "unsupported" | "contradicted" | "fabricated" | "unverified";
export type VerdictTone = "ok" | "warn" | "bad" | "muted";

export const JEV_VERDICTS: readonly JevVerdict[] = ["verified", "unsupported", "contradicted", "fabricated", "unverified"];

/** Display contract for a verdict. The glyph and the word both carry the
 *  meaning, so nothing depends on the tone colour alone. */
export const JEV_VERDICT_META: Record<JevVerdict, { label: string; glyph: string; tone: VerdictTone; description: string }> = {
  verified: { label: "verified", glyph: "✓", tone: "ok", description: "Jev checked this source and it supports the claim it is cited for." },
  unsupported: { label: "not supported", glyph: "?", tone: "warn", description: "Jev checked this source and it does not support the claim it is cited for." },
  contradicted: { label: "contradicted", glyph: "≠", tone: "bad", description: "Jev checked this source and it contradicts the claim it is cited for." },
  fabricated: { label: "not in source", glyph: "!", tone: "bad", description: "Jev could not find the cited claim in this source." },
  unverified: { label: "not checked", glyph: "–", tone: "muted", description: "The citation check did not run for this source." },
};

export function parseJevVerdict(v: unknown): JevVerdict | null {
  return typeof v === "string" && (JEV_VERDICTS as readonly string[]).includes(v) ? (v as JevVerdict) : null;
}

/** Whether a verdict should be flagged inline on the `[n]` chip. Verified and
 *  unchecked cites stay quiet in the prose; their badge lives on the card. */
export const verdictNeedsAttention = (v: JevVerdict | null | undefined) =>
  v === "unsupported" || v === "contradicted" || v === "fabricated";

// ── Fact supersession ───────────────────────────────────────────────────────

/** `statement` is the superseding fact's text (first 280 characters). The
 *  server sends it only for a fact this reader may open; it is null on older
 *  payloads. */
export interface SupersededBy { semantic_atom_id: string; state_key: string | null; kind: string | null; rule: string | null; statement: string | null }
/** `supersession` on an outcome: present only on superseded facts. */
export interface FactSupersession { status: "superseded"; superseded_by: SupersededBy[] }

export function parseSupersession(v: unknown): FactSupersession | null {
  if (!record(v) || v.status !== "superseded") return null;
  const by = Array.isArray(v.superseded_by) ? v.superseded_by : [];
  const str = (s: unknown) => (typeof s === "string" && s ? s : null);
  return {
    status: "superseded",
    superseded_by: by.flatMap((b) => (record(b) && uuid(b.semantic_atom_id)
      ? [{ semantic_atom_id: b.semantic_atom_id, state_key: str(b.state_key), kind: str(b.kind), rule: str(b.rule), statement: typeof b.statement === "string" && b.statement.trim() ? b.statement : null }]
      : [])),
  };
}

/** Split into current facts first and superseded history after, each in the
 *  order the server returned them (stable, never re-ranked by recency). */
export function splitBySupersession<T extends { supersession?: unknown }>(items: readonly T[]): { current: T[]; superseded: T[] } {
  const current: T[] = [];
  const superseded: T[] = [];
  for (const item of items) (parseSupersession(item.supersession) ? superseded : current).push(item);
  return { current, superseded };
}

/** Human line for a superseded fact. The superseder is named by `resolve`
 *  (its statement when that fact is in the same list) or by the statement the
 *  server sent with it; otherwise the line stays generic rather than
 *  inventing what replaced it. */
export function supersessionLine(s: FactSupersession, resolve?: (id: string) => string | null | undefined): string {
  const named = s.superseded_by.map((b) => resolve?.(b.semantic_atom_id) || b.statement).filter((t): t is string => !!t && !!t.trim());
  if (named.length === 1) return `Superseded by a later fact: “${clip(named[0], 140)}”`;
  if (named.length > 1) return `Superseded by ${named.length} later facts`;
  if (s.superseded_by.length > 1) return `Superseded by ${s.superseded_by.length} later facts`;
  return "Superseded by a later fact";
}

function clip(text: string, max: number) {
  const t = text.trim().replace(/\s+/g, " ");
  return Array.from(t).length > max ? `${Array.from(t).slice(0, max - 1).join("")}…` : t;
}

// ── Reef digests ────────────────────────────────────────────────────────────

export interface DigestAttribution { model: string | null; generated_at: string | null; input_count: number | null }
export interface DigestExcerpt { atom_id: string; excerpt: string; contribution: string | null }
export type ReefDigestView =
  /** A digest this reader may see. `attribution` is null on a digest that
   *  predates attribution. */
  | { state: "served"; summary: string | null; excerpts: DigestExcerpt[]; attribution: DigestAttribution | null }
  /** Withheld or not generated yet. Never says why: that could reveal what
   *  the reader cannot see. */
  | { state: "unavailable"; regenerating: boolean }
  /** The reef has no readable corals to summarize. */
  | { state: "empty" };

/** Parse a `GET /v1/nodes/{id}/digest` body. Throws on a shape it cannot
 *  account for, so the panel shows an error instead of guessing. */
export function parseReefDigest(v: unknown): ReefDigestView {
  if (!record(v)) throw new Error("This topic summary could not be read.");
  if (v.unavailable === true) return { state: "unavailable", regenerating: v.regenerating === true };
  if (!(v.summary === null || v.summary === undefined || typeof v.summary === "string") || !(v.excerpts === undefined || Array.isArray(v.excerpts)))
    throw new Error("This topic summary could not be read.");
  const excerpts: DigestExcerpt[] = (Array.isArray(v.excerpts) ? v.excerpts : []).flatMap((e) =>
    record(e) && typeof e.atom_id === "string" && e.atom_id && typeof e.excerpt === "string" && e.excerpt.trim()
      ? [{ atom_id: e.atom_id, excerpt: e.excerpt, contribution: typeof e.contribution === "string" && e.contribution.trim() ? e.contribution : null }]
      : []);
  const summary = typeof v.summary === "string" && v.summary.trim() ? v.summary : null;
  if (!summary && excerpts.length === 0) return { state: "empty" };
  const a = v.attribution;
  const attribution: DigestAttribution | null = record(a)
    ? {
        model: typeof a.model === "string" && a.model.trim() ? a.model : null,
        generated_at: instant(a.generated_at) ? a.generated_at : null,
        input_count: Number.isSafeInteger(a.input_count) && Number(a.input_count) >= 0 ? Number(a.input_count) : null,
      }
    : null;
  return { state: "served", summary, excerpts, attribution };
}

/** "openai/gpt-x" → "gpt-x": the provider prefix is routing noise. */
export const modelLabel = (model: string) => model.split("/").pop() || model;

/** Mono attribution line: "from 12 items · gpt-x · 3 Oct 2026". */
export function digestAttributionLine(a: DigestAttribution | null, locale?: string): string | null {
  if (!a) return null;
  const parts: string[] = [];
  if (a.input_count != null) parts.push(`from ${a.input_count} item${a.input_count === 1 ? "" : "s"}`);
  if (a.model) parts.push(modelLabel(a.model));
  if (a.generated_at) parts.push(new Date(a.generated_at).toLocaleDateString(locale, { day: "numeric", month: "short", year: "numeric" }));
  return parts.length ? parts.join(" · ") : null;
}

// ── Relationship edges ──────────────────────────────────────────────────────

export interface EdgeEndpoint { node_id: string; title: string | null; type: string }
export interface GraphEdgeLike {
  edge_id: string;
  predicate: string;
  src: EdgeEndpoint;
  dst: EdgeEndpoint;
  valid_from?: string | null;
  valid_until?: string | null;
}
export interface PredicateGroup<E extends GraphEdgeLike> {
  /** Stable key: predicate + direction. */
  key: string;
  predicate: string;
  /** `out`: this reef is the subject. `in`: this reef is the object. */
  direction: "out" | "in";
  /** Humanized predicate ("parent of"). Direction is rendered separately so
   *  no passive form has to be invented for a closed predicate vocabulary. */
  label: string;
  edges: { edge: E; other: EdgeEndpoint }[];
}

export const humanPredicate = (p: string) => p.replace(/[_-]+/g, " ").trim().toLowerCase();

/** Group edges touching `nodeId` by predicate and direction, preserving the
 *  server's order within each group and ordering groups by first appearance.
 *  A self-loop counts as outgoing. */
export function groupEdgesByPredicate<E extends GraphEdgeLike>(edges: readonly E[], nodeId: string): PredicateGroup<E>[] {
  const self = nodeId.toLowerCase();
  const groups = new Map<string, PredicateGroup<E>>();
  for (const edge of edges) {
    const out = edge.src.node_id.toLowerCase() === self;
    const direction = out ? "out" : "in";
    const key = `${direction}:${edge.predicate}`;
    let group = groups.get(key);
    if (!group) {
      const p = humanPredicate(edge.predicate);
      group = { key, predicate: edge.predicate, direction, label: p, edges: [] };
      groups.set(key, group);
    }
    group.edges.push({ edge, other: out ? edge.dst : edge.src });
  }
  return Array.from(groups.values());
}

/** "since 3 Oct 2026", "until …", "3 Oct 2026 – 1 Jan 2027", or null when the
 *  edge states no validity (v1 edges and undated v2 edges). */
export function validityLabel(from: string | null | undefined, until: string | null | undefined, locale?: string): string | null {
  // Stated validity is a calendar date anchored at UTC midnight by extraction;
  // rendering it in the reader's zone would shift it a day west of UTC.
  const fmt = (s: string) => new Date(s).toLocaleDateString(locale, { day: "numeric", month: "short", year: "numeric", timeZone: "UTC" });
  const f = instant(from) ? fmt(from) : null;
  const u = instant(until) ? fmt(until) : null;
  if (f && u) return `${f} – ${u}`;
  if (f) return `since ${f}`;
  if (u) return `until ${u}`;
  return null;
}

/** An edge whose validity has ended is history, not current state. */
export const edgeEnded = (until: string | null | undefined, now = Date.now()) => instant(until) && Date.parse(until) < now;
