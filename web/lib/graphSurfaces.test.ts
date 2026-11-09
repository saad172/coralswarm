import { describe, expect, it } from "vitest";
import {
  digestAttributionLine,
  edgeEnded,
  groupEdgesByPredicate,
  parseJevVerdict,
  parseReefDigest,
  parseSupersession,
  splitBySupersession,
  supersessionLine,
  validityLabel,
  verdictNeedsAttention,
} from "./graphSurfaces";

const A = "11111111-1111-4111-8111-111111111111";
const B = "22222222-2222-4222-8222-222222222222";
const C = "33333333-3333-4333-8333-333333333333";

describe("Jev verdicts", () => {
  it("accepts only the documented verdicts and ignores anything else", () => {
    for (const v of ["verified", "unsupported", "contradicted", "fabricated", "unverified"]) expect(parseJevVerdict(v)).toBe(v);
    expect(parseJevVerdict("VERIFIED")).toBeNull();
    expect(parseJevVerdict(undefined)).toBeNull();
    expect(parseJevVerdict(1)).toBeNull();
  });
  it("flags only verdicts that undermine the claim inline", () => {
    expect(verdictNeedsAttention("verified")).toBe(false);
    expect(verdictNeedsAttention("unverified")).toBe(false);
    expect(verdictNeedsAttention("unsupported")).toBe(true);
    expect(verdictNeedsAttention("contradicted")).toBe(true);
    expect(verdictNeedsAttention("fabricated")).toBe(true);
  });
});

describe("fact supersession", () => {
  const superseded = { status: "superseded", superseded_by: [{ semantic_atom_id: B, state_key: "owner", kind: "fact", rule: "functional" }] };
  it("parses only superseded markers and drops malformed superseders", () => {
    expect(parseSupersession(superseded)?.superseded_by).toHaveLength(1);
    expect(parseSupersession({ status: "current" })).toBeNull();
    expect(parseSupersession(undefined)).toBeNull();
    expect(parseSupersession({ status: "superseded", superseded_by: [{ semantic_atom_id: "nope" }] })).toEqual({ status: "superseded", superseded_by: [] });
  });
  it("puts current facts first and keeps server order within each half", () => {
    const items = [{ id: 1, supersession: superseded }, { id: 2 }, { id: 3, supersession: superseded }, { id: 4 }];
    const { current, superseded: history } = splitBySupersession(items);
    expect(current.map((i) => i.id)).toEqual([2, 4]);
    expect(history.map((i) => i.id)).toEqual([1, 3]);
  });
  it("is a no-op when no item carries supersession (dry_run mode today)", () => {
    const items: { id: number; supersession?: unknown }[] = [{ id: 1 }, { id: 2 }];
    expect(splitBySupersession(items)).toEqual({ current: items, superseded: [] });
  });
  it("names the superseding fact only when it is on hand", () => {
    const s = parseSupersession(superseded);
    if (!s) throw new Error("expected a supersession");
    expect(supersessionLine(s)).toBe("Superseded by a later fact");
    expect(supersessionLine(s, (id) => (id === B ? "Dana owns Atlas now." : null))).toBe("Superseded by a later fact: “Dana owns Atlas now.”");
    const two = parseSupersession({ status: "superseded", superseded_by: [{ semantic_atom_id: B }, { semantic_atom_id: C }] });
    if (!two) throw new Error("expected a supersession");
    expect(supersessionLine(two)).toBe("Superseded by 2 later facts");
  });
  it("names the superseding fact from the statement the server sent", () => {
    const s = parseSupersession({ status: "superseded", superseded_by: [{ semantic_atom_id: B, statement: "Dana owns Atlas now." }] });
    if (!s) throw new Error("expected a supersession");
    expect(s.superseded_by[0].statement).toBe("Dana owns Atlas now.");
    expect(supersessionLine(s)).toBe("Superseded by a later fact: “Dana owns Atlas now.”");
    const blank = parseSupersession({ status: "superseded", superseded_by: [{ semantic_atom_id: B, statement: "  " }] });
    if (!blank) throw new Error("expected a supersession");
    expect(blank.superseded_by[0].statement).toBeNull();
    expect(supersessionLine(blank)).toBe("Superseded by a later fact");
  });
});

describe("topic digests", () => {
  it("reads a served, attributed digest", () => {
    const view = parseReefDigest({
      summary: "Atlas is the onboarding rewrite.", cached: true,
      excerpts: [{ atom_id: A, excerpt: "We agreed Atlas ships in Q4.", contribution: "Sets the date" }, { atom_id: B, excerpt: "  " }],
      attribution: { receipt_id: C, model: "deepseek/deepseek-v4-flash", generated_at: "2026-10-03T12:00:00Z", input_count: 12, purpose: "managed_enrichment" },
    });
    expect(view.state).toBe("served");
    if (view.state !== "served") return;
    expect(view.excerpts).toEqual([{ atom_id: A, excerpt: "We agreed Atlas ships in Q4.", contribution: "Sets the date" }]);
    expect(digestAttributionLine(view.attribution, "en-US")).toBe("from 12 items · deepseek-v4-flash · Oct 3, 2026");
  });
  it("tolerates the unattributed local-store shape", () => {
    const view = parseReefDigest({ summary: "Local summary", excerpts: [] });
    expect(view).toEqual({ state: "served", summary: "Local summary", excerpts: [], attribution: null });
    expect(digestAttributionLine(null)).toBeNull();
  });
  it("distinguishes unavailable (with or without a queued regeneration) from empty", () => {
    expect(parseReefDigest({ summary: null, excerpts: [], unavailable: true, regenerating: true })).toEqual({ state: "unavailable", regenerating: true });
    expect(parseReefDigest({ summary: null, excerpts: [], unavailable: true })).toEqual({ state: "unavailable", regenerating: false });
    expect(parseReefDigest({ summary: null, excerpts: [] })).toEqual({ state: "empty" });
  });
  it("rejects a body it cannot account for", () => {
    expect(() => parseReefDigest(null)).toThrow();
    expect(() => parseReefDigest({ summary: 3 })).toThrow();
    expect(() => parseReefDigest({ summary: "x", excerpts: "nope" })).toThrow();
  });
});

describe("relationship edges", () => {
  const edge = (id: string, predicate: string, src: string, dst: string, extra: object = {}) => ({
    edge_id: id, predicate, src: { node_id: src, title: `t-${src.slice(0, 1)}`, type: "entity" }, dst: { node_id: dst, title: `t-${dst.slice(0, 1)}`, type: "entity" }, ...extra,
  });
  it("groups by predicate and direction relative to the topic, in first-seen order", () => {
    const groups = groupEdgesByPredicate([edge("e1", "works_on", A, B), edge("e2", "manages", C, A), edge("e3", "works_on", A, C), edge("e4", "works_on", B, A)], A);
    expect(groups.map((g) => [g.key, g.label, g.edges.map((e) => e.other.node_id)])).toEqual([
      ["out:works_on", "works on", [B, C]],
      ["in:manages", "manages", [C]],
      ["in:works_on", "works on", [B]],
    ]);
  });
  it("matches the topic id case-insensitively", () => {
    expect(groupEdgesByPredicate([edge("e1", "x", A.toUpperCase(), B)], A)[0].direction).toBe("out");
  });
  it("labels stated validity and hides it when absent", () => {
    expect(validityLabel("2026-01-05T00:00:00Z", "2026-03-01T00:00:00Z", "en-US")).toBe("Jan 5, 2026 – Mar 1, 2026");
    expect(validityLabel("2026-01-05T00:00:00Z", null, "en-US")).toBe("since Jan 5, 2026");
    expect(validityLabel(undefined, "2026-03-01T00:00:00Z", "en-US")).toBe("until Mar 1, 2026");
    expect(validityLabel(undefined, undefined)).toBeNull();
    expect(validityLabel("garbage", null)).toBeNull();
  });
  it("treats only a past valid_until as ended", () => {
    const now = Date.parse("2026-06-01T00:00:00Z");
    expect(edgeEnded("2026-03-01T00:00:00Z", now)).toBe(true);
    expect(edgeEnded("2026-09-01T00:00:00Z", now)).toBe(false);
    expect(edgeEnded(null, now)).toBe(false);
  });
});
