import { describe, expect, it } from "vitest";
import type { AtlasGraph } from "@/lib/api";
import {
  ANCHOR_WEIGHT,
  applyEdgeStatus,
  buildGraphModel,
  connectionsOf,
  COOCCUR_WEIGHT_CAP,
  DEFAULT_PALETTE,
  fade,
  HALO_INNER,
  HALO_OUTER,
  hash01,
  initialsOf,
  LABEL_MAX,
  neighborhood,
  placeHalo,
  reefSize,
  shortLabel,
  sourceMeta,
  visibleBBox,
} from "./toGraphology";

const R1 = "11111111-1111-4111-8111-111111111111";
const R2 = "22222222-2222-4222-8222-222222222222";
const D1 = "33333333-3333-4333-8333-333333333333";
const C1 = "44444444-4444-4444-8444-444444444444";
const E_CONFIRMED = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const E_SUGGESTED = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const OTHER_OCEAN = "99999999-9999-4999-8999-999999999999";

function atlas(): AtlasGraph {
  return {
    ocean: { id: "o", name: "Personal", kind: "personal", is_primary: true },
    nodes: [
      { id: "ocean", type: "ocean", label: "Personal", kind: "personal", is_primary: true },
      { id: R1, type: "reef", reef_kind: "entity", label: "Harper", count: 25, promoted_from_graph_id: null },
      { id: R2, type: "reef", reef_kind: "thread", label: "Auth migration", count: 0, promoted_from_graph_id: OTHER_OCEAN },
      { id: D1, type: "decision", label: "Use JWT", count: 3, date: "2026-07-08T12:00:00Z", promoted_from_graph_id: null },
      { id: "src:slack", type: "source", source_type: "slack", label: "slack" },
      { id: "person:U1", type: "person", label: "Sara Khan", resolved: true },
      { id: "person:U2", type: "person", label: null, resolved: false },
      { id: C1, type: "coral", label: "we agreed on jwt", source_type: "slack", src_id: "src:slack", reef_id: R1, person_id: "person:U1" },
    ],
    links: [
      { a: "ocean", b: R1, kind: "ocean-reef" },
      { a: "ocean", b: R2, kind: "ocean-reef" },
      { a: "ocean", b: D1, kind: "ocean-decision" },
      { a: R1, b: C1, kind: "reef-coral" },
      { a: C1, b: "src:slack", kind: "coral-source" },
      { a: C1, b: "person:U1", kind: "authored" },
      { a: "ocean", b: "person:U1", kind: "member" },
      { a: R1, b: R2, kind: "reef-reef", predicate: "works_on", edge_id: E_CONFIRMED, status: "confirmed", origin: "stated", confidence: 0.9 },
      { a: R1, b: D1, kind: "decision-reef", predicate: "responsible_for", edge_id: E_SUGGESTED, status: "suggested", origin: "inferred", confidence: 0.4 },
      // A link to a node the payload never declared must be dropped, not thrown.
      { a: R1, b: "ghost", kind: "reef-reef", predicate: "knows" },
    ],
    truncated: { atoms: true },
  };
}

describe("buildGraphModel", () => {
  it("adds every node once with its semantic type and counts them", () => {
    const b = buildGraphModel(atlas());
    expect(b.graph.order).toBe(8);
    expect(b.counts).toEqual({ ocean: 1, reef: 2, decision: 1, coral: 1, source: 1, person: 2, total: 8, unlinked: 0 });
    expect(b.truncated).toBe(true);
    expect(b.name).toBe("Personal");
  });

  it("splits core from halo by structural degree — hub spokes don't count", () => {
    const a = atlas();
    // A reef and a decision that only hang off the ocean, like most of a real ocean.
    a.nodes.push({ id: "r-lone", type: "reef", reef_kind: "entity", label: "Lonely", count: 40 });
    a.nodes.push({ id: "d-lone", type: "decision", label: "Decided alone", count: 1, date: "2026-07-08T12:00:00Z" });
    a.links.push({ a: "ocean", b: "r-lone", kind: "ocean-reef" }, { a: "ocean", b: "d-lone", kind: "ocean-decision" });
    const { graph, counts } = buildGraphModel(a);
    expect(counts.unlinked).toBe(2);
    expect(graph.getNodeAttributes("r-lone")).toMatchObject({ linked: false, degree: 0 });
    expect(graph.getNodeAttributes(R1)).toMatchObject({ linked: true, degree: 3 }); // coral + 2 currents
    // Halo nodes: small, faded (8-digit hex), unlabelled by size, seeded outside the core.
    expect(graph.getNodeAttribute("r-lone", "size")).toBeLessThanOrEqual(5);
    expect(graph.getNodeAttribute("r-lone", "color")).toMatch(/^#[0-9a-f]{8}$/i);
    expect(graph.getNodeAttribute(R1, "color")).toBe(DEFAULT_PALETTE.pink);
    const radius = (id: string) => Math.hypot(graph.getNodeAttribute(id, "x"), graph.getNodeAttribute(id, "y"));
    expect(radius("r-lone")).toBeGreaterThan(radius(R1));
    expect(radius("d-lone")).toBeGreaterThan(radius(D1));
    // 40 corals don't buy an unlinked reef a landmark size; connections do.
    expect(graph.getNodeAttribute("r-lone", "size")).toBeLessThan(graph.getNodeAttribute(R1, "size"));
  });

  it("keeps hub spokes for the panels but hides and unweights them for the canvas", () => {
    const { graph } = buildGraphModel(atlas());
    expect(graph.getEdgeAttributes(`ocean-reef:ocean:${R1}`)).toMatchObject({ hidden: true, weight: 0 });
    expect(graph.getEdgeAttributes(`member:ocean:person:U1`)).toMatchObject({ hidden: true, weight: 0 });
    expect(graph.getEdgeAttribute(E_CONFIRMED, "weight")).toBe(1);
    expect(graph.getEdgeAttribute(E_CONFIRMED, "hidden")).toBeUndefined();
    expect(graph.getEdgeAttribute(`reef-coral:${R1}:${C1}`, "weight")).toBeLessThan(1);
  });

  it("maps hues by the design-system taxonomy", () => {
    const { graph } = buildGraphModel(atlas());
    expect(graph.getNodeAttribute("ocean", "color")).toBe(DEFAULT_PALETTE.coral); // personal ocean
    expect(graph.getNodeAttribute(R1, "hue")).toBe("pink"); // entity reef
    expect(graph.getNodeAttribute(R2, "hue")).toBe("amber"); // thread reef
    expect(graph.getNodeAttribute(D1, "hue")).toBe("coral"); // decision
    expect(graph.getNodeAttribute("src:slack", "hue")).toBe("amber");
    // A coral inherits its parent reef's hue and takes the reef's title as sublabel.
    expect(graph.getNodeAttribute(C1, "hue")).toBe("pink");
    expect(graph.getNodeAttribute(C1, "sub")).toBe("Harper");
  });

  it("pins the ocean hub at the origin and seeds everything else deterministically", () => {
    const a = buildGraphModel(atlas());
    const b = buildGraphModel(atlas());
    expect(a.graph.getNodeAttributes("ocean")).toMatchObject({ x: 0, y: 0, fixed: true });
    for (const id of [R1, R2, D1, C1, "src:slack", "person:U1"]) {
      expect(a.graph.getNodeAttribute(id, "x")).toBe(b.graph.getNodeAttribute(id, "x"));
      expect(a.graph.getNodeAttribute(id, "y")).toBe(b.graph.getNodeAttribute(id, "y"));
      expect(a.graph.getNodeAttribute(id, "x") !== 0 || a.graph.getNodeAttribute(id, "y") !== 0).toBe(true);
    }
  });

  it("marks promoted reefs with a bordered program and remembers the source ocean", () => {
    const { graph } = buildGraphModel(atlas());
    expect(graph.getNodeAttributes(R2)).toMatchObject({ type: "bordered", promotedFrom: OTHER_OCEAN });
    expect(graph.getNodeAttribute(R2, "borderColor")).toBe(DEFAULT_PALETTE.ink2);
    expect(graph.getNodeAttributes(R1)).toMatchObject({ type: "circle", promotedFrom: null });
    expect(graph.getNodeAttribute(R1, "borderColor")).toBeUndefined();
  });

  it("renders unresolved authors as 'Unknown author', never the raw id", () => {
    const { graph } = buildGraphModel(atlas());
    expect(graph.getNodeAttribute("person:U2", "label")).toBe("Unknown author");
    expect(graph.getNodeAttribute("person:U2", "initials")).toBe("?");
    expect(graph.getNodeAttribute("person:U1", "initials")).toBe("SK");
  });

  it("keys typed edges by edge_id and carries their review state", () => {
    const { graph } = buildGraphModel(atlas());
    expect(graph.hasEdge(E_CONFIRMED)).toBe(true);
    expect(graph.getEdgeAttributes(E_CONFIRMED)).toMatchObject({
      kind: "reef-reef",
      type: "curve",
      predicate: "works_on",
      status: "confirmed",
      origin: "stated",
      confidence: 0.9,
      color: fade(DEFAULT_PALETTE.ink3, 0.45),
      attribution: false,
    });
    expect(graph.getEdgeAttributes(E_SUGGESTED)).toMatchObject({
      kind: "decision-reef",
      status: "suggested",
      color: DEFAULT_PALETTE.amber,
    });
    // Suggested draws thinner than confirmed — the tentative/fact distinction.
    expect(graph.getEdgeAttribute(E_SUGGESTED, "size")).toBeLessThan(graph.getEdgeAttribute(E_CONFIRMED, "size"));
  });

  it("flags attribution edges and keys structural edges by kind+endpoints", () => {
    const { graph } = buildGraphModel(atlas());
    expect(graph.getEdgeAttribute(`authored:${C1}:person:U1`, "attribution")).toBe(true);
    expect(graph.getEdgeAttribute(`coral-source:${C1}:src:slack`, "attribution")).toBe(true);
    expect(graph.getEdgeAttribute(`ocean-reef:ocean:${R1}`, "attribution")).toBe(false);
    expect(graph.getEdgeAttribute(`reef-coral:${R1}:${C1}`, "type")).toBe("line");
  });

  it("drops links whose endpoints are not in the payload", () => {
    const { graph } = buildGraphModel(atlas());
    expect(graph.hasNode("ghost")).toBe(false);
    expect(graph.size).toBe(9);
  });

  it("tolerates a repeated wire link without throwing", () => {
    const a = atlas();
    a.links.push({ a: "ocean", b: R1, kind: "ocean-reef" });
    expect(() => buildGraphModel(a)).not.toThrow();
    expect(buildGraphModel(a).graph.size).toBe(9);
  });
});

describe("placeHalo / visibleBBox", () => {
  const ALL = { ocean: true, reef: true, decision: true, coral: true, source: true, person: true };
  function withHalo() {
    const a = atlas();
    for (let i = 0; i < 20; i++) {
      a.nodes.push({ id: `lone${i}`, type: "reef", reef_kind: "entity", label: `Lone ${i}`, count: i % 2 });
      a.links.push({ a: "ocean", b: `lone${i}`, kind: "ocean-reef" });
    }
    return buildGraphModel(a);
  }

  it("rings unlinked nodes just outside the measured core, deterministically", () => {
    const { graph } = withHalo();
    const core = placeHalo(graph);
    expect(core).toBeGreaterThan(0);
    for (let i = 0; i < 20; i++) {
      const r = Math.hypot(graph.getNodeAttribute(`lone${i}`, "x"), graph.getNodeAttribute(`lone${i}`, "y"));
      expect(r).toBeGreaterThanOrEqual(core * HALO_INNER - 1e-6);
      expect(r).toBeLessThanOrEqual(core * HALO_OUTER + 1e-6);
    }
    const again = withHalo().graph;
    placeHalo(again);
    expect(again.getNodeAttribute("lone3", "x")).toBe(graph.getNodeAttribute("lone3", "x"));
    // Halo nodes are pinned for the layout worker; core nodes are not.
    expect(graph.getNodeAttribute("lone3", "fixed")).toBe(true);
    expect(graph.getNodeAttribute(R1, "fixed")).toBeUndefined();
  });

  it("visibleBBox shrinks to the core when the halo is switched off", () => {
    const { graph } = withHalo();
    placeHalo(graph);
    const all = visibleBBox(graph, ALL, true);
    const core = visibleBBox(graph, ALL, false);
    if (!all || !core) throw new Error("expected boxes");
    expect(core.x[1] - core.x[0]).toBeLessThan(all.x[1] - all.x[0]);
    expect(core.y[1] - core.y[0]).toBeLessThan(all.y[1] - all.y[0]);
    expect(visibleBBox(graph, { ...ALL, reef: false, decision: false, coral: false, source: false, person: false }, false)).toEqual({
      x: [-1, 1],
      y: [-1, 1],
    });
  });
});

describe("3.2 derived layers / you-hub", () => {
  const SELF = "55555555-5555-4555-8555-555555555555";

  it("keeps owner spokes out of degree and layout weight", () => {
    const a = atlas();
    a.nodes.push({ id: SELF, type: "reef", reef_kind: "entity", label: "Alex", count: 100, is_self: true });
    a.links.push({ a: SELF, b: R1, kind: "reef-reef", predicate: "works_on", owner: true, origin: "stated", status: "confirmed" });
    const { graph } = buildGraphModel(a);
    expect(graph.getNodeAttributes(SELF)).toMatchObject({ isSelf: true, label: "you" });
    expect(graph.getNodeAttribute(R1, "degree")).toBe(3); // coral + 2 currents, not the owner spoke
    const ownerEdge = [...graph.edgeEntries()].find((e) => e.attributes.owner);
    expect(ownerEdge?.attributes).toMatchObject({ hidden: true, weight: 0 });
  });

  it("merges a stated current and a derived pair onto one edge", () => {
    const a = atlas();
    a.links.push({ a: R1, b: R2, kind: "co-occurs", origin: "derived", weight: 4 });
    const { graph } = buildGraphModel(a);
    expect(graph.hasEdge(E_CONFIRMED)).toBe(true);
    expect(graph.getEdgeAttribute(E_CONFIRMED, "origin")).toBe("stated");
    expect(graph.getEdgeAttribute(E_CONFIRMED, "shared")).toBe(4);
    expect(graph.getEdgeAttribute(E_CONFIRMED, "weight")).toBeGreaterThanOrEqual(1);
    expect([...graph.edgeEntries()].filter((e) => e.source === R1 && e.target === R2).length).toBe(1);
  });

  it("draws derived co-occurrence thinner, caps weight, and anchors at 0.1", () => {
    const a = atlas();
    const r3 = "66666666-6666-4666-8666-666666666666";
    a.nodes.push({ id: r3, type: "reef", reef_kind: "entity", label: "Postgres", count: 8 });
    a.links.push(
      { a: R1, b: r3, kind: "co-occurs", origin: "derived", weight: 40 },
      { a: D1, b: r3, kind: "decision-reef", origin: "derived", weight: 2, anchor_rank: 1 },
    );
    const { graph } = buildGraphModel(a);
    const co = [...graph.edgeEntries()].find((e) => e.attributes.kind === "co-occurs");
    const anchor = [...graph.edgeEntries()].find((e) => e.attributes.origin === "derived" && e.attributes.kind === "decision-reef");
    expect(co?.attributes.weight).toBe(COOCCUR_WEIGHT_CAP);
    expect(co?.attributes.size).toBeLessThan(graph.getEdgeAttribute(E_CONFIRMED, "size"));
    expect(anchor?.attributes.weight).toBe(ANCHOR_WEIGHT);
    expect(anchor?.attributes.size).toBeLessThan(graph.getEdgeAttribute(E_CONFIRMED, "size"));
  });
});

describe("applyEdgeStatus", () => {
  it("restyles a suggested edge as confirmed in place", () => {
    const { graph } = buildGraphModel(atlas());
    applyEdgeStatus(graph, E_SUGGESTED, "confirmed");
    expect(graph.getEdgeAttributes(E_SUGGESTED)).toMatchObject({
      status: "confirmed",
      color: fade(DEFAULT_PALETTE.ink3, 0.45),
      weight: 1,
      kind: "decision-reef",
      predicate: "responsible_for",
    });
    expect(graph.getEdgeAttribute(E_SUGGESTED, "size")).toBe(graph.getEdgeAttribute(E_CONFIRMED, "size"));
  });
  it("ignores unknown keys", () => {
    const { graph } = buildGraphModel(atlas());
    expect(() => applyEdgeStatus(graph, "nope", "confirmed")).not.toThrow();
  });
});

describe("connectionsOf / neighborhood", () => {
  it("lists both directions with the far endpoint and orientation", () => {
    const { graph } = buildGraphModel(atlas());
    const conns = connectionsOf(graph, R1);
    const byOther = Object.fromEntries(conns.map((c) => [c.other, c]));
    expect(byOther[R2]).toMatchObject({ outgoing: true, edgeKey: E_CONFIRMED });
    expect(byOther["ocean"]).toMatchObject({ outgoing: false });
    expect(byOther[C1]).toMatchObject({ outgoing: true });
    expect(neighborhood(graph, R1)).toEqual(new Set([R1, "ocean", R2, D1, C1]));
  });

  it("is empty for an unknown node", () => {
    const { graph } = buildGraphModel(atlas());
    expect(connectionsOf(graph, "nope")).toEqual([]);
    expect(neighborhood(graph, "nope")).toEqual(new Set(["nope"]));
  });
});

describe("helpers", () => {
  it("sizes reefs by connections first, corals second, within bounds", () => {
    // Unlinked: a dot, whatever it holds.
    expect(reefSize(0)).toBeCloseTo(3.2, 5);
    expect(reefSize(10_000)).toBe(5);
    // Linked: grows with sqrt(degree), nudged by corals, capped.
    expect(reefSize(0, 1)).toBeCloseTo(5.5, 1);
    expect(reefSize(0, 16)).toBeLessThan(reefSize(0, 100));
    expect(reefSize(25, 4)).toBeGreaterThan(reefSize(0, 4));
    expect(reefSize(10_000, 10_000)).toBe(18);
  });
  it("shortLabel clips at a word boundary with an ellipsis", () => {
    expect(shortLabel("Harper")).toBe("Harper");
    const long = "Keep existing better-sqlite3/SQLite architecture rather than migrating to Postgres.";
    const s = shortLabel(long);
    expect(s.length).toBeLessThanOrEqual(LABEL_MAX);
    expect(s.endsWith("…")).toBe(true);
    // The last space is too early (< 60% in) → hard clip rather than a stub.
    expect(s).toBe("Keep existing better-sqlite3/SQLi…");
    expect(shortLabel("Auth migration for the Keller ISD platform")).toBe("Auth migration for the Keller…");
    expect(shortLabel("x".repeat(60), 10)).toBe("xxxxxxxxx…");
  });
  it("fade multiplies alpha (so an already-faded colour dims further) and passes unknown formats through", () => {
    expect(fade("#eb7290", 0.5)).toBe("#eb729080");
    expect(fade("#abc", 1)).toBe("#aabbccff");
    expect(fade("#eb729080", 0.5)).toBe("#eb729040");
    expect(fade("rgb(1, 2, 3)", 0.25)).toBe("rgba(1,2,3,0.25)");
    expect(fade("rgba(1, 2, 3, 0.5)", 0.5)).toBe("rgba(1,2,3,0.25)");
    expect(fade("tomato", 0.5)).toBe("tomato");
  });
  it("hash01 is stable and in [0, 1)", () => {
    expect(hash01("abc")).toBe(hash01("abc"));
    expect(hash01("abc")).not.toBe(hash01("abd"));
    expect(hash01("abc")).toBeGreaterThanOrEqual(0);
    expect(hash01("abc")).toBeLessThan(1);
  });
  it("sourceMeta falls back to a prettified name and first letter", () => {
    expect(sourceMeta("slack").name).toBe("Slack");
    expect(sourceMeta("coding_session")).toMatchObject({ letter: "C", name: "Coding Session", hue: "plum" });
  });
  it("initialsOf handles one and many words", () => {
    expect(initialsOf("Sara")).toBe("SA");
    expect(initialsOf("Sara Al Khan")).toBe("SK");
    expect(initialsOf("  ")).toBe("?");
  });
});
