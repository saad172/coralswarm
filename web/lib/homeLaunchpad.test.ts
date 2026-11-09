import { describe, expect, it } from "vitest";
import type { AttentionResponse, ContinueItem, WhatsNewItem } from "./homeLaunchpadApi";
import {
  attentionMeta,
  continueHref,
  continueMeta,
  firstNameOf,
  freshnessLabel,
  homeState,
  isNewSince,
  newSinceLabel,
  nextLastVisit,
  promptStarters,
  readLastVisit,
  readOnboarded,
  reefHref,
  reefHue,
  setupChecklist,
  updatedAt,
  whatsNewOceanHue,
  writeLastVisit,
  writeOnboarded,
} from "./homeLaunchpad";

const reef = (over: Partial<WhatsNewItem> = {}): WhatsNewItem => ({
  reef_id: "r1",
  kind: "decision",
  title: "Usage-based billing starts at each Q1 renewal",
  ocean: { graph_id: "g1", name: "Go-to-market", kind: "team" },
  formed_at: "2026-11-03T09:00:00Z",
  ...over,
});

const item = (over: Partial<ContinueItem> = {}): ContinueItem => ({
  kind: "chat",
  id: "c1",
  title: "Why is the read model serving stale reefs?",
  context: "Platform",
  count: null,
  graph_id: null,
  at: "2026-11-03T12:00:00Z",
  ...over,
});

const attention = (over: Partial<AttentionResponse> = {}): AttentionResponse => ({
  has_content: true,
  has_source: true,
  has_mcp: false,
  has_asked: false,
  attention: null,
  generated_at: "2026-11-03T12:00:00Z",
  ...over,
});

function memoryStorage() {
  const map = new Map<string, string>();
  return { getItem: (k: string) => map.get(k) ?? null, setItem: (k: string, v: string) => void map.set(k, v), map };
}

describe("homeState", () => {
  const ready = { attentionFailed: false, memoryLoaded: true, onboarded: false };
  it("is loading until the attention read answers", () => {
    expect(homeState({ ...ready, attention: undefined })).toBe("loading");
  });
  it("is loading, not State B, until the browser memory has been read", () => {
    expect(homeState({ ...ready, attention: attention({ has_content: false }), memoryLoaded: false })).toBe("loading");
    expect(homeState({ ...ready, attention: undefined, attentionFailed: true, memoryLoaded: false })).toBe("loading");
  });
  it("shows State B while the ocean has no content", () => {
    expect(homeState({ ...ready, attention: attention({ has_content: false }) })).toBe("empty");
  });
  it("shows State A once there is content", () => {
    expect(homeState({ ...ready, attention: attention({ has_content: true }) })).toBe("populated");
  });
  it("never returns to the checklist once this identity has had content", () => {
    expect(homeState({ ...ready, attention: attention({ has_content: false }), onboarded: true })).toBe("populated");
    expect(homeState({ ...ready, attention: undefined, onboarded: true })).toBe("populated");
  });
  it("shows State A, not setup, when the attention read failed with nothing cached", () => {
    expect(homeState({ ...ready, attention: undefined, attentionFailed: true })).toBe("populated");
  });
});

describe("the new dot", () => {
  const last = "2026-11-02T12:00:00Z";
  it("marks a reef formed after the last visit", () => {
    expect(isNewSince("2026-11-02T12:00:01Z", last)).toBe(true);
  });
  it("does not mark a reef formed at or before it", () => {
    expect(isNewSince(last, last)).toBe(false);
    expect(isNewSince("2026-11-01T00:00:00Z", last)).toBe(false);
  });
  it("marks nothing on a first visit or an unreadable time", () => {
    expect(isNewSince("2026-11-03T00:00:00Z", null)).toBe(false);
    expect(isNewSince("not a date", last)).toBe(false);
    expect(isNewSince("2026-11-03T00:00:00Z", "garbage")).toBe(false);
  });
  it("counts the new rows and phrases the header", () => {
    const items = [reef({ formed_at: "2026-11-03T00:00:00Z" }), reef({ reef_id: "r2", formed_at: "2026-11-02T13:00:00Z" }), reef({ reef_id: "r3", formed_at: "2026-10-30T00:00:00Z" })];
    expect(newSinceLabel(items, last)).toBe("2 new since your last visit");
    expect(newSinceLabel(items, "2026-11-04T00:00:00Z")).toBe("0 new since your last visit");
    expect(newSinceLabel(items, null)).toBeNull();
  });
  it("stores the server's generated_at as the next baseline, else the client's now", () => {
    const now = new Date("2026-11-03T15:00:00Z");
    expect(nextLastVisit(null, "2026-11-03T14:58:00Z", now)).toBe("2026-11-03T14:58:00Z");
    expect(nextLastVisit(null, undefined, now)).toBe(now.toISOString());
    expect(nextLastVisit(null, "nope", now)).toBe(now.toISOString());
  });
  it("never moves the baseline backwards", () => {
    const now = new Date("2026-11-03T15:00:00Z");
    // A cached answer older than the stored visit (or another tab's newer one).
    expect(nextLastVisit("2026-11-03T14:59:00Z", "2026-11-03T14:50:00Z", now)).toBe("2026-11-03T14:59:00Z");
    expect(nextLastVisit("2026-11-03T14:00:00Z", "2026-11-03T14:50:00Z", now)).toBe("2026-11-03T14:50:00Z");
    expect(nextLastVisit("2026-11-03T14:50:00Z", "2026-11-03T14:50:00Z", now)).toBe("2026-11-03T14:50:00Z");
    // An unreadable stored value does not pin it.
    expect(nextLastVisit("corrupt", "2026-11-03T14:50:00Z", now)).toBe("2026-11-03T14:50:00Z");
  });
  it("remembers the last visit and the onboarded flag per identity", () => {
    const storage = memoryStorage();
    expect(readLastVisit("a", storage)).toBeNull();
    writeLastVisit("a", "2026-11-03T10:00:00Z", storage);
    expect(readLastVisit("a", storage)).toBe("2026-11-03T10:00:00Z");
    expect(readLastVisit("b", storage)).toBeNull();
    storage.map.set("cs.home.lastVisit.c", "corrupt");
    expect(readLastVisit("c", storage)).toBeNull();
    expect(readOnboarded("a", storage)).toBe(false);
    writeOnboarded("a", new Date("2026-11-03T10:00:00Z"), storage);
    expect(readOnboarded("a", storage)).toBe(true);
    expect(readOnboarded("b", storage)).toBe(false);
  });
  it("degrades to a first visit when storage is blocked", () => {
    const blocked = { getItem: () => { throw new Error("blocked"); }, setItem: () => { throw new Error("blocked"); } };
    expect(readLastVisit("a", blocked)).toBeNull();
    expect(() => writeLastVisit("a", "2026-11-03T10:00:00Z", blocked)).not.toThrow();
    expect(readOnboarded("a", blocked)).toBe(false);
  });
});

describe("prompt starters", () => {
  // The mock's sample rows (HomeScreen.jsx HOME_DATA, data.js topics). Each
  // template is read through `promptStarters`: the first chip a single row
  // yields, and the fallbacks when a row yields none.
  const FALLBACKS = ["What did I work on this week?", "What decisions were made recently?", "Who's working on what?"];
  const fromReef = (r: WhatsNewItem) => promptStarters([], [r])[0];
  const fromRow = (i: ContinueItem) => promptStarters([i], [])[0];
  const label = (x: { label: string }) => x.label;
  const question = (x: { question: string }) => x.question;

  it("quotes statement titles instead of splicing them into a question", () => {
    expect(fromReef(reef({ kind: "decision", title: "Usage-based billing starts at each Q1 renewal" }))).toEqual({
      label: "What led to “Usage-based billing starts at…”?",
      question: "What led to “Usage-based billing starts at each Q1 renewal”?",
    });
    expect(question(fromReef(reef({ kind: "decision", title: "Read model may serve data up to 30s stale" })))).toBe(
      "What led to “Read model may serve data up to 30s stale”?",
    );
    expect(question(fromReef(reef({ kind: "decision", title: "Move billing to usage-based" })))).toBe("What led to “Move billing to usage-based”?");
    expect(question(fromReef(reef({ kind: "thread", title: "Connector retry and back-off policy" })))).toBe(
      "Where did “Connector retry and back-off policy” land?",
    );
    expect(fromReef(reef({ kind: "thread", title: "EU invoicing rework" }))).toEqual({
      label: "Where did “EU invoicing rework” land?",
      question: "Where did “EU invoicing rework” land?",
    });
    expect(label(fromReef(reef({ kind: "reference", title: "Pricing FAQ v3" })))).toBe("Summarize “Pricing FAQ v3”");
    // Unknown kinds read like a thread.
    expect(label(fromReef(reef({ kind: "other", title: "Auth rewrite discussion" })))).toBe("Where did “Auth rewrite discussion” land?");
  });

  it("asks about a person or entity by name, without the title's descriptor", () => {
    expect(label(fromReef(reef({ kind: "person", title: "Kim Ng — pricing owner" })))).toBe("What do we know about Kim Ng?");
    expect(label(fromReef(reef({ kind: "entity", title: "Ari Vance – EU entity" })))).toBe("What do we know about Ari Vance?");
    expect(label(fromReef(reef({ kind: "entity", title: "SOC2" })))).toBe("What do we know about SOC2?");
    // A hyphen inside a word is not a descriptor.
    expect(label(fromReef(reef({ kind: "entity", title: "Usage-based billing" })))).toBe("What do we know about Usage-based billing?");
  });

  it("templates Continue rows: a branch by name, a task title quoted, and no chats", () => {
    expect(label(fromRow(item({ kind: "coding", title: "coralswarm · cs-read-model-serve-stale" })))).toBe("Catch me up on cs-read-model-serve-stale");
    // The branch is the last ` · ` part; a title without one is quoted whole.
    expect(label(fromRow(item({ kind: "coding", title: "repo · feature · branch" })))).toBe("Catch me up on branch");
    expect(label(fromRow(item({ kind: "agent", title: "no separator" })))).toBe("Catch me up on “no separator”");
    expect(fromRow(item({ kind: "agent", title: "Backfill #gtm-deals into Go-to-market" }))).toEqual({
      label: "Catch me up on “Backfill #gtm-deals into…”",
      question: "Catch me up on “Backfill #gtm-deals into Go-to-market”",
    });
    expect(label(fromRow(item({ kind: "workspace", title: "Platform" })))).toBe("Who's working on Platform?");
    expect(label(fromRow(item({ kind: "meeting", title: "Home redesign review" })))).toBe("What came out of Home redesign review?");
    expect(promptStarters([item({ kind: "chat" })], []).map(label)).toEqual(FALLBACKS);
    expect(promptStarters([item({ kind: "workspace", title: "  " })], []).map(label)).toEqual(FALLBACKS);
  });

  it("keeps chips short and cuts cleanly, at a word, never after a dash", () => {
    expect(label(fromReef(reef({ title: "  What   did we decide?  " })))).toBe("What led to “What did we decide”?");
    expect(label(fromReef(reef({ kind: "thread", title: "Pricing owner for EU billing — Kim Ng and more" })))).toBe(
      "Where did “Pricing owner for EU billing…” land?",
    );
    // No word break in reach: cut at the limit.
    expect(label(fromReef(reef({ title: "Supercalifragilisticexpialidocious-and-more-words" })))).toBe(
      "What led to “Supercalifragilisticexpialidocio…”?",
    );
    // A chip shows at most 32 characters of a title, plus the ellipsis.
    for (const title of ["Usage-based billing starts at each Q1 renewal", "Read model may serve data up to 30s stale", "Connector retry and back-off policy"]) {
      const topic = /“(.*)”/.exec(label(fromReef(reef({ title }))))?.[1] ?? "";
      expect(topic.length).toBeLessThanOrEqual(33);
    }
  });

  it("builds the mock's three: a decision, a branch, a meeting", () => {
    const starters = promptStarters(
      [
        item({ kind: "chat" }),
        item({ kind: "coding", id: "s1", title: "coralswarm · cs-read-model-serve-stale" }),
        item({ kind: "meeting", id: "m1", title: "Home redesign review" }),
        item({ kind: "agent", id: "a1", title: "Backfill #gtm-deals into Go-to-market" }),
        item({ kind: "workspace", id: "w1", title: "Platform" }),
      ],
      [
        reef({ title: "Usage-based billing starts at each Q1 renewal" }),
        reef({ reef_id: "r2", title: "Read model may serve data up to 30s stale" }),
        reef({ reef_id: "r3", kind: "thread", title: "Connector retry and back-off policy" }),
        reef({ reef_id: "r4", kind: "person", title: "Kim Ng — pricing owner" }),
      ],
    );
    expect(starters.map((s) => s.label)).toEqual([
      "What led to “Usage-based billing starts at…”?",
      "Catch me up on cs-read-model-serve-stale",
      "What came out of Home redesign review?",
    ]);
    expect(starters[0].question).toBe("What led to “Usage-based billing starts at each Q1 renewal”?");
  });

  it("repeats a template only when the data offers no other", () => {
    expect(promptStarters([], [reef({ title: "Drop the legacy seat tier" }), reef({ reef_id: "r2", title: "Move billing to usage-based" })]).map((s) => s.label)).toEqual([
      "What led to “Drop the legacy seat tier”?",
      "What led to “Move billing to usage-based”?",
      "What did I work on this week?",
    ]);
  });

  it("de-duplicates and tops up from the fallbacks", () => {
    const dup = reef({ title: "EU invoicing rework", kind: "thread" });
    expect(promptStarters([], [dup, { ...dup, reef_id: "r2", title: "EU Invoicing Rework" }]).map((s) => s.label)).toEqual([
      "Where did “EU invoicing rework” land?",
      "What did I work on this week?",
      "What decisions were made recently?",
    ]);
    expect(promptStarters([], [])).toEqual(
      ["What did I work on this week?", "What decisions were made recently?", "Who's working on what?"].map((text) => ({ label: text, question: text })),
    );
  });
});

describe("freshness", () => {
  const t = Date.parse("2026-11-03T12:00:00Z");
  it("labels the age of the data, or refreshing", () => {
    expect(freshnessLabel(t, t + 20_000, false)).toBe("updated just now");
    expect(freshnessLabel(t, t + 4 * 60_000 + 5_000, false)).toBe("updated 4 min ago");
    expect(freshnessLabel(t, t + 125 * 60_000, false)).toBe("updated 2 h ago");
    expect(freshnessLabel(t, t + 60_000, true)).toBe("refreshing…");
    expect(freshnessLabel(null, t, false)).toBe("");
  });
  it("never reads a clock running ahead as negative age", () => {
    expect(freshnessLabel(t + 90_000, t, false)).toBe("updated just now");
  });
  it("measures age from the client's own fetch time, never the server clock", () => {
    expect(updatedAt(t)).toBe(t);
    expect(updatedAt(0)).toBeNull();
    // A server clock 10 minutes behind does not make a fresh answer look old:
    // the label never sees generated_at.
    expect(freshnessLabel(updatedAt(t), t + 20_000, false)).toBe("updated just now");
  });
});

describe("rows", () => {
  it("opens each Continue kind on its existing route", () => {
    expect(continueHref(item({ kind: "chat", id: "c 1" }))).toBe("/chats?id=c%201");
    expect(continueHref(item({ kind: "workspace", id: "g1" }))).toBe("/workspaces?id=g1");
    // A meeting row's id is its transcript source_id; lib/provenance builds the URL.
    expect(continueHref(item({ kind: "meeting", id: "src 1", graph_id: "g1" }))).toBe("/meeting?source_id=src+1&graph_id=g1");
    expect(continueHref(item({ kind: "meeting", id: "m1" }))).toBe("/meeting?source_id=m1");
    // Sessions have no page; Today on the session's local day lists them.
    expect(continueHref(item({ kind: "coding", id: "s1", at: "2026-11-03T12:00:00Z" }))).toBe("/today?d=2026-11-03");
    expect(continueHref(item({ kind: "agent", id: "s1", at: "bad" }))).toBe("/today");
  });
  it("opens a reef on its Topic page in its own ocean", () => {
    expect(reefHref(reef({ reef_id: "n1", ocean: { graph_id: "g 2", name: "x", kind: "org" } }))).toBe("/topics?id=n1&graph_id=g%202");
  });
  it("prefixes the context line with the kind", () => {
    expect(continueMeta(item({ kind: "meeting", context: "34 min · 5 people" }))).toBe("meeting · 34 min · 5 people");
    expect(continueMeta(item({ kind: "workspace", context: null }))).toBe("workspace");
  });
  it("never puts a grouped row's count in the context line (the pill shows it)", () => {
    const grouped = item({ kind: "coding", title: "coralswarm · cs-read-model-serve-stale", context: "claude code · last: cache warmup on cold start", count: 3 });
    expect(continueMeta(grouped)).toBe("coding · claude code · last: cache warmup on cold start");
    expect(continueMeta(grouped)).not.toMatch(/\d+ sessions?/);
  });
  it("colours reef kinds per the design system", () => {
    expect(reefHue("decision")).toBe("coral");
    expect(reefHue("thread")).toBe("amber");
    expect(reefHue("entity")).toBe("pink");
    expect(reefHue("person")).toBe("pink");
    expect(reefHue("reference")).toBe("plum");
  });
  it("gives the private ocean its fixed pink", () => {
    expect(whatsNewOceanHue({ graph_id: "g1", name: "Mine", kind: "private" })).toBe("pink");
    expect(whatsNewOceanHue({ graph_id: "g1", name: "Team", kind: "team" })).not.toBe("pink");
  });
  it("names the broken connector and what is wrong", () => {
    expect(attentionMeta({ provider: "slack", reason: "reauth" })).toBe("slack · sign-in expired");
    expect(attentionMeta({ provider: "github", reason: "failing" })).toBe("github · sync failing");
  });
});

describe("greeting", () => {
  // The salutation is lib/chatPrompts.ts `greeting`, tested there.
  it("uses the session's first name, never an id", () => {
    expect(firstNameOf({ firstName: "Dana", fullName: "Dana Ross" })).toBe("Dana");
    expect(firstNameOf({ firstName: null, fullName: "Dana Ross" })).toBe("Dana");
    expect(firstNameOf({ firstName: " ", fullName: null })).toBeNull();
    expect(firstNameOf(null)).toBeNull();
  });
});

describe("setup checklist", () => {
  const none = { mcp: false, ask: false };
  const data = (over: Partial<{ hasSource: boolean; hasMcp: boolean; hasAsked: boolean; needsAttention: boolean }> = {}) => ({
    hasSource: false,
    hasMcp: false,
    hasAsked: false,
    needsAttention: false,
    ...over,
  });
  it("starts at step 1", () => {
    const c = setupChecklist(none, data());
    expect(c.done).toBe(0);
    expect(c.total).toBe(3);
    expect(c.steps.map((s) => [s.id, s.done, s.current])).toEqual([
      ["connect", false, true],
      ["mcp", false, false],
      ["ask", false, false],
    ]);
  });
  it("marks step 1 done from has_source alone, never from a broken connector or a click", () => {
    const c = setupChecklist(none, data({ hasSource: true }));
    expect(c.done).toBe(1);
    expect(c.steps.find((s) => s.current)?.id).toBe("mcp");
    expect(setupChecklist(none, data()).steps[0].done).toBe(false);
    expect(setupChecklist(none, data({ needsAttention: true })).steps[0]).toMatchObject({ done: false, attention: false });
  });
  it("keeps step 1 done but not a success while a connector is broken", () => {
    const c = setupChecklist(none, data({ hasSource: true, needsAttention: true }));
    expect(c.done).toBe(1);
    expect(c.steps[0]).toMatchObject({ id: "connect", done: true, attention: true, current: false });
    expect(c.steps.filter((s) => s.attention).map((s) => s.id)).toEqual(["connect"]);
    expect(setupChecklist(none, data({ hasSource: true })).steps[0].attention).toBe(false);
  });
  it("marks step 2 done from has_mcp or from copying the URL here", () => {
    expect(setupChecklist(none, data({ hasMcp: true })).steps[1].done).toBe(true);
    expect(setupChecklist({ ...none, mcp: true }, data()).steps[1].done).toBe(true);
    expect(setupChecklist(none, data()).steps[1].done).toBe(false);
  });
  it("marks step 3 done from has_asked or from asking on Home here", () => {
    expect(setupChecklist(none, data({ hasAsked: true })).steps[2].done).toBe(true);
    expect(setupChecklist({ ...none, ask: true }, data()).steps[2].done).toBe(true);
    expect(setupChecklist(none, data()).steps[2].done).toBe(false);
  });
  it("has no current step when all are done", () => {
    const c = setupChecklist({ mcp: true, ask: false }, data({ hasSource: true, hasAsked: true }));
    expect(c.done).toBe(3);
    expect(c.steps.some((s) => s.current)).toBe(false);
  });
});
