import { describe, expect, it } from "vitest";
import type { Graph, RetractionEntry, RetractionOutcome, RetractionRow } from "@/lib/api";
import {
  D8_READERS,
  entryOutcomeSummary,
  groupByOcean,
  mergeSessionRetractionEntries,
  NOTHING_REPORTED_BODY,
  NOTHING_REPORTED_TITLE,
  NOTHING_REPORTED_WHAT_REMAINS,
  oceanName,
  outcomeLabel,
  outcomeReason,
  outcomeTone,
  OUTCOME_LABEL,
  planRetractCompletion,
  planRetractReport,
  RETRACT_DIALOG_BODY,
  retractDialogDescription,
  sourceTypeLabel,
  surfaceLabel,
  survivorsNote,
  SYNC_TAKE_IT_BACK_AFTER,
  SYNC_TAKE_IT_BACK_BEFORE,
  SYNC_TAKE_IT_BACK_LABEL,
  SYNC_TAKE_IT_BACK_NOTE,
  whatRemainsText,
} from "@/lib/retraction";

function row(overrides: Partial<RetractionRow> = {}): RetractionRow {
  return {
    graph_id: "g1",
    is_personal_ocean: false,
    source_type: "coding_session",
    source_id: "cc:test:0",
    atom_id: null,
    surface: "content",
    outcome: "removed",
    survivors: [],
    ...overrides,
  };
}

const GRAPHS: Graph[] = [
  {
    id: "g1",
    name: "My private space",
    kind: "personal",
    is_primary: true,
    created_at: "2026-01-01T00:00:00Z",
    team_id: null,
    // Other Graph fields the type may carry are intentionally omitted here —
    // every helper under test reads only `id`/`name`/`kind`.
  } as Graph,
];

describe("outcomeReason — D6's bar: an unexplained ⚠ is worse than the green check it replaced", () => {
  it("removed carries no reason — a reached copy has nothing to explain", () => {
    expect(outcomeReason("removed")).toBeNull();
  });

  it("out_of_window names the 14-day rewrite window in plain words, not the enum value", () => {
    const reason = outcomeReason("out_of_window");
    expect(reason).not.toBeNull();
    expect(reason).not.toBe("out_of_window");
    expect(reason?.toLowerCase()).toContain("14-day");
    expect(reason?.toLowerCase()).not.toContain("out_of_window");
  });

  it("not_a_member names the ex-membership case in plain words, not the enum value", () => {
    const reason = outcomeReason("not_a_member");
    expect(reason).not.toBeNull();
    expect(reason).not.toBe("not_a_member");
    expect(reason?.toLowerCase()).toContain("member");
    expect(reason?.toLowerCase()).not.toContain("not_a_member");
  });

  it("the two unreached reasons are DISTINCT sentences — D7's refinement is per-(ocean,surface), not one generic warning", () => {
    expect(outcomeReason("out_of_window")).not.toBe(outcomeReason("not_a_member"));
  });
});

describe("outcomeTone", () => {
  it("removed is ok-toned; both unreached outcomes are warn-toned, with no hierarchy between them", () => {
    expect(outcomeTone("removed")).toBe("ok");
    expect(outcomeTone("out_of_window")).toBe("warn");
    expect(outcomeTone("not_a_member")).toBe("warn");
  });
});

describe("OUTCOME_LABEL — exactly D7's three-value vocabulary, nothing else", () => {
  it("covers exactly removed · out_of_window · not_a_member", () => {
    const values: RetractionOutcome[] = ["removed", "out_of_window", "not_a_member"];
    for (const v of values) {
      expect(OUTCOME_LABEL[v]).toBeTruthy();
    }
    expect(Object.keys(OUTCOME_LABEL).sort()).toEqual(values.slice().sort());
  });
});

describe("surfaceLabel", () => {
  it("humanizes the four surfaces this module's writer actually emits", () => {
    expect(surfaceLabel("content")).not.toBe("content");
    expect(surfaceLabel("coding_session")).not.toBe("coding_session");
    expect(surfaceLabel("meeting_transcript")).not.toBe("meeting_transcript");
    expect(surfaceLabel("journal_rollup")).not.toBe("journal_rollup");
  });

  it("falls back to the raw value for a surface it hasn't been taught about yet — never a blank row", () => {
    expect(surfaceLabel("some_future_surface")).toBe("some_future_surface");
  });
});

describe("oceanName", () => {
  it("resolves a known graph_id to its real name", () => {
    expect(oceanName(row({ graph_id: "g1" }), GRAPHS)).toBe("My private space");
  });

  it("an unknown not_a_member row names the ex-membership case, never a raw id", () => {
    const name = oceanName(row({ graph_id: "unknown-id", outcome: "not_a_member" }), GRAPHS);
    expect(name).not.toContain("unknown-id");
    expect(name.toLowerCase()).toContain("member");
  });

  it("an unknown personal row falls back to a generic personal label, never a raw id", () => {
    const name = oceanName(row({ graph_id: "unknown-id", is_personal_ocean: true, outcome: "removed" }), GRAPHS);
    expect(name).not.toContain("unknown-id");
  });
});

describe("groupByOcean — D6's own negative control: reached and unreached in ONE list", () => {
  it("mixed outcomes for the SAME ocean land in exactly one group, not split by outcome", () => {
    const rows = [
      row({ graph_id: "g1", surface: "content", outcome: "removed" }),
      row({ graph_id: "g1", surface: "journal_rollup", outcome: "out_of_window" }),
    ];
    const groups = groupByOcean(rows, GRAPHS);
    expect(groups).toHaveLength(1);
    expect(groups[0].rows).toHaveLength(2);
    const outcomes = groups[0].rows.map((r) => r.outcome).sort();
    expect(outcomes).toEqual(["out_of_window", "removed"]);
  });

  it("a not_a_member row for a DIFFERENT ocean lands in its own group, in first-seen order", () => {
    const rows = [
      row({ graph_id: "g1", outcome: "removed" }),
      row({ graph_id: "g2", outcome: "not_a_member" }),
    ];
    const groups = groupByOcean(rows, GRAPHS);
    expect(groups.map((g) => g.graphId)).toEqual(["g1", "g2"]);
    expect(groups[1].rows[0].outcome).toBe("not_a_member");
  });

  it("every row survives grouping — nothing is silently dropped, reached or not", () => {
    const rows = [
      row({ graph_id: "g1", outcome: "removed" }),
      row({ graph_id: "g1", outcome: "out_of_window" }),
      row({ graph_id: "g2", outcome: "not_a_member" }),
    ];
    const groups = groupByOcean(rows, GRAPHS);
    const total = groups.reduce((n, g) => n + g.rows.length, 0);
    expect(total).toBe(rows.length);
  });
});

describe("the retract confirm dialog's copy (D6: struck 'any time' and any cascade claim)", () => {
  it("never contains the words 'any time', in either the description or the body", () => {
    const description = retractDialogDescription("Sheet A-201 elevations").toLowerCase();
    const body = RETRACT_DIALOG_BODY.toLowerCase();
    expect(description).not.toContain("any time");
    expect(body).not.toContain("any time");
  });

  it("does not claim retraction reaches EVERYTHING — no bare 'everywhere'/'every copy' without qualification", () => {
    const body = RETRACT_DIALOG_BODY.toLowerCase();
    expect(body).not.toMatch(/\beverywhere\b/);
    expect(body).not.toMatch(/every copy\b/);
  });

  it("names FM-3's 14-day window explicitly, in the body a user actually reads", () => {
    expect(RETRACT_DIALOG_BODY).toContain("14 days");
  });

  it("names FM-4's ex-member case explicitly, in the body a user actually reads", () => {
    expect(RETRACT_DIALOG_BODY.toLowerCase()).toContain("left");
  });

  it("the description names the retracted item, not just an unqualified 'it'", () => {
    const description = retractDialogDescription("Sheet A-201 elevations");
    expect(description).toContain("Sheet A-201 elevations");
  });
});

// ── Independent review of PR #729 — the three ACCEPTANCE-BAR tests ─────────
//
// Each of these is stated by the review to FAIL on today's (pre-fix) code.
// `planRetractReport` and `whatRemainsText` are called DIRECTLY by
// `session/page.tsx` / `SessionRetractedView.tsx` — not reimplemented here —
// so this is a test of what those screens actually render, the same
// harness-free equivalent the dialog-copy tests above already rely on
// (this repo's vitest has no component-render harness).

describe("planRetractReport — finding 1 (BLOCKING): a partial retract must still produce a report", () => {
  it("a session-ending retract (coding_session removed) reports sessionGone=true", () => {
    const outcomes = [row({ surface: "coding_session", outcome: "removed" })];
    expect(planRetractReport(outcomes).sessionGone).toBe(true);
  });

  it("a PARTIAL retract (session NOT ended) still returns its unreached rows in reportRows", () => {
    const outcomes = [
      row({ surface: "content", outcome: "removed" }),
      row({ surface: "journal_rollup", outcome: "out_of_window" }),
    ];
    const plan = planRetractReport(outcomes);
    expect(plan.sessionGone).toBe(false);
    expect(plan.reportRows).toHaveLength(2);
    expect(plan.reportRows.some((r) => r.outcome === "out_of_window")).toBe(true);
  });

  it("reportRows is NEVER discarded, session-ending or not — same rows in, same rows out", () => {
    const outcomes = [row({ outcome: "not_a_member" })];
    expect(planRetractReport(outcomes).reportRows).toEqual(outcomes);
  });
});

describe("planRetractCompletion — independent review finding (outside-diff, no thread — BLOCKING): an empty report must reconcile with the server, not be treated as a successful no-op partial retract", () => {
  it("a session-ending retract (coding_session removed) returns sessionGone with the real rows", () => {
    const outcomes = [row({ surface: "coding_session", outcome: "removed" })];
    expect(planRetractCompletion(outcomes)).toEqual({ kind: "sessionGone", rows: outcomes });
  });

  it("a genuine partial retract (non-empty, session NOT ended) returns partial with the real rows", () => {
    const outcomes = [row({ surface: "content", outcome: "removed" })];
    expect(planRetractCompletion(outcomes)).toEqual({ kind: "partial", rows: outcomes });
  });

  // THE FIX. `surface_outcomes: []` comes back over HTTP 200 whenever the
  // atom was already absent — including when another actor's retract
  // already reached this session's LAST checkpoint and FM-6 already
  // deleted the whole session row server-side. Nothing in an empty array
  // distinguishes that from an ordinary already-gone no-op, so the caller
  // must not guess either way — it must ask the server.
  it("an EMPTY surface_outcomes (this session's last coral, deleted by another actor) returns reconcile — never sessionGone, never partial", () => {
    expect(planRetractCompletion([])).toEqual({ kind: "reconcile" });
  });

  // NEGATIVE CONTROL: this is literally the pre-fix branch this page used
  // to run (`session/page.tsx`'s old `const { sessionGone } =
  // planRetractReport(outcomes); if (sessionGone) { … } else { …
  // corals.filter(...), coral_count - 1 … }`). For an empty report,
  // `sessionGone` is `false` — `[].some(...)` is vacuously `false` — so the
  // OLD code takes the `else` branch: it optimistically renders the session
  // as still alive with one fewer coral, with no re-fetch and no 404
  // handling, even though the session may already be gone. This asserts
  // that shape fails exactly where `planRetractCompletion` above succeeds.
  it("NEGATIVE CONTROL: the pre-fix sessionGone-only branch takes the optimistic 'partial retract' path on an empty report, instead of reconciling", () => {
    const outcomes: RetractionRow[] = [];
    // `planRetractReport` itself is UNCHANGED by this fix (still the exact
    // function `session/page.tsx` used to branch on directly) — calling it,
    // not a re-implementation of it, is what makes this a control against
    // the real pre-fix logic rather than a paraphrase of it.
    const oldSessionGone = planRetractReport(outcomes).sessionGone;
    // This is the bug: an empty report reads as "not session-ending", so
    // the old code falls into the branch that mutates cached state as if
    // this were an ordinary partial retract — never reconciling with the
    // server, never considering that the session itself might be gone.
    expect(oldSessionGone).toBe(false);
    const oldBranchTaken = oldSessionGone ? "sessionGone" : "partial (optimistic local mutation)";
    expect(oldBranchTaken).toBe("partial (optimistic local mutation)");
    // The fixed decision function refuses to take that branch for the same
    // input — it hands back a THIRD outcome the old code had no way to
    // reach, forcing the caller to reconcile instead.
    expect(planRetractCompletion(outcomes).kind).not.toBe(oldSessionGone ? "sessionGone" : "partial");
    expect(planRetractCompletion(outcomes).kind).toBe("reconcile");
  });
});

describe('whatRemainsText — finding 2 (BLOCKING): the "what remains" card must reflect entry.rows', () => {
  it("every row removed keeps the original total-removal claim, verbatim", () => {
    const text = whatRemainsText([row({ outcome: "removed" }), row({ outcome: "removed" })]);
    expect(text).toContain("is visible anywhere anymore");
  });

  it("an out_of_window row changes the text — it must NOT claim total removal", () => {
    const text = whatRemainsText([row({ outcome: "removed" }), row({ outcome: "out_of_window" })]);
    expect(text).not.toContain("is visible anywhere anymore");
    expect(text).not.toContain("Nothing about it");
  });

  it("a not_a_member row also changes the text, and it says something was NOT removed", () => {
    const text = whatRemainsText([row({ outcome: "not_a_member" })]);
    expect(text.toLowerCase()).toContain("not");
    expect(text).not.toContain("is visible anywhere anymore");
  });
});

describe("outcomeLabel — finding 14 (LOW): an off-union outcome must render a visible label, never an empty badge", () => {
  it("every real outcome value still gets its normal label", () => {
    const values: RetractionOutcome[] = ["removed", "out_of_window", "not_a_member"];
    for (const v of values) {
      expect(outcomeLabel(v)).toBe(OUTCOME_LABEL[v]);
    }
  });

  it("an off-union value falls back to itself, never undefined/blank", () => {
    const offUnion = "future_outcome_value" as RetractionOutcome;
    expect(outcomeLabel(offUnion)).toBe("future_outcome_value");
    expect(outcomeLabel(offUnion)).toBeTruthy();
  });
});

describe("sourceTypeLabel — finding 13 (LOW): keyed by real source_type values, not surface values", () => {
  it("a surface value that used to squat in the old map is NOT a recognized source_type", () => {
    // "content" and "journal_rollup" are SURFACE values (SURFACE_LABELS'
    // domain) — the old settings-page map used them as source_type keys by
    // mistake, so they were dead entries. This asserts they fall back to
    // themselves here, exactly like any other unrecognized string.
    expect(sourceTypeLabel("content")).toBe("content");
    expect(sourceTypeLabel("journal_rollup")).toBe("journal_rollup");
  });

  it("real source types the old map never covered are humanized", () => {
    expect(sourceTypeLabel("slack")).not.toBe("slack");
    expect(sourceTypeLabel("gmail")).not.toBe("gmail");
    expect(sourceTypeLabel("coding_session")).not.toBe("coding_session");
  });

  it("labels a coding session as a coding session", () => {
    expect(sourceTypeLabel("coding_session")).toBe("coding session");
  });

  it("falls back to the raw value for a source_type it hasn't been taught about — never a blank row", () => {
    expect(sourceTypeLabel("some_future_source")).toBe("some_future_source");
  });
});

describe("mergeSessionRetractionEntries — finding 3 (BLOCKING): every per-checkpoint call's rows survive the merge", () => {
  function entry(overrides: Partial<RetractionEntry> = {}): RetractionEntry {
    return {
      retraction_id: "r1",
      retracted_at: "2026-08-25T10:00:00Z",
      actor_user_id: "u1",
      is_own: true,
      truncated: false,
      rows: [row()],
      ...overrides,
    };
  }

  it("returns null for an empty entries array — the caller falls back to its own synchronous rows", () => {
    expect(mergeSessionRetractionEntries([], "r1")).toBeNull();
  });

  it("merges rows from EVERY entry, not just the triggering one", () => {
    const e1 = entry({ retraction_id: "r1", rows: [row({ source_id: "cc:s:checkpoint-1" })] });
    const e2 = entry({ retraction_id: "r2", rows: [row({ source_id: "cc:s:checkpoint-2" })] });
    const merged = mergeSessionRetractionEntries([e1, e2], "r2");
    expect(merged?.rows).toHaveLength(2);
    expect(merged?.retraction_id).toBe("r2");
  });

  it("retracted_at is the LATEST timestamp across every merged entry", () => {
    const older = entry({ retraction_id: "r1", retracted_at: "2026-08-25T09:00:00Z" });
    const newer = entry({ retraction_id: "r2", retracted_at: "2026-08-25T11:00:00Z" });
    const merged = mergeSessionRetractionEntries([older, newer], "r2");
    expect(merged?.retracted_at).toBe("2026-08-25T11:00:00Z");
  });

  it("is_own is true only when EVERY merged entry is the caller's own", () => {
    const own = entry({ retraction_id: "r1", is_own: true });
    const other = entry({ retraction_id: "r2", is_own: false });
    expect(mergeSessionRetractionEntries([own], "r1")?.is_own).toBe(true);
    expect(mergeSessionRetractionEntries([own, other], "r1")?.is_own).toBe(false);
  });

  it("truncated is true if ANY merged entry was truncated", () => {
    const clean = entry({ retraction_id: "r1", truncated: false });
    const capped = entry({ retraction_id: "r2", truncated: true });
    expect(mergeSessionRetractionEntries([clean, capped], "r1")?.truncated).toBe(true);
  });
});

// ── Absence must never render as success ────────────────────────────────────
//
// `DELETE /v1/graphs/{id}/corals/{atom}` is idempotent and answers **200
// with `surface_outcomes: []`** whenever the atom was already gone — its own
// OpenAPI text says "Deleted (or already absent — idempotent)", and
// `services::deletion::delete_atom_cascade` returns `surface_outcomes:
// vec![]` on the `atom_was_present: false` arm. Every consumer below used to
// read that empty array as "no problems": the total-removal sentence, a
// green "fully removed" badge, and an empty container under a heading
// promising a report. Each test here has its NON-empty counterpart beside
// it as the negative control — an assertion that only ever sees `[]` would
// pass just as happily against a function that always returns the empty
// answer.

describe("an EMPTY report is not a clean one (absence-renders-as-success)", () => {
  it("planRetractReport flags a zero-row response, and does NOT flag a real one", () => {
    expect(planRetractReport([]).nothingReported).toBe(true);
    // Negative control: a genuine one-row report must not trip the flag, or
    // the flag would be "always true" and prove nothing.
    expect(planRetractReport([row()]).nothingReported).toBe(false);
    expect(planRetractReport([row({ outcome: "out_of_window" })]).nothingReported).toBe(false);
  });

  it("planRetractReport still returns the rows unchanged — the flag is additive, not a filter", () => {
    const rows = [row(), row({ surface: "journal_rollup", outcome: "out_of_window" })];
    expect(planRetractReport(rows).reportRows).toEqual(rows);
    expect(planRetractReport([]).reportRows).toEqual([]);
  });

  it("whatRemainsText([]) never makes the total-removal claim", () => {
    const empty = whatRemainsText([]);
    // The exact words the all-removed sentence uses. If any of these appear
    // for a zero-row report, the claim is being made from no evidence.
    expect(empty).not.toContain("anywhere anymore");
    expect(empty).not.toContain("has been removed along with its items");
    expect(empty.toLowerCase()).toContain("no outcomes");
  });

  it("whatRemainsText STILL makes the total-removal claim when rows prove it — the negative control", () => {
    const allRemoved = whatRemainsText([row(), row({ surface: "coding_session" })]);
    expect(allRemoved).toContain("anywhere anymore");
    // ...and the empty case is a genuinely DIFFERENT string, not the same
    // sentence reached by another route.
    expect(allRemoved).not.toEqual(whatRemainsText([]));
  });

  it("whatRemainsText([]) is the shared NOTHING_REPORTED_WHAT_REMAINS copy", () => {
    expect(whatRemainsText([])).toBe(NOTHING_REPORTED_WHAT_REMAINS);
  });

  it("the nothing-reported copy says out loud that it is not a clean result", () => {
    expect(NOTHING_REPORTED_TITLE.toLowerCase()).toContain("no outcomes");
    expect(NOTHING_REPORTED_BODY).toContain("not a clean one");
    // It must not silently imply removal happened. The only occurrence of
    // "was removed" in this string is inside its own NEGATION, so assert
    // the negation is present rather than banning the substring — a bare
    // `not.toContain("was removed")` would fail on the correct copy and
    // pass on a copy that said "removed everywhere" instead.
    expect(NOTHING_REPORTED_BODY).toContain("nothing here says any copy was removed");
    expect(NOTHING_REPORTED_WHAT_REMAINS).toContain("not the same as everything having been removed");
  });

  it("entryOutcomeSummary([]) is NOT clean and does not say 'fully removed'", () => {
    const empty = entryOutcomeSummary([]);
    expect(empty.clean).toBe(false);
    expect(empty.text).not.toContain("fully removed");
    expect(empty.text).toContain("no outcomes");
  });

  it("entryOutcomeSummary IS clean for an all-removed entry — the negative control", () => {
    const clean = entryOutcomeSummary([row(), row({ surface: "coding_session" })]);
    expect(clean).toEqual({ text: "fully removed", clean: true });
  });

  it("entryOutcomeSummary counts only the unreached rows, and pluralizes", () => {
    expect(entryOutcomeSummary([row(), row({ outcome: "out_of_window" })])).toEqual({
      text: "1 copy not reached",
      clean: false,
    });
    expect(
      entryOutcomeSummary([
        row(),
        row({ outcome: "out_of_window" }),
        row({ outcome: "not_a_member" }),
      ]),
    ).toEqual({ text: "2 copies not reached", clean: false });
  });
});

// ── D7's survivors[] — rendered as a count, never as ids ────────────────────

describe("survivorsNote — D7's survivors[], with no live producer today", () => {
  it("is null when the row names none — a row with nothing surviving renders no line", () => {
    expect(survivorsNote(row())).toBeNull();
    expect(survivorsNote(row({ survivors: [] }))).toBeNull();
  });

  it("counts survivors and never leaks an id (lib/coral.ts's cardinal rule)", () => {
    const id = "6f1c1c8e-6a41-4a1e-9f1d-2c9b0a5b7e11";
    const note = survivorsNote(row({ survivors: [id] }));
    expect(note).toBe("1 derived item left standing.");
    expect(note).not.toContain(id);
    expect(survivorsNote(row({ survivors: [id, "another-id"] }))).toBe(
      "2 derived items left standing.",
    );
  });

  it("tolerates a row whose survivors field is absent over the wire", () => {
    const wireRow = { ...row(), survivors: undefined } as unknown as RetractionRow;
    expect(survivorsNote(wireRow)).toBeNull();
  });
});

// ── D8, in plain words ──────────────────────────────────────────────────────

describe("D8_READERS — all three clauses, the same on both surfaces", () => {
  it("has exactly three clauses: author, shared-workspace admin, and nobody else", () => {
    expect(D8_READERS).toHaveLength(3);
    expect(D8_READERS[0]).toContain("every workspace");
    expect(D8_READERS[1].toLowerCase()).toContain("administrator");
  });

  it("names the personal-ocean/author-only clause — the one a two-bullet list drops", () => {
    const all = D8_READERS.join(" ").toLowerCase();
    expect(all).toContain("private space");
    expect(all).toContain("you alone");
  });

  it("never claims an admin can read the content (D7)", () => {
    expect(D8_READERS[1]).toContain("Never what it said");
    const all = D8_READERS.join(" ").toLowerCase();
    expect(all).not.toContain("the text");
    expect(all).not.toContain("what was said");
  });
});

// ── The sync/share chooser's own "Taking it back" row (D6 UI, first half) ───

describe("the sync chooser's 'Taking it back' copy (D6: 'any time' and the cascade are struck)", () => {
  const all = [
    SYNC_TAKE_IT_BACK_LABEL,
    SYNC_TAKE_IT_BACK_BEFORE,
    SYNC_TAKE_IT_BACK_AFTER,
    SYNC_TAKE_IT_BACK_NOTE,
  ].join(" ");

  it("never contains the words 'any time' or 'anytime'", () => {
    expect(all).not.toContain("any time");
    expect(all).not.toContain("anytime");
  });

  it("implies no cascade — no unqualified 'everywhere' / 'every copy' / 'all copies'", () => {
    const lower = all.toLowerCase();
    expect(lower).not.toContain("everywhere");
    expect(lower).not.toContain("every copy");
    expect(lower).not.toContain("all copies");
  });

  it("is the artboard's before → after pair, and the after is 'retract'", () => {
    expect(SYNC_TAKE_IT_BACK_LABEL).toBe("Taking it back");
    expect(SYNC_TAKE_IT_BACK_AFTER).toBe("retract");
    expect(SYNC_TAKE_IT_BACK_BEFORE).not.toBe(SYNC_TAKE_IT_BACK_AFTER);
  });

  it("names FM-3's 14-day window and FM-4's left-workspace case, before the copy is made", () => {
    expect(SYNC_TAKE_IT_BACK_NOTE).toContain("14 days");
    expect(SYNC_TAKE_IT_BACK_NOTE.toLowerCase()).toContain("workspace you've left");
  });

  it("scopes retract's reach to current membership rather than promising all destinations", () => {
    expect(SYNC_TAKE_IT_BACK_NOTE).toContain("still a member of");
  });
});

// ── A reason must name the CONSEQUENCE, not restate the rule ────────────────
//
// D6's bar is that an unexplained ⚠ is worse than the green check it
// replaced. A sentence that says "the summaries outside the window were not
// rewritten" is not an explanation — it restates the rule and never tells
// the reader what it costs them. These assert the cost is named.

describe("outcomeReason names what the unreached copy actually means for the reader", () => {
  it("out_of_window says that text still DESCRIBES the retracted thing", () => {
    const reason = outcomeReason("out_of_window")?.toLowerCase();
    expect(reason).toBeTruthy();
    expect(reason).toContain("still describe");
    // ...and still names the window itself, so the reader can tell which
    // items are affected.
    expect(reason).toContain("14-day");
  });

  it("not_a_member says who CAN remove the copy, since the reader can't", () => {
    const reason = outcomeReason("not_a_member")?.toLowerCase();
    expect(reason).toBeTruthy();
    expect(reason).toContain("administrator");
    expect(reason).toContain("no longer a member");
  });

  it("both reasons speak of a 'workspace', never the internal 'ocean' — the surrounding rows do too", () => {
    // `oceanName`'s own fallbacks say "workspace"; a row that says "ocean"
    // in its reason and "workspace" in its heading reads as two places.
    for (const outcome of ["out_of_window", "not_a_member"] as const) {
      const reason = outcomeReason(outcome);
      expect(reason).not.toBeNull();
      expect(reason?.toLowerCase()).not.toContain("ocean");
    }
    // Negative control: the fallbacks these have to agree with really do
    // say "workspace", so this is an agreement test, not a banned-word test.
    expect(
      oceanName(row({ graph_id: "unknown", outcome: "not_a_member" }), GRAPHS).toLowerCase(),
    ).toContain("workspace");
  });
});
