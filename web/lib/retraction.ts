import type { RetractionEntry, RetractionOutcome, RetractionRow } from "@/lib/api";
import type { Graph } from "@/lib/api";

/* Independent review of PR #729 fixed most of what follows in this module —
   see each function's own doc for its finding number. Two structural notes
   that don't belong to any ONE function:

   1. This repo's vitest config has NO `NEXT_PUBLIC_*` env (and no DOM-render
      harness — see the dialog-copy section below) — a module-scope env read
      makes an assertion pass VACUOUSLY. Nothing here reads `process.env`, so
      that trap does not apply to this file, but it is why every test in
      `retraction.test.ts` passes VALUES as parameters rather than reading
      shared/global state.
   2. `planRetractReport` and `whatRemainsText` below are EXTRACTED, not
      decorative — `web/app/(app)/session/page.tsx` and
      `SessionRetractedView.tsx` call them directly, so a test against these
      functions is a test against what those screens actually render, not a
      parallel reimplementation that could drift from the real one. */

/* D6/D7's outcome vocabulary, translated to plain words (ticket 02's own
   bar: "an unexplained ⚠ is worse than the green check it replaced" —
   docs/plans/capture/02-b2-retract-fixes.md). Every string here names a
   REAL, accepted limitation (FM-3's 14-day rewrite window, FM-4's
   ex-member case) rather than a raw enum value — never render `outcome`
   itself to a user. */

/** Short label for the outcome chip — matches the artboard's own wording
 *  ("removed" / "out of window" / "not a member"). */
export const OUTCOME_LABEL: Record<RetractionOutcome, string> = {
  removed: "removed",
  out_of_window: "out of window",
  not_a_member: "not a member",
};

/** `Badge`'s `tone` prop for one outcome — green for reached, amber for
 *  both unreached kinds (D6 draws no visual hierarchy between the two
 *  unreached reasons; both get equal weight against `removed`). */
export function outcomeTone(outcome: RetractionOutcome): "ok" | "warn" {
  return outcome === "removed" ? "ok" : "warn";
}

/** The plain-word reason a copy was NOT reached — `null` for `removed`,
 *  since a reached row carries no "why". Exact wording is deliberate:
 *  it names the accepted limitation (FM-3 or FM-4), never the enum value.
 *
 *  **No `default:` arm (independent review of PR #729, finding 14, LOW).**
 *  `RetractionOutcome` is a closed 3-value union; a `switch` over it with no
 *  `default` is exhaustiveness-checked by TypeScript — add a 4th outcome to
 *  the union without adding an arm here and the build fails at the call
 *  site that added it, rather than this function silently returning `null`
 *  (D6's own bar: "an unexplained ⚠ is worse than the green check it
 *  replaced" — a `default: return null` is exactly that unexplained ⚠, just
 *  deferred to runtime instead of caught at compile time). */
export function outcomeReason(outcome: RetractionOutcome): string | null {
  switch (outcome) {
    case "removed":
      return null;
    case "out_of_window":
      // Names the CONSEQUENCE, not just the mechanism. The previous
      // wording ("the summaries beyond the 14-day rewrite window were not
      // rewritten") is circular: it restates the rule and never tells the
      // reader what it costs them, which is that prose describing the
      // retracted thing is still sitting there. D6's bar is a reason "in
      // plain words", and a rule is not a reason.
      return (
        "Daily summaries are only rewritten inside a 14-day window. Older ones are not, so some " +
        "of them still describe this."
      );
    case "not_a_member":
      // "workspace", not "ocean": every other string on these two screens
      // says workspace (including `oceanName`'s own fallback, "a workspace
      // you're no longer a member of"), and a row that says both in the
      // same card reads as two different places. Second sentence is the
      // artboard's: the remedy exists, it just is not yours to take.
      return (
        "You're no longer a member of that workspace, so the copy there couldn't be reached. An " +
        "administrator of it can remove the copy."
      );
  }
}

/** `OUTCOME_LABEL[outcome]`, WITH a fallback to the raw value (independent
 *  review of PR #729, finding 14, LOW) — use this instead of indexing
 *  `OUTCOME_LABEL` directly. An off-union value arriving over the wire (the
 *  backend's `outcome` column is free TEXT under a CHECK constraint, not a
 *  type-checked enum — a 4th value shipped there before the frontend knows
 *  about it is exactly the skew this guards) used to render an EMPTY
 *  `<Badge>` (`OUTCOME_LABEL[value]` is `undefined`, and React renders
 *  `undefined` as nothing) — the one shape D6 calls out by name: "an
 *  unexplained ⚠ is worse than the green check it replaced", except this was
 *  worse still, a BLANK badge, not even a ⚠. Mirrors `surfaceLabel` two
 *  functions below, which already falls back to the raw value for the exact
 *  same reason ("never a blank row"). */
export function outcomeLabel(outcome: RetractionOutcome): string {
  return OUTCOME_LABEL[outcome] ?? outcome;
}

/** Human label for a `surface` value. Free text on the wire (new surfaces
 *  are expected over time per D7's own schema comment) — an unrecognized
 *  value falls back to itself rather than disappearing, so a future surface
 *  the frontend hasn't been taught about still renders SOMETHING instead of
 *  a blank row. */
const SURFACE_LABELS: Record<string, string> = {
  content: "The item itself",
  coding_session: "The session record",
  meeting_transcript: "The meeting transcript",
  journal_rollup: "Daily summaries covering this",
};
export function surfaceLabel(surface: string): string {
  return SURFACE_LABELS[surface] ?? surface;
}

/** Human label for a `source_type` value — moved here from
 *  `settings/privacy/page.tsx` (independent review of PR #729,
 *  finding 13, LOW): that page's own map was keyed with SURFACE values
 *  (`"content"`, `"journal_rollup"`) instead of real `source_type` values,
 *  so two of its four entries were unreachable dead code (no row's
 *  `source_type` is ever `"content"` or `"journal_rollup"` — those are
 *  `surface` strings, `SURFACE_LABELS`' domain, not this one's) while real
 *  source types (`"slack"`, `"coding_session"`, `"gmail"`, …) fell through
 *  to the raw-value fallback unlabeled. This module already owns the
 *  surface/outcome vocabulary split (`SURFACE_LABELS` above,
 *  `OUTCOME_LABEL`) — the source-type map belongs beside them, not on a
 *  page component. Same "never a blank row" fallback as `surfaceLabel`. */
const SOURCE_TYPE_LABELS: Record<string, string> = {
  coding_session: "coding session",
  agent_session: "agent session",
  work_session: "work session",
  meeting_session: "meeting",
  transcript: "meeting",
  slack: "Slack",
  gmail: "email",
  google_calendar: "calendar event",
  teams_message: "Teams message",
  outlook_thread: "email",
  notion: "Notion",
  note: "note",
};
export function sourceTypeLabel(sourceType: string): string {
  return SOURCE_TYPE_LABELS[sourceType] ?? sourceType;
}

/** Resolve a `graph_id` to a human ocean name using the caller's OWN known
 *  oceans (`useApp().graphs` — no separate name lookup on the backend; see
 *  `routes::retractions`' module doc, which deliberately returns bare ids).
 *  A `not_a_member` row's ocean is, by definition, one the caller can no
 *  longer resolve a name for — that absence is itself information, not a
 *  bug, so it gets its own sentence rather than a raw id. */
export function oceanName(row: RetractionRow, graphs: Graph[]): string {
  const known = graphs.find((g) => g.id === row.graph_id);
  if (known) return known.name;
  if (row.outcome === "not_a_member") return "a workspace you're no longer a member of";
  return row.is_personal_ocean ? "your private space" : "a shared workspace";
}

/** Group rows by ocean, preserving first-seen order — the shape
 *  `SessionRetracted`'s "Where this reached" list and its artboard both
 *  render: one card per ocean, its surfaces listed underneath. */
export function groupByOcean(
  rows: RetractionRow[],
  graphs: Graph[],
): { graphId: string; name: string; isPersonalOcean: boolean; rows: RetractionRow[] }[] {
  const order: string[] = [];
  const byGraph = new Map<string, RetractionRow[]>();
  for (const row of rows) {
    const existing = byGraph.get(row.graph_id);
    if (existing) {
      existing.push(row);
    } else {
      byGraph.set(row.graph_id, [row]);
      order.push(row.graph_id);
    }
  }
  return order.flatMap((graphId) => {
    const groupRows = byGraph.get(graphId);
    if (!groupRows || groupRows.length === 0) return [];
    return [
      {
        graphId,
        name: oceanName(groupRows[0], graphs),
        isPersonalOcean: groupRows[0].is_personal_ocean,
        rows: groupRows,
      },
    ];
  });
}

// ── An EMPTY report is not a clean one ──────────────────────────────────────
//
// `DELETE /v1/graphs/{id}/corals/{atom}` answers **200 with
// `surface_outcomes: []`** whenever the atom was already absent — the
// route's own OpenAPI text says so ("Deleted (or already absent —
// idempotent)"), and `services::deletion::delete_atom_cascade` returns
// `surface_outcomes: vec![]` on the `atom_was_present: false` arm. A
// double-click, a retry after a dropped response, a stale list, or a
// checkpoint someone else already retracted all land here.
//
// Every consumer of that array treated "no rows" as "no problems": the
// oceans list rendered an empty box under a heading promising a report,
// `whatRemainsText` returned its total-removal sentence, and
// `entryOutcomeSummary`'s predecessor returned "fully removed". That is
// this program's single most repeated defect, verbatim from its own
// negative-control list — *"a status code turning missing data into a
// green result"* — inside the very feature written to prevent it. The
// strings below are the visible difference, and
// `planRetractReport().nothingReported` is the flag every caller branches
// on.

/** Heading for the "we were told nothing" state. Deliberately not a ⚠ with
 *  no words next to it — D6's bar is that an unexplained ⚠ is worse than
 *  the green check it replaced, and this state is the one most likely to be
 *  mistaken for a green check. */
export const NOTHING_REPORTED_TITLE = "This retract reported no outcomes";

/** Why it can happen, and — the load-bearing half — what it does NOT mean. */
export const NOTHING_REPORTED_BODY =
  "An empty report is not a clean one. Retract is idempotent, so the usual cause is that this was " +
  "already gone before the call and there was no work left to report — but nothing here says any " +
  "copy was removed. Settings › Privacy has what earlier retracts of it actually reached.";

/** The `SessionRetracted` "what remains" card, when the report is empty —
 *  the same refusal to claim removal, phrased for that card. */
export const NOTHING_REPORTED_WHAT_REMAINS =
  "This retract reported no outcomes, so this page cannot tell you what it reached. That is not the " +
  "same as everything having been removed — check Settings › Privacy for every retraction " +
  "that named this session.";

// ── The post-retract report (independent review of PR #729, findings 1/2) ──
//
// D6's own frame: "the promise shrinks and the REPORT carries the truth."
// Two findings said the report as shipped did not do that — both are fixed
// by the functions below, and both are called DIRECTLY by the screens that
// render the report (`session/page.tsx`, `SessionRetractedView.tsx`), not
// duplicated in them, so a test against these functions is a test against
// what actually renders.

/** What ONE retract call's own `surface_outcomes` mean for the page that
 *  just made the call — `sessionGone` (FM-6: did this call empty the whole
 *  session, so the WHOLE-PAGE `SessionRetracted` state should replace the
 *  session detail view) and `reportRows` (the rows a report must be shown
 *  for).
 *
 *  **Finding 1, BLOCKING.** Before this fix, `session/page.tsx`'s retract
 *  handler branched on `sessionGone` and, in the `else` (partial-retract)
 *  branch, discarded `surface_outcomes` entirely — no state ever captured
 *  them, so a retract that was NOT the session's last checkpoint rendered
 *  no report at all: the coral row just vanished from the list. That is the
 *  "success state with a footnote" (worse: no footnote) D6 exists to
 *  eliminate, directly under a confirm dialog that just promised "you'll
 *  see exactly what happened, next" (`RETRACT_DIALOG_BODY` above).
 *  `reportRows` is now ALWAYS `surfaceOutcomes`, unconditionally — the
 *  caller renders a report every time, session-ending or not; only WHERE it
 *  renders (replacing the page vs. an inline card) depends on
 *  `sessionGone`. */
export function planRetractReport(surfaceOutcomes: RetractionRow[]): {
  sessionGone: boolean;
  reportRows: RetractionRow[];
  nothingReported: boolean;
} {
  const sessionGone = surfaceOutcomes.some(
    (o) => o.surface === "coding_session" && o.outcome === "removed",
  );
  return {
    sessionGone,
    reportRows: surfaceOutcomes,
    nothingReported: surfaceOutcomes.length === 0,
  };
}

/** What `session/page.tsx`'s retract handler does NEXT, given one call's
 *  `surface_outcomes` — replaces the `sessionGone ? … : …` branch that used
 *  to be inlined there.
 *
 *  **Independent review finding (outside-diff, no review thread — invisible
 *  in the GitHub UI, BLOCKING): the "someone else's retract" case named in
 *  `planRetractReport`'s own doc above was fixed for how the REPORT
 *  renders, not for whether the session still exists.** `nothingReported`
 *  (an empty `surfaceOutcomes`) is NOT evidence that nothing changed — it
 *  only means THIS call did no work, and the idempotent-absence contract
 *  that produces it covers two different facts: the coral was already gone
 *  before this call (a double-click, a retry, a stale list) — session
 *  unaffected — OR another actor's retract already reached it and, if it
 *  was the session's LAST checkpoint, already deleted the whole session row
 *  (FM-6). `surfaceOutcomes` alone cannot tell these apart. The caller used
 *  to assume the cheaper one and mutate its own cache to match
 *  (`corals.filter(...)`, `coral_count - 1`) — rendering a session that
 *  might already be gone as still alive with one fewer coral, with no
 *  re-fetch and no 404 handling. That is "absence renders as success" one
 *  layer above the report this PR exists to fix.
 *
 *  `"reconcile"` tells the caller to ask the SERVER, not its own cache,
 *  whether the session still exists (re-`GET` it; a 404 is the truth, not
 *  an edge case to paper over). `"sessionGone"` and `"partial"` are both
 *  backed by a real (surface, outcome) row and keep the previous, already
 *  correct behavior unchanged. */
export type RetractCompletionAction =
  | { kind: "sessionGone"; rows: RetractionRow[] }
  | { kind: "reconcile" }
  | { kind: "partial"; rows: RetractionRow[] };

export function planRetractCompletion(surfaceOutcomes: RetractionRow[]): RetractCompletionAction {
  const { sessionGone, reportRows, nothingReported } = planRetractReport(surfaceOutcomes);
  if (sessionGone) return { kind: "sessionGone", rows: reportRows };
  if (nothingReported) return { kind: "reconcile" };
  return { kind: "partial", rows: reportRows };
}

/** The `SessionRetracted` "what remains" card's text, COMPUTED from
 *  `entry.rows` — the same source `entrySummary` in
 *  `settings/privacy/page.tsx` already derives its own summary
 *  from, rather than a second, potentially-drifting hand-written claim.
 *
 *  **Finding 2, BLOCKING.** The card used to be a static, unconditional
 *  string asserting total removal ("Nothing about it … is visible anywhere
 *  anymore") rendered directly beneath rows that can PROVE it false (an
 *  `out_of_window` row means a journal summary inside the 14-day window
 *  still describes the session) — precisely the "success state with a
 *  footnote" this screen's own module header says it exists to eliminate.
 *  When every row is `removed`, the original total-removal claim is true
 *  and is kept verbatim; otherwise the text names that copies remain, points
 *  at the list above for which ones, and never claims "nowhere" or
 *  "anymore" again. */
export function whatRemainsText(rows: RetractionRow[]): string {
  // ZERO rows is not "nothing was left behind" — it is "nothing was
  // reported", and the two must never render the same. See
  // `NOTHING_REPORTED_BODY` above for the case that produces it. Without this
  // branch the `unreached.length === 0` test below is satisfied VACUOUSLY
  // by an empty array and the total-removal claim is made from no evidence
  // at all: `absence-renders-as-success`, on the one screen whose entire
  // purpose is to not do that.
  if (rows.length === 0) return NOTHING_REPORTED_WHAT_REMAINS;
  const unreached = rows.filter((r) => r.outcome !== "removed");
  if (unreached.length === 0) {
    return (
      "The session record itself has been removed along with its items. Nothing about it — " +
      "not the title, not the app, not how long it ran — is visible anywhere anymore."
    );
  }
  const n = unreached.length;
  return (
    `The session record and its items were removed everywhere retract could reach — but ${n} ` +
    `${n === 1 ? "copy" : "copies"} could not be. See "Where this reached" above for exactly ` +
    `which, and why.`
  );
}

/** One retraction entry's headline verdict — the artboard's own wording
 *  ("fully removed" / "N copies remain"), computed from the SAME rows the
 *  expanded detail renders rather than a separate summary field that could
 *  drift from them.
 *
 *  **Moved out of `settings/privacy/page.tsx` so the zero-row
 *  case is testable.** Its predecessor computed
 *  `rows.filter(unreached).length === 0 -> "fully removed"`, which an EMPTY
 *  `rows` array satisfies vacuously: an entry the backend truncated to
 *  nothing, or a synchronous entry built from an idempotent no-op retract's
 *  empty `surface_outcomes`, rendered a GREEN "fully removed" badge from no
 *  evidence at all. `clean` is now `true` only when at least one row was
 *  actually seen AND every one of them was reached. */
export function entryOutcomeSummary(rows: RetractionRow[]): { text: string; clean: boolean } {
  if (rows.length === 0) return { text: "no outcomes recorded", clean: false };
  const unreached = rows.filter((r) => r.outcome !== "removed").length;
  if (unreached === 0) return { text: "fully removed", clean: true };
  return { text: `${unreached} ${unreached === 1 ? "copy" : "copies"} not reached`, clean: false };
}

/** D7's `survivors[]` in plain words — "ids of derived artifacts left
 *  standing" — as a COUNT, never the ids themselves (`lib/coral.ts`'s
 *  cardinal rule: never show a raw id to a user). `null` when the row names
 *  none, so a row with nothing surviving renders no line at all rather than
 *  an empty one.
 *
 *  ⚠ **`survivors` has no live producer today.** All four `SurfaceOutcome`
 *  construction sites in `backend/src/services/deletion.rs` pass
 *  `survivors: vec![]`, so this function returns `null` for every row the
 *  running system can currently write — the same shape `not_a_member` is
 *  in (schema-real, constraint-tested, never emitted). It is rendered
 *  anyway because `RetractionHistory` CLAIMS the record keeps "anything
 *  derived from it that is still standing"; a claim with no surface to show
 *  it is the kind of sentence this program treats as a defect. Do not read
 *  its permanent absence as "nothing ever survives a retract". */
export function survivorsNote(row: RetractionRow): string | null {
  const n = row.survivors?.length ?? 0;
  if (n === 0) return null;
  return `${n} derived ${n === 1 ? "item" : "items"} left standing.`;
}

/** Merge every `RetractionEntry` group `GET /v1/retractions/session/{id}`
 *  returns for ONE session into a SINGLE composite entry for
 *  `SessionRetracted` to render — the union of every row from every
 *  per-checkpoint retract call the session ever had, not only the one that
 *  finally emptied it (independent review of PR #729, finding 3, BLOCKING —
 *  see `web/app/(app)/session/page.tsx`'s own comment for why the fix reads
 *  the audit log back rather than wiring `DELETE /v1/graphs/{id}/sources`).
 *
 *  `retraction_id` keeps `triggeringRetractionId` (the call that JUST
 *  ended the session) so the `?retraction=` URL marker stays meaningful for
 *  re-hydration; `retracted_at` is the LATEST timestamp across every merged
 *  entry (the moment the session actually disappeared); `is_own` is `true`
 *  only when EVERY merged call was the caller's own (conservative — a
 *  session's retraction history should not read as "all yours" if any part
 *  of it wasn't); `truncated` is `true` if ANY merged entry was itself
 *  truncated (finding 6). Returns `null` for an empty `entries` array —
 *  the caller falls back to the triggering call's own synchronous rows
 *  rather than rendering an empty report. */
export function mergeSessionRetractionEntries(
  entries: RetractionEntry[],
  triggeringRetractionId: string,
): RetractionEntry | null {
  if (entries.length === 0) return null;
  const triggering = entries.find((e) => e.retraction_id === triggeringRetractionId);
  const retracted_at = entries.reduce<string | null>((latest, e) => {
    if (!e.retracted_at) return latest;
    if (!latest || e.retracted_at > latest) return e.retracted_at;
    return latest;
  }, null);
  return {
    retraction_id: triggeringRetractionId,
    retracted_at,
    actor_user_id: triggering?.actor_user_id ?? entries[0].actor_user_id,
    is_own: entries.every((e) => e.is_own),
    truncated: entries.some((e) => e.truncated),
    rows: entries.flatMap((e) => e.rows),
  };
}

// ── D8, in plain words — who can read a retraction record ───────────────────
//
// D8 (docs/plans/capture/02-b2-retract-fixes.md): "Retraction rows are
// readable by the author (all oceans) and by admins of the ocean the row
// belongs to. Personal-ocean rows are author-only." All three clauses are
// here because the third is the one that protects the user and the one a
// two-bullet list silently drops — and because D8's own accepted cost is
// that an admin gets a volume-and-timing signal, which is only fair if the
// person retracting can read the same rule.
//
// Enforced in SQL, not by these strings: `retraction_audit_log`'s SELECT
// policy (`migrations_control/0196`) is
// `actor_user_id = app.user_id OR (NOT is_personal_ocean AND EXISTS(...
// account_members ... role IN ('owner','admin')))`. The middle clause is
// keyed to an ACCOUNT-level `owner`/`admin` role, which is how "admin of
// the ocean" is spelled in this schema — there is no per-ocean admin grant.
// The wording below therefore says "of the organization a shared workspace
// belongs to" rather than implying a per-workspace setting that does not
// exist and that a reader could go looking for.
export const D8_READERS: readonly string[] = [
  "You — every retraction you have made, in every workspace.",
  "An owner or administrator of the organization a shared workspace belongs to — that a " +
    "retraction touched it: who, when, which item, and the outcome. Never what it said.",
  "Nobody else. A retraction in your private space is visible to you alone — not to an " +
    "administrator of your organization, and not to anyone else in it.",
];

// ── The "moment of decision" dialog's own copy (D6 UI) ──────────────────────
//
// docs/plans/capture/02-b2-retract-fixes.md, D6: "At the moment of decision
// the dialog says only... The words 'any time' and any implication of a
// full cascade are struck — both are false under D5 (FM-3's 14-day
// lookback, FM-4's ex-member)." Exported as plain strings/functions — not
// inlined in `RetractConfirmDialog` — so a test can assert on the exact
// text directly: this repo's vitest config runs `.test.ts` only, with NO
// component-render harness (`vitest.config.ts`'s own comment: "this project
// deliberately has no component-test harness"), so a DOM-rendered assertion
// isn't available here; testing the same strings the component renders is
// the equivalent, harness-free negative control.

export const RETRACT_DIALOG_TITLE = "Retract this?";

/** `label` is the thing being retracted, named plainly (a checkpoint's own
 *  title) — never "it" alone, and never a claim about WHERE it reaches. */
export function retractDialogDescription(label: string): string {
  return `Removes “${label}” from search, answers, and every workspace you're still a member of.`;
}

/** The body line naming BOTH accepted limitations (FM-3, FM-4) explicitly —
 *  this is what replaces the struck "any time"/cascade promise, not a
 *  vaguer substitute for it. */
export const RETRACT_DIALOG_BODY =
  "Summaries written more than 14 days ago won't be rewritten, and a workspace you've since left " +
  "can't be reached at all — you'll see exactly what happened, next.";

// ── The sync/share chooser's "Taking it back" row (D6 UI, first half) ───────
//
// D6, verbatim: "At the moment of decision the dialog says only: 'Taking it
// back: delete the file -> retract.' The words 'any time' and any
// implication of a full cascade are struck — both are false under D5
// (FM-3's 14-day lookback, FM-4's ex-member)."
//
// The original sync-chooser design drew that as a row in a "what changes"
// panel: label "Taking it back", before "delete the file", after "retract".
// `PromoteModal` is its shipped counterpart and had NO such row at all: it named nothing about how a shared
// copy comes back. PR #729 recorded "nothing to fix there" on the grounds
// that SyncChooser has no live implementation — the UI-track table says it
// does. Silence is not the corrected copy; it is the same promise left
// unstated, on the one dialog where the decision is actually taken.
//
// Note what these strings do NOT say: no "any time", no "everywhere", no
// "removes every copy". The second sentence names FM-3 and FM-4 in the same
// breath as the promise, because after this dialog those are the two ways
// the promise fails.

export const SYNC_TAKE_IT_BACK_LABEL = "Taking it back";

/** Before/after. `PromoteModal` shares a coral / reef / session that is
 *  already in an ocean, so before sharing, taking it back is a local delete
 *  in one ocean; after, it is a retract whose reach the note below bounds. */
export const SYNC_TAKE_IT_BACK_BEFORE = "delete it here";
export const SYNC_TAKE_IT_BACK_AFTER = "retract";

/** The one qualifying sentence that keeps the row from reading as a full
 *  cascade — the same two accepted limitations `RETRACT_DIALOG_BODY` names,
 *  stated BEFORE the copy is made rather than after. */
export const SYNC_TAKE_IT_BACK_NOTE =
  "Retract reaches the workspaces you're still a member of. Summaries written more than 14 days " +
  "ago aren't rewritten, and a workspace you've left can't be reached — you'll be shown exactly " +
  "what each retract did reach.";
