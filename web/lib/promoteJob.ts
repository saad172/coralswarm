// Pure decision logic for the async promote-into-ocean flow
// (`PromoteModal.tsx`'s "reef" target — one reef, per-coral picker, the exact
// path that timed out on a 3,129-coral reef before feat/promote-async-batch).
//
// Split out for the same reason lib/teamEvents.ts, lib/activeOcean.ts and
// friends are: this project's vitest setup runs a Node environment with no
// DOM (vitest.config.ts) and deliberately has no component-render harness
// (see PromoteModal.tsx's own file-level comment) — so anything worth unit
// testing has to be a plain function of plain data, not a mounted component.
// `PromoteModal.tsx`, `lib/useJobPoll.ts` and `lib/promoteJobsProvider.tsx`
// are thin wiring around the functions below; the actual decisions live
// here, where they're tested.
//
// Ownership note (persistent indicator, second half of this feature): a
// single "reef" promote's per-ocean legs used to be owned entirely by
// `PromoteModal`'s own React state, polled by its own `useJobPoll` call, and
// mirrored to storage only so a REMOUNT of that same modal could resume.
// That meant closing the modal — the ordinary way anyone dismisses it — tore
// the poll down along with it: nothing survived to show a passive indicator
// elsewhere, and a job that finished while the modal was closed produced no
// signal anywhere until the user manually reopened Promote on that exact
// reef. `PromoteBatch`/`PromoteBatchMap` below and `lib/promoteJobsProvider
// .tsx` are the fix: the batch (and its poll) now lives in a provider
// mounted once in `AppShell`'s layout, which outlives any one modal mount —
// `PromoteModal` is a CONSUMER of that shared state now, not its owner.

import type { JobRunState, JobStatus, PublicationProcessing } from "./api";

// ── One target ocean's promote leg ──────────────────────────────────────────

/** One chosen ocean's outcome for a "reef" promote. A leg starts as either
 *  `succeeded` (no job — either `corals_requested` was 0, or the coral copy
 *  is otherwise already done) or `failed` (the `POST /promote` call itself
 *  rejected) or `job` (a background job is copying/linking corals and must be
 *  polled). A `job` leg transitions to `succeeded`/`failed` once
 *  `applyJobUpdate`/`applyJobPollError` sees a terminal status — it never
 *  goes back.
 *
 *  `targetNodeId` is the destination reef's own `node_id` — promotion mints
 *  a NEW node id for the copy in the target ocean (see
 *  `backend/src/store/mod.rs`'s `ON CONFLICT (graph_id,
 *  promoted_from_node_id)` insert), it is never the same id as the reef
 *  being promoted. It's `undefined` in exactly two cases, both of which
 *  must render as "no link", never fall back to the source id: (1) a
 *  `failed` leg, where the `POST /promote` call itself never returned one,
 *  and (2) a leg rehydrated from a pre-this-field persisted batch (see
 *  `StoredLeg`/`fromStoredLeg` below). A `job` leg already knows its
 *  `targetNodeId` (the destination reef row is created synchronously; only
 *  the coral copy is a background job) but callers must still treat a
 *  `job` leg as unlinkable — see `batchLinkTargetNodeId` — since landing on
 *  the destination topic before its corals finish copying renders a
 *  confident "no corals" empty state right next to a still-spinning
 *  progress meter. */
export type PromoteLeg =
  | { oceanId: string; oceanName: string; itemId?: string; itemTitle?: string; phase: "succeeded"; coralsRequested: number; targetNodeId?: string }
  | { oceanId: string; oceanName: string; itemId?: string; itemTitle?: string; phase: "failed"; error: string | null }
  | {
      oceanId: string;
      oceanName: string; itemId?: string; itemTitle?: string;
      phase: "job";
      jobId: string;
      processing?: PublicationProcessing;
      pollUnavailable?: boolean;
      status: JobRunState;
      current: number | null;
      total: number | null;
      /** A transient `error_message` a `running` job reported before its last
       *  update — kept only for display; a `job` leg with this set is NOT
       *  terminal (only `failed`/`dead` `status` values retire the leg). */
      error: string | null;
      /** See the type-level doc comment above — carried forward into the
       *  `succeeded` leg `applyJobUpdate` produces once the job lands. */
      targetNodeId?: string;
    };

/** What one `POST /v1/nodes/{id}/promote` call settled to, already reduced to
 *  the facts {@link legFromPromoteResult} needs — keeps that function
 *  (and its tests) free of `ApiError`/`Promise` mechanics. `targetNodeId` is
 *  the backend's `PromoteResponse.target_node_id` (the destination reef's
 *  own, newly-minted node id) — optional here only so existing fixtures in
 *  this file's tests that predate this field keep compiling; a real call
 *  site (`PromoteModal.confirmReef`) always has one to pass on success. */
export type PromoteCallResult =
  | { ok: true; jobId: string | null; coralsRequested: number; targetNodeId?: string }
  | { ok: false; error: string | null };

/** The first transition: what a promote leg looks like the instant
 *  `POST /promote` settles, before any polling has happened. */
export function legFromPromoteResult(
  oceanId: string,
  oceanName: string,
  result: PromoteCallResult,
): PromoteLeg {
  if (!result.ok) return { oceanId, oceanName, phase: "failed", error: result.error };
  if (result.jobId == null) {
    return { oceanId, oceanName, phase: "succeeded", coralsRequested: result.coralsRequested, targetNodeId: result.targetNodeId };
  }
  return {
    oceanId,
    oceanName,
    phase: "job",
    jobId: result.jobId,
    status: "pending",
    current: null,
    total: null,
    error: null,
    targetNodeId: result.targetNodeId,
  };
}

/** Apply one `GET /v1/jobs/{id}` poll response to a leg. No-ops (returns
 *  `leg` unchanged) for a leg that isn't `phase: "job"` — a poll response for
 *  an already-terminal leg is stale and must never resurrect it (the poll
 *  loop only polls non-terminal legs, but this keeps the function safe to
 *  call unconditionally). */
export function applyJobUpdate(leg: PromoteLeg, res: JobStatus): PromoteLeg {
  if (leg.phase !== "job") return leg;
  if (res.processing?.state === "attention") {
    return { oceanId: leg.oceanId, oceanName: leg.oceanName, itemId: leg.itemId, itemTitle: leg.itemTitle, phase: "failed", error: "Evidence was copied, but some processing needs attention. Retry to finish." };
  }
  if (res.status === "done" && res.processing?.state === "processing") {
    return { ...leg, status: "running", current: res.progress_current, total: res.progress_total, processing: res.processing, pollUnavailable: false, error: null };
  }
  if (res.status === "done") {
    // `progress_total` is the best available count of what the job copied —
    // falls back to 0 (not `current`) so an indeterminate job that finished
    // without ever reporting a total doesn't fabricate a specific number.
    // `targetNodeId` carries forward from the `job` leg unchanged — this is
    // the only place a `job` leg's destination becomes link-eligible (see
    // `batchLinkTargetNodeId`), now that the leg is actually terminal.
    return {
      oceanId: leg.oceanId,
      oceanName: leg.oceanName, itemId: leg.itemId, itemTitle: leg.itemTitle,
      phase: "succeeded",
      coralsRequested: res.progress_total ?? 0,
      targetNodeId: leg.targetNodeId,
    };
  }
  if (res.status === "failed" || res.status === "dead") {
    return { oceanId: leg.oceanId, oceanName: leg.oceanName, itemId: leg.itemId, itemTitle: leg.itemTitle, phase: "failed", error: res.error_message };
  }
  return { ...leg, status: res.status, current: res.progress_current, total: res.progress_total, error: res.error_message, processing: res.processing, pollUnavailable: false };
}

/** Apply a failed poll attempt (network error, or the job endpoint itself
 *  erroring) to a leg. A 404 is a genuine terminal state per the contract —
 *  "a wrong-account or nonexistent job id returns 404 — handle that as a
 *  terminal error state ... don't retry-poll forever" — everything else is
 *  treated as transient and the leg is left untouched so the next tick tries
 *  again (mirrors `meeting/page.tsx`'s poll effect: "transient — keep
 *  polling; a persistent failure will surface via load"). */
export function applyJobPollError(leg: PromoteLeg, status: number | undefined): PromoteLeg {
  if (leg.phase !== "job") return leg;
  if (status === 404) {
    return {
      oceanId: leg.oceanId,
      oceanName: leg.oceanName, itemId: leg.itemId, itemTitle: leg.itemTitle,
      phase: "failed",
      error: "This promote job could no longer be found — it may have been interrupted. Please retry.",
    };
  }
  return { ...leg, pollUnavailable: true };
}

export function isLegTerminal(leg: PromoteLeg): boolean {
  return leg.phase !== "job";
}

export function allLegsTerminal(legs: PromoteLeg[]): boolean {
  return legs.every(isLegTerminal);
}

/** Job ids still worth polling — i.e. every non-terminal leg's `jobId`. Feed
 *  this straight to `useJobPoll`; once every leg lands, this returns `[]` and
 *  the poll loop tears itself down. */
export function activeJobIds(legs: PromoteLeg[]): string[] {
  return legs.filter((l): l is Extract<PromoteLeg, { phase: "job" }> => l.phase === "job").map((l) => l.jobId);
}

/** 0-100 determinate progress for one `job` leg, or `null` for the
 *  indeterminate/spinner case — either bound absent, or a non-positive total
 *  (which would otherwise divide by zero or read as 100% of nothing). */
export function legProgressPct(leg: Extract<PromoteLeg, { phase: "job" }>): number | null {
  if (leg.pollUnavailable) return null;
  if (leg.processing?.state === "processing" && leg.processing.stages.some((s) => s.stage === "copying" && s.completed === s.total)) return null;
  if (leg.current == null || leg.total == null || leg.total <= 0) return null;
  return Math.max(0, Math.min(100, Math.round((leg.current / leg.total) * 100)));
}

// ── Aggregate outcome + copy ─────────────────────────────────────────────────

export interface PromoteOutcome {
  /** DISTINCT ocean names, in first-success order. A bulk promote has one leg
   *  per (ocean × reef) pair, so the raw leg names repeat an ocean once per
   *  reef — and every consumer of this field joins it into a sentence
   *  ("Promoted to Acme, Acme, Acme"). Counts live in `succeededLegs` /
   *  `failedLegs`, never in `.length` of these arrays (CodeRabbit review of
   *  #871). */
  succeededNames: string[];
  failedNames: string[];
  /** Number of succeeded/failed LEGS — (ocean × reef) pairs, not distinct
   *  oceans. This is what a bulk promote reports as its `reefs` figure. */
  succeededLegs: number;
  failedLegs: number;
  /** Total corals actually requested/copied across every succeeded leg —
   *  what the success confirmation counts. */
  coralsPromoted: number;
  allSucceeded: boolean;
  someSucceeded: boolean;
}

function distinct(names: string[]): string[] {
  return [...new Set(names)];
}

/** Reduce a (terminal) set of legs to the same succeeded/failed shape
 *  `PromoteModal`'s pre-async `confirm()` already built by hand — kept as a
 *  pure function so the "partial failure" copy below (and its tests) don't
 *  depend on when in the promote lifecycle it's called. Safe to call on a
 *  non-terminal set too (e.g. for a live in-progress summary), it just counts
 *  whatever has landed so far. */
export function summarizeLegs(legs: PromoteLeg[]): PromoteOutcome {
  const succeeded = legs.filter((l): l is Extract<PromoteLeg, { phase: "succeeded" }> => l.phase === "succeeded");
  const failed = legs.filter((l): l is Extract<PromoteLeg, { phase: "failed" }> => l.phase === "failed");
  return {
    succeededNames: distinct(succeeded.map((l) => l.oceanName)),
    failedNames: distinct(failed.map((l) => l.oceanName)),
    succeededLegs: succeeded.length,
    failedLegs: failed.length,
    coralsPromoted: succeeded.reduce((n, l) => n + l.coralsRequested, 0),
    allSucceeded: failed.length === 0,
    someSucceeded: succeeded.length > 0,
  };
}

/** The inline banner string for a (fully terminal) outcome with at least one
 *  failure — mirrors the exact wording `PromoteModal.confirm()` used before
 *  async promote existed, so a retry after a partial failure still reads the
 *  same. `null` when nothing failed (nothing to say). */
export function promoteFailureMessage(outcome: PromoteOutcome): string | null {
  if (outcome.failedNames.length === 0) return null;
  return outcome.someSucceeded
    ? `Promoted to ${outcome.succeededNames.join(", ")}. Couldn't promote to ${outcome.failedNames.join(", ")} — please retry.`
    : `Couldn't promote to ${outcome.failedNames.join(", ")} — please retry.`;
}

/** The success panel's confirmation sentence — calm, precise, ocean lexicon,
 *  no exclamation marks (the brand skill's voice rule) — for a fully-
 *  succeeded outcome. `itemLabel` is the reef's title. Zero corals (a
 *  reef-only promote, or a job that genuinely copied nothing) reads as "no
 *  corals" rather than "0 corals", matching this modal's existing "no corals
 *  in this reef — reef only" phrasing elsewhere. */
export function promoteSuccessMessage(itemLabel: string, outcome: PromoteOutcome): string {
  const to = outcome.succeededNames.join(", ");
  const corals =
    outcome.coralsPromoted === 0
      ? "no items"
      : `${outcome.coralsPromoted.toLocaleString()} item${outcome.coralsPromoted === 1 ? "" : "s"}`;
  return `Promoted ${itemLabel} to ${to} — ${corals} travelled with it.`;
}

export const PROCESSING_STAGE_ORDER = ["copying", "building", "checking"] as const;
export type ProcessingStageName = (typeof PROCESSING_STAGE_ORDER)[number];

/** One `job` leg's status line under its progress bar — "1,240 of 3,129
 *  corals" once the job reports a granular total, else a state-appropriate
 *  placeholder for the indeterminate case (never "null of null"). */
export function legStatusLabel(leg: Extract<PromoteLeg, { phase: "job" }>): string {
  if (leg.pollUnavailable) return "Status unavailable · reconnecting…";
  if (leg.processing) {
    const stage = PROCESSING_STAGE_ORDER.map((name) => leg.processing?.stages.find((s) => s.stage === name && s.completed < s.total)).find(Boolean);
    if (stage) return `${stage.stage === "copying" ? "Copying evidence" : stage.stage === "building" ? "Building topics and relationships" : "Running follow-up checks"} · ${stage.completed} / ${stage.total} jobs`;
  }
  if (leg.current != null && leg.total != null && leg.total > 0) {
    return `${leg.current.toLocaleString()} of ${leg.total.toLocaleString()} items`;
  }
  return leg.status === "pending" ? "queued…" : "copying items…";
}

/** The word on the right of a progress card — same vocabulary the promoting
 *  panel and the expanded header pill share, so they never disagree. `null`
 *  is the instant after Share is clicked and no leg exists yet. */
export function legPhaseWord(leg: PromoteLeg | null): string {
  if (leg === null) return "starting…";
  if (leg.phase === "succeeded") return "✓ done";
  if (leg.phase === "failed") return "✗ failed";
  return leg.status === "pending" ? "queued" : "in progress";
}

/** Count line under a succeeded leg — "no corals" rather than "0 corals",
 *  matching `promoteSuccessMessage`. */
export function succeededCoralLabel(count: number): string {
  return count === 0 ? "no items" : `${count.toLocaleString()} item${count === 1 ? "" : "s"}`;
}

/** Ocean name, plus the reef title when a bulk promote fans one ocean out
 *  across several topics. */
export function promoteLegHeading(oceanName: string, itemTitle?: string): string {
  return itemTitle ? `${oceanName} · ${itemTitle}` : oceanName;
}

/** A single-reef batch already names the topic above the cards, and every
 *  leg carries that same `itemTitle` (PromoteModal stamps it on every POST).
 *  Repeating it on the card (`Coralswarm Team · Q3 roadmap…`) truncates the
 *  ocean and adds nothing. Only keep the per-leg title when it differs from
 *  the batch title — i.e. a bulk promote of several topics. */
export function promoteCardItemTitle(batchTitle: string, itemTitle?: string): string | undefined {
  if (!itemTitle || itemTitle === batchTitle) return undefined;
  return itemTitle;
}

export function processingStageTitle(stage: ProcessingStageName): string {
  return stage === "copying"
    ? "Copy evidence"
    : stage === "building"
      ? "Build topics and relationships"
      : "Check and resolve relationships";
}

/** One row of the processing-stage list under a still-running job —
 *  "✓ Copy evidence: 1/1" or "◦ Build topics and relationships: 1/2 · 1 needed no changes". */
export function processingStageLine(stage: PublicationProcessing["stages"][number]): string {
  const mark = stage.completed === stage.total ? "✓" : "◦";
  const skip = stage.skipped ? ` · ${stage.skipped} needed no changes` : "";
  return `${mark} ${processingStageTitle(stage.stage)}: ${stage.completed}/${stage.total}${skip}`;
}

/** Which panel `PromoteModal` shows for a "reef" target: the picker
 *  (`idle`), a per-ocean progress list (`promoting`, including the moment
 *  right after Share is clicked and no leg exists yet), a success
 *  confirmation (`done`), or an error panel with retry (`error`). */
export type PromotePanelPhase = "idle" | "promoting" | "done" | "error";

export function promotePanelPhase(legs: PromoteLeg[] | null, submitting: boolean): PromotePanelPhase {
  if (submitting) return "promoting";
  if (!legs || legs.length === 0) return "idle";
  if (!allLegsTerminal(legs)) return "promoting";
  return summarizeLegs(legs).allSucceeded ? "done" : "error";
}

// ── Poll cadence ─────────────────────────────────────────────────────────────
//
// "A few times a second" is too aggressive for a job that can legitimately
// run for several minutes on thousands of corals, so this steps DOWN in
// frequency the longer a job has been running — starting responsive (a
// promote of a handful of corals should feel close to instant) and backing
// off once it's clear this is a longer haul. Deliberately uncapped in the
// "give up" sense (contrast `meeting/page.tsx`'s `SUMMARY_POLL_CAP`): a
// promote job on thousands of corals is exactly the case this feature exists
// to fix, and abandoning the poll while the server-side job is still quietly
// succeeding would recreate the "no idea what happened" UX the async
// rewrite was for. Backing off keeps a long poll cheap instead.
export const PROMOTE_POLL_INITIAL_MS = 1500;
export const PROMOTE_POLL_STEP_MS = 3000;
export const PROMOTE_POLL_LONG_MS = 6000;
/** Elapsed-ms thresholds at which the interval steps up. */
export const PROMOTE_POLL_STEP_AFTER_MS = 30_000;
export const PROMOTE_POLL_LONG_AFTER_MS = 120_000;

export function nextPromotePollDelayMs(elapsedMs: number): number {
  if (elapsedMs >= PROMOTE_POLL_LONG_AFTER_MS) return PROMOTE_POLL_LONG_MS;
  if (elapsedMs >= PROMOTE_POLL_STEP_AFTER_MS) return PROMOTE_POLL_STEP_MS;
  return PROMOTE_POLL_INITIAL_MS;
}

// ── Resilience to navigating away and back ──────────────────────────────────
//
// `PromoteModal` is conditionally mounted (`{promoteTarget && <PromoteModal
// .../>}` in topics/page.tsx) — closing it, or navigating off the topic page
// entirely, unmounts it and loses whatever's only in React state. A
// thousands-of-corals job can run for minutes, well past the time someone
// reasonably keeps a modal pinned open, so the in-flight batch is mirrored to
// `localStorage` here and reloaded on mount: reopening the promote panel for
// the SAME reef resumes polling instead of re-offering the picker for a
// promote that's already running.
//
// `storage` is an injected `KVStorage` rather than a bare `localStorage`
// reference so this is testable under this project's Node-env vitest (no
// `window`) — `PromoteModal` passes `promoteJobLocalStorage()` (a thin
// `typeof window !== "undefined"` guard, the same idiom `getDeviceId` /
// `activeOcean.ts` already use for browser-only storage).
export interface KVStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

/** `KVStorage` plus enumeration — exactly what `window.localStorage`
 *  natively supports (`.length`/`.key(i)`), needed by
 *  `listPromoteBatchReefIds` to find every in-flight batch on provider boot
 *  without already knowing which reefs to look for. A plain `KVStorage` fake
 *  in a test can add these two members trivially; nothing else in this file
 *  needs them, which is why they're a separate, wider interface rather than
 *  added to `KVStorage` itself. */
export interface ListableKVStorage extends KVStorage {
  readonly length: number;
  key(index: number): string | null;
}

/** A `KVStorage` backed by `window.localStorage`, or `null` during SSR/build
 *  (no `window`) or in a Node-env test — callers must treat `null` as "no
 *  persistence available" and simply skip it, matching every other
 *  browser-storage call site in this codebase. Its return type is the wider
 *  `ListableKVStorage` because the real `localStorage` always satisfies it
 *  (it's just `Storage`'s own shape) — callers that only need `KVStorage`
 *  use it as that; `listPromoteBatchReefIds` needs the rest. */
export function promoteJobLocalStorage(): ListableKVStorage | null {
  if (typeof window === "undefined") return null;
  return window.localStorage;
}

const STORAGE_PREFIX = "cs:promoteJob:";

export function promoteJobStorageKey(reefNodeId: string): string {
  return `${STORAGE_PREFIX}${reefNodeId}`;
}

/** Every reef id with a stored (potentially in-flight) batch — how
 *  `PromoteJobsProvider` discovers what to resume on boot without already
 *  knowing which reefs someone was promoting. Order is whatever the storage
 *  enumerates in (unspecified by `Storage`); callers that care about display
 *  order sort by `startedAt`/similar themselves. Entries that turn out
 *  unparsable (see `loadPromoteBatch`) are still returned here — the caller
 *  finds that out (and drops them) via `loadPromoteBatch` itself, so this
 *  function doesn't have to parse every value just to enumerate keys. */
export function listPromoteBatchReefIds(storage: ListableKVStorage): string[] {
  const ids: string[] = [];
  for (let i = 0; i < storage.length; i++) {
    const key = storage.key(i);
    if (key && key.startsWith(STORAGE_PREFIX)) ids.push(key.slice(STORAGE_PREFIX.length));
  }
  return ids;
}

/** The subset of a `PromoteLeg` worth persisting — job legs need their
 *  `jobId` to resume polling; already-terminal legs need just enough to
 *  rebuild the aggregate outcome (name + succeeded/failed + count) without
 *  re-deriving anything from a live response that won't come again.
 *  `targetNodeId` is optional here purely because a v2 (pre-this-field)
 *  entry never wrote one — see `StoredPromoteBatch.v`'s doc comment, which
 *  drops those entries outright rather than resume a batch that could never
 *  produce a link. */
type StoredLeg =
  | { oceanId: string; oceanName: string; itemId?: string; itemTitle?: string; phase: "succeeded"; coralsRequested: number; targetNodeId?: string }
  | { oceanId: string; oceanName: string; itemId?: string; itemTitle?: string; phase: "failed"; error: string | null }
  | { oceanId: string; oceanName: string; itemId?: string; itemTitle?: string; phase: "job"; jobId: string; targetNodeId?: string };

interface StoredPromoteBatch {
  /** Schema guard — bumped if the shape below ever changes, so a stale entry
   *  from an older build is dropped instead of misread. v1 (no `reefTitle`)
   *  predates the persistent indicator, which needs a title to show without
   *  the reef page having ever loaded in this tab. v2 (no `targetNodeId` on
   *  any leg) predates the pill's link to the promoted-into topic — a v2
   *  entry's `succeeded`/`job` legs have no destination id at all, so
   *  resuming one could only ever render unlinked, and worse, a
   *  pre-this-fix v2 entry is exactly the shape that ships the ORIGINAL bug
   *  (a stale in-memory fallback to the source id) if some future edit ever
   *  loosens the "no targetNodeId → no link" rule. Rather than carry that
   *  shape forward and rely on every future reader getting the fallback
   *  right, it's simplest and safest to drop it, same as v1: the in-flight
   *  job itself is unaffected (it's on the server), only THIS tab's memory
   *  of it, and the next `POST /promote` on that reef starts a normal fresh
   *  v3 entry. */
  v: 3;
  reefNodeId: string;
  reefTitle: string;
  legs: StoredLeg[];
}

function toStoredLeg(leg: PromoteLeg): StoredLeg {
  if (leg.phase === "job") return { oceanId: leg.oceanId, oceanName: leg.oceanName, itemId: leg.itemId, itemTitle: leg.itemTitle, phase: "job", jobId: leg.jobId, targetNodeId: leg.targetNodeId };
  return leg;
}

/** Resuming a `job` leg from storage can't know its last-seen status/progress
 *  (that lived only in the React state that's now gone) — starts it back at
 *  the same "nothing known yet, first poll will tell us" state a brand-new
 *  `job` leg starts at. That's a strictly safe under-claim: the very next
 *  poll tick (immediate, see `useJobPoll`) replaces it with the real status.
 *  `targetNodeId` passes through unchanged (a `job` leg's destination id is
 *  already known and doesn't change on resume — only its poll progress
 *  does). */
function fromStoredLeg(leg: StoredLeg): PromoteLeg {
  if (leg.phase === "job") {
    return {
      oceanId: leg.oceanId,
      oceanName: leg.oceanName, itemId: leg.itemId, itemTitle: leg.itemTitle,
      phase: "job",
      jobId: leg.jobId,
      status: "pending",
      current: null,
      total: null,
      error: null,
      targetNodeId: leg.targetNodeId,
    };
  }
  return leg;
}

/** Persist the current batch of legs for `reefNodeId`, or clear the entry
 *  once every leg has landed (nothing left worth resuming — an empty/absent
 *  key is the "no in-flight promote" state `loadPromoteBatch` expects). */
export function savePromoteBatch(storage: KVStorage, reefNodeId: string, reefTitle: string, legs: PromoteLeg[]): void {
  const key = promoteJobStorageKey(reefNodeId);
  if (legs.length === 0 || allLegsTerminal(legs)) {
    storage.removeItem(key);
    return;
  }
  const payload: StoredPromoteBatch = { v: 3, reefNodeId, reefTitle, legs: legs.map(toStoredLeg) };
  storage.setItem(key, JSON.stringify(payload));
}

/** Read back an in-flight batch for `reefNodeId`, or `null` if there is none
 *  (nothing stored, the stored value doesn't parse, or it's a pre-indicator
 *  v1/v2 entry — see `StoredPromoteBatch.v`'s doc comment). */
export function loadPromoteBatch(storage: KVStorage, reefNodeId: string): { reefTitle: string; legs: PromoteLeg[] } | null {
  const raw = storage.getItem(promoteJobStorageKey(reefNodeId));
  if (!raw) return null;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (
      !parsed ||
      typeof parsed !== "object" ||
      (parsed as StoredPromoteBatch).v !== 3 ||
      typeof (parsed as StoredPromoteBatch).reefTitle !== "string" ||
      !Array.isArray((parsed as StoredPromoteBatch).legs)
    ) {
      return null;
    }
    const legs = (parsed as StoredPromoteBatch).legs.map(fromStoredLeg);
    return legs.length > 0 ? { reefTitle: (parsed as StoredPromoteBatch).reefTitle, legs } : null;
  } catch {
    return null;
  }
}

export function clearPromoteBatch(storage: KVStorage, reefNodeId: string): void {
  storage.removeItem(promoteJobStorageKey(reefNodeId));
}

// ── Multi-reef batch map (persistent indicator) ─────────────────────────────
//
// One entry per reef currently being promoted, keyed by `reefNodeId` — the
// same granularity `cs:promoteJob:<reefNodeId>` storage already uses, so two
// DIFFERENT reefs promoting concurrently (the multi-job case this indicator
// has to support) simply get two map entries and never collide. Two promotes
// of the SAME reef from two different browser tabs at once is a real but
// narrower case this map doesn't fully arbitrate: each tab keeps its own
// in-memory map and last-write-wins into the shared storage key, same as any
// other per-key localStorage race in this app — not solved here, and not new
// (the resumable-single-batch version of this file had the identical race).

export interface PromoteBatch {
  reefNodeId: string;
  reefTitle: string;
  legs: PromoteLeg[];
  /** Acknowledged off the passive indicator. Independent of what
   *  `PromoteModal` shows: reopening Promote on this reef always renders the
   *  real `legs`/phase regardless of `dismissed` — this only hides the
   *  batch from `visibleBatches` (the indicator's own read). Kept in the map
   *  rather than deleted so re-dismissing is idempotent and a batch that
   *  finished passively isn't immediately forgotten before anyone saw it. */
  dismissed: boolean;
}

export type PromoteBatchMap = Record<string, PromoteBatch>;

/** Start a fresh batch for `reefNodeId`, replacing anything already there —
 *  a retry always begins a brand-new batch (fresh jobs), never merges into a
 *  stale one, so "replace" rather than "merge" is the correct semantics here
 *  even when a batch for this reef already exists. */
export function upsertBatch(map: PromoteBatchMap, reefNodeId: string, reefTitle: string, legs: PromoteLeg[]): PromoteBatchMap {
  return { ...map, [reefNodeId]: { reefNodeId, reefTitle, legs, dismissed: false } };
}

export function removeBatch(map: PromoteBatchMap, reefNodeId: string): PromoteBatchMap {
  if (!(reefNodeId in map)) return map;
  const next = { ...map };
  delete next[reefNodeId];
  return next;
}

/** No-op (same reference back) if the batch is missing or already
 *  dismissed — lets a caller (an auto-dismiss timer that raced a manual
 *  dismiss, or a retry that removed the batch first) call this
 *  unconditionally without checking first. */
export function dismissBatch(map: PromoteBatchMap, reefNodeId: string): PromoteBatchMap {
  const b = map[reefNodeId];
  if (!b || b.dismissed) return map;
  return { ...map, [reefNodeId]: { ...b, dismissed: true } };
}

/** Route one `GET /v1/jobs/{id}` response to whichever batch/leg owns
 *  `jobId` — the multi-batch counterpart of `applyJobUpdate`. Returns `map`
 *  UNCHANGED (same reference) when no leg anywhere owns `jobId`, so a stray
 *  or late response (e.g. for a job whose batch was just cleared by a
 *  retry) can neither resurrect anything nor cause a needless re-render. */
export function applyJobUpdateToBatches(map: PromoteBatchMap, jobId: string, res: JobStatus): PromoteBatchMap {
  for (const b of Object.values(map)) {
    const idx = b.legs.findIndex((l) => l.phase === "job" && l.jobId === jobId);
    if (idx === -1) continue;
    const legs = b.legs.slice();
    legs[idx] = applyJobUpdate(legs[idx], res);
    return { ...map, [b.reefNodeId]: { ...b, legs } };
  }
  return map;
}

/** The multi-batch counterpart of `applyJobPollError` — same no-op-on-miss
 *  contract as {@link applyJobUpdateToBatches}. */
export function applyJobPollErrorToBatches(map: PromoteBatchMap, jobId: string, status: number | undefined): PromoteBatchMap {
  for (const b of Object.values(map)) {
    const idx = b.legs.findIndex((l) => l.phase === "job" && l.jobId === jobId);
    if (idx === -1) continue;
    const legs = b.legs.slice();
    legs[idx] = applyJobPollError(legs[idx], status);
    return { ...map, [b.reefNodeId]: { ...b, legs } };
  }
  return map;
}

/** Every job id worth polling, across every batch — feed straight to
 *  `useJobPoll`. Empties out as legs land, same shrink-to-teardown contract
 *  `activeJobIds` documents for one batch. */
export function allBatchJobIds(map: PromoteBatchMap): string[] {
  return Object.values(map).flatMap((b) => activeJobIds(b.legs));
}

/** Batches the indicator should actually render — every one not dismissed. */
export function visibleBatches(map: PromoteBatchMap): PromoteBatch[] {
  return Object.values(map).filter((b) => !b.dismissed);
}

export type BatchPhase = "promoting" | "done" | "error";

export function batchPhase(batch: PromoteBatch): BatchPhase {
  if (!allLegsTerminal(batch.legs)) return "promoting";
  return summarizeLegs(batch.legs).allSucceeded ? "done" : "error";
}

/** The one destination reef `PromoteJobsIndicator`'s row should link to, or
 *  `null` when there isn't exactly one — a promote fires one leg per chosen
 *  target ocean, so a batch with two or more succeeded legs has two or more
 *  equally-valid destinations and no single "the" topic to send a click to;
 *  a batch with zero succeeded legs (still promoting, every leg failed, or a
 *  legacy persisted leg with no `targetNodeId` — see `StoredLeg`) has none.
 *  Only a `succeeded` leg's `targetNodeId` is ever a candidate: a `job` leg
 *  already knows its destination id but its corals are still copying (see
 *  `PromoteLeg`'s doc comment) and a `failed` leg never got one — this is
 *  the terminal-and-actually-succeeded gate `PromoteJobsIndicator` needs to
 *  never link mid-job or to a dead attempt.
 *
 *  The "exactly one" count MUST be taken over succeeded legs, never over
 *  known ids: two legs can succeed while only one of them carries a
 *  `targetNodeId` (a legacy persisted leg missing the field is exactly this
 *  — see `StoredLeg`), and counting ids alone would then see "one
 *  candidate" and confidently link to it, even though a second ocean also
 *  succeeded and this row isn't uniquely about the one with a known id. A
 *  succeeded leg with no id must make the batch LESS linkable, never
 *  invisible to the count. */
export function batchLinkTargetNodeId(batch: PromoteBatch): string | null {
  const succeeded = batch.legs.filter(
    (l): l is Extract<PromoteLeg, { phase: "succeeded" }> => l.phase === "succeeded",
  );
  return succeeded.length === 1 ? (succeeded[0].targetNodeId ?? null) : null;
}

/** Summed progress across every DETERMINATE still-running leg in a batch
 *  (one per chosen ocean) — `null` when none of them have reported a
 *  current/total yet, so the indicator can fall back to an indeterminate
 *  presentation instead of claiming "0 of 0". A leg that already landed
 *  (succeeded/failed) contributes nothing further — it's done moving. */
export function batchProgressCounts(batch: PromoteBatch): { current: number; total: number } | null {
  const determinate = batch.legs.filter(
    (l): l is Extract<PromoteLeg, { phase: "job" }> => l.phase === "job" && legProgressPct(l) != null && l.current != null && l.total != null && l.total > 0,
  );
  if (determinate.length === 0) return null;
  return {
    current: determinate.reduce((n, l) => n + (l.current ?? 0), 0),
    total: determinate.reduce((n, l) => n + (l.total ?? 0), 0),
  };
}

export function batchProgressPct(batch: PromoteBatch): number | null {
  const counts = batchProgressCounts(batch);
  if (!counts || counts.total <= 0) return null;
  return Math.max(0, Math.min(100, Math.round((counts.current / counts.total) * 100)));
}

/** The pill's label for ONE batch, across all three visible phases —
 *  "Promoting …" with a live count once any leg reports one, a plain verb
 *  otherwise (every leg still indeterminate), and the same success/failure
 *  wording family as `promoteSuccessMessage`/`promoteFailureMessage` use
 *  inside the modal, so the pill and the modal never disagree about what
 *  happened. */
export function batchIndicatorLabel(batch: PromoteBatch): string {
  const phase = batchPhase(batch);
  if (phase === "done") return `Promoted ${batch.reefTitle}`;
  if (phase === "error") return `Couldn't promote ${batch.reefTitle}`;
  const counts = batchProgressCounts(batch);
  return counts ? `Promoting ${batch.reefTitle}… ${counts.current.toLocaleString()} / ${counts.total.toLocaleString()}` : `Promoting ${batch.reefTitle}…`;
}

/** The summary pill's label when more than one batch is visible at once —
 *  "2 promoting · 1 done", omitting any phase with a zero count. */
export function indicatorSummaryLabel(batches: PromoteBatch[]): string {
  const counts: Record<BatchPhase, number> = { promoting: 0, done: 0, error: 0 };
  for (const b of batches) counts[batchPhase(b)] += 1;
  const words: Record<BatchPhase, string> = { promoting: "promoting", done: "done", error: "failed" };
  return (["promoting", "done", "error"] as BatchPhase[])
    .filter((p) => counts[p] > 0)
    .map((p) => `${counts[p]} ${words[p]}`)
    .join(" · ");
}

/** The collapsed pill's visible text. A single still-promoting batch drops
 *  the live count here — it does not fit next to the meter and chevron, and
 *  the expanded panel is where the count actually belongs. Done/error keep
 *  the full sentence. Several batches collapse to the phase summary. */
export function indicatorTriggerLabel(batches: PromoteBatch[]): string {
  if (batches.length !== 1) return indicatorSummaryLabel(batches);
  const b = batches[0];
  const phase = batchPhase(b);
  if (phase === "promoting") return `Promoting ${b.reefTitle}…`;
  return batchIndicatorLabel(b);
}

/** Worst-first phase across every visible batch — what the collapsed pill's
 *  status dot reports. Promoting wins over error over done: a still-running
 *  job is the thing the pill exists to surface. */
export function indicatorSummaryPhase(batches: PromoteBatch[]): BatchPhase {
  if (batches.some((b) => batchPhase(b) === "promoting")) return "promoting";
  if (batches.some((b) => batchPhase(b) === "error")) return "error";
  return "done";
}

/** Accessible name for the expand control. Uses the full
 *  `batchIndicatorLabel` (count included) even when the visible trigger
 *  dropped it, so a screen reader still hears the live progress. */
export function indicatorExpandAriaLabel(batches: PromoteBatch[]): string {
  const summary = batches.length === 1 ? batchIndicatorLabel(batches[0]) : indicatorSummaryLabel(batches);
  const anyPromoting = batches.some((b) => batchPhase(b) === "promoting");
  return `${summary}. ${anyPromoting ? "Expand for progress." : "Expand for details."}`;
}

/** How long a fully-succeeded batch stays on the indicator before quietly
 *  dismissing itself. A failure never auto-dismisses — see
 *  `PromoteJobsProvider`'s scheduling effect — because a failure is
 *  actionable (Retry) and disappearing on its own would bury the one signal
 *  that anything needs attention; a success is not actionable, so lingering
 *  serves no purpose past "yes, that happened", and closes the loop this
 *  whole feature exists for without turning into one more thing to dismiss. */
export const PROMOTE_INDICATOR_AUTO_DISMISS_MS = 6000;
