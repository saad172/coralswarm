"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { invalidate, invalidationFor } from "@/lib/appQueries";
import {
  api,
  ApiError,
  type Reef,
  type ReefAtom,
  type ReefDigest,
  type Graph,
} from "@/lib/api";
import { Button, Chip, Dot, ErrorNote, Modal, Notice, SearchInput, workspaceHue, type Hue } from "@/components/ui";
import { IconAsk } from "@/components/icons";
import { srcName, fmtDate, isRawSlackUserId } from "@/lib/coral";
import { isOwnerOnlyOcean } from "@/lib/askScope";
import {
  SYNC_TAKE_IT_BACK_AFTER,
  SYNC_TAKE_IT_BACK_BEFORE,
  SYNC_TAKE_IT_BACK_LABEL,
  SYNC_TAKE_IT_BACK_NOTE,
} from "@/lib/retraction";
import {
  allLegsTerminal,
  legFromPromoteResult,
  promoteFailureMessage,
  promotePanelPhase,
  promoteSuccessMessage,
  summarizeLegs,
  type PromoteCallResult,
  type PromoteLeg,
} from "@/lib/promoteJob";
import { SourcePromotionModal } from "./SourcePublication";
import { PromoteProgressList } from "./PromoteProgressList";
import { usePromoteJobs } from "@/lib/promoteJobsProvider";

/* ════════════════════════════════════════════════════════════════════════
   The one promote-to-ocean modal (ADR-0006), shared by every promotion entry
   point in the app (reef detail, reefs workbench row/peek/bulk-bar/raw
   material source groups, coral detail). Previously four call sites each
   grew their own divergent flavor of this (a full modal, a source×ocean
   button grid, an inline `<select>` + fire, a bare button grid) — this file
   is the single canonical implementation; every other copy has been deleted
   in favor of it.

   The helpers below (`atomLabel`, `SourceIcon`/`iconKindFor`/
   `hueFor`) are local copies, deliberately NOT re-exported for other pages to
   import — this reverses (for this component's own needs only) an earlier
   handoff constraint against sharing UI helpers between the in-flight reef
   pages. Pages that already have their own copies of these (e.g. the reef
   detail page's coral timeline) keep them; nothing here refactors those.
   ════════════════════════════════════════════════════════════════════════ */

/** What's being promoted. A single reef gets a selectable per-coral picker
 *  (all corals checked by default, live "N of M" summary, zero selected =
 *  reef-only promote); a bulk selection of reefs or a set of source instances
 *  get a simpler read-only list — see the per-kind content render below. */
export type PromoteTarget =
  | { kind: "reef"; reef: Reef; atoms: ReefAtom[]; digest?: ReefDigest | null; previewByAtomId?: Map<string, string> }
  | { kind: "reefs"; reefs: Reef[] }
  | { kind: "sources"; sources: { id: string; label: string }[]; graphId: string }
  | { kind: "session"; sessionId: string; label: string; coralCount: number; graphId: string };

export interface PromoteModalProps {
  target: PromoteTarget;
  oceans: Graph[];
  currentGraphId: string | undefined;
  token: string | null;
  /** Unused since the 2026-07 redesign (the kit's `Modal` has no top-border
   *  accent slot) — kept optional so existing callers (e.g. `topics/page.tsx`
   *  passing `REEF_META[reef.type].hue`) don't need an unrelated edit. */
  accentHue?: Hue;
  /** Fired on every dismissal path (Cancel, X, Escape, backdrop, and a fully-
   *  successful promote). `completed` is `true` only for the latter — the
   *  modal closing itself because every chosen ocean succeeded — and `false`/
   *  omitted for every user-initiated cancel. Callers that key follow-up
   *  state (e.g. clearing a bulk selection) on "did the promote actually
   *  happen" must branch on this flag rather than assuming any `onClose` call
   *  means success. */
  onClose: (completed?: boolean) => void;
  /** Fired with the oceans that actually succeeded (may be a subset of what
   *  was attempted, on a partial failure) and a summary of what moved.
   *  `summary` fields are populated per target kind: `corals` for a single
   *  reef, `reefs` for a bulk reef promote, `reefs`+`corals` for a source
   *  promote (aggregated across every promoted source). */
  onPromoted: (
    oceanNames: string[],
    summary: {
      reefs?: number;
      corals?: number;
      sources?: number;
      /** Session promotes only: whether the `coding_sessions` container row
       *  itself travelled. `false` on a cross-plane (org-tenant) promote,
       *  where the backend copies reefs and their corals but cannot move the
       *  container — so the caller must NOT claim the session was published
       *  (CodeRabbit review). `true` only when EVERY succeeding ocean got the
       *  container; a mixed result reports the weaker truth. */
      sessionCopied?: boolean;
    },
  ) => void;
}

/* Display name for a participant — unresolved participants come through as
   raw Slack ids and must never render (mirrors the reef detail page). */
function participantDisplay(p: string): string {
  return isRawSlackUserId(p) ? "Unknown participant" : p;
}

/* Source hues follow the design-system semantics: amber = Slack sources,
   teal = meetings/ingested, plum = systems (MCP, Notion), pink = people. */
const SOURCE_HUE: Record<string, Hue> = {
  slack: "amber",
  email: "pink",
  note: "coral",
  transcript: "teal",
  zoom: "teal",
  meet: "teal",
  notion: "plum",
  mcp: "plum",
  coding_session: "plum",
};
const hueFor = (s: string): Hue => SOURCE_HUE[s] ?? "teal";

type SourceIconKind = "slack" | "meeting" | "doc" | "email" | "note" | "code" | "default";
const SRC_ICON: Record<string, SourceIconKind> = {
  slack: "slack",
  email: "email",
  transcript: "meeting",
  zoom: "meeting",
  meet: "meeting",
  note: "note",
  notion: "doc",
  mcp: "doc",
  coding_session: "code",
};
const iconKindFor = (t: string): SourceIconKind => SRC_ICON[t] ?? "default";

/** Tiny line icon per source type (24-grid, 1.7 stroke) — local copy, see
 *  the file-level doc comment above. */
function SourceIcon({ kind, size = 15 }: { kind: SourceIconKind; size?: number }) {
  const s = {
    width: size,
    height: size,
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 1.7,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
  };
  switch (kind) {
    case "slack":
      return (
        <svg {...s}>
          <path d="M9 3v9M9 3a2 2 0 1 0 0 4M15 21v-9M15 21a2 2 0 1 0 0-4M3 15h9M3 15a2 2 0 1 0 4 0M21 9h-9M21 9a2 2 0 1 0-4 0" />
        </svg>
      );
    case "meeting":
      return (
        <svg {...s}>
          <rect x="3" y="5" width="13" height="14" rx="2" />
          <path d="m16 10 5-3v10l-5-3z" />
        </svg>
      );
    case "doc":
      return (
        <svg {...s}>
          <path d="M7 3h7l4 4v14H7z" />
          <path d="M14 3v4h4M10 13h6M10 17h6" />
        </svg>
      );
    case "email":
      return (
        <svg {...s}>
          <rect x="3" y="5" width="18" height="14" rx="2" />
          <path d="m3 7 9 6 9-6" />
        </svg>
      );
    case "note":
      return (
        <svg {...s}>
          <path d="M5 4h14v12l-5 5H5z" />
          <path d="M14 21v-5h5" />
        </svg>
      );
    case "code":
      return (
        <svg {...s}>
          <path d="m8 8-4 4 4 4M16 8l4 4-4 4M13.5 5l-3 14" />
        </svg>
      );
    default:
      return (
        <svg {...s}>
          <circle cx="12" cy="12" r="9" />
        </svg>
      );
  }
}

/** One coral's presentation in the single-reef promote picker: source
 *  icon/hue, a body line, and a mono provenance line. `ReefAtom` carries no
 *  preview text of its own, so the body falls back through three tiers: the
 *  LLM digest's per-coral excerpt/contribution (richest), then the
 *  caller-supplied ocean-wide coral preview map (real inline content when
 *  the atom has any), then a neutral type label as the last resort. */
function atomLabel(
  a: ReefAtom,
  digest: ReefDigest | null | undefined,
  previewByAtomId: Map<string, string>,
): { icon: SourceIconKind; hue: Hue; body: string; meta: string } {
  const hue = hueFor(a.source_type);
  const icon = iconKindFor(a.source_type);
  const d = digest?.excerpts.find((e) => e.atom_id === a.atom_id);
  const body = d?.contribution || d?.excerpt || previewByAtomId.get(a.atom_id) || `${srcName(a.source_type)} item`;
  const person = a.participants[0];
  const who = person ? participantDisplay(person) : null;
  const meta = [who, srcName(a.source_type), fmtDate(a.timestamp)].filter(Boolean).join(" · ");
  return { icon, hue, body, meta };
}

function targetsOf(oceans: Graph[], currentGraphId: string | undefined): Graph[] {
  return oceans.filter((g) => (g.kind === "team" || g.kind === "org") && g.id !== currentGraphId);
}

function headerFor(target: PromoteTarget): { title: string; description: string; itemName: string | null } {
  if (target.kind === "reef") {
    return { title: "Promote this topic", description: "Publish", itemName: target.reef.title };
  }
  if (target.kind === "reefs") {
    return {
      title: `Promote ${target.reefs.length} topic${target.reefs.length === 1 ? "" : "s"}`,
      description: `Publish ${target.reefs.length} topic${target.reefs.length === 1 ? "" : "s"} into a shared workspace.`,
      itemName: null,
    };
  }
  if (target.kind === "session") {
    return {
      title: "Promote this coding session",
      // Just the verb: with `itemName` set, the shared renderer emits
      // "{description} {itemName} into a shared ocean." — a description
      // carrying its own destination clause reads as two of them (CodeRabbit
      // review). What travels is spelled out in the session content block
      // below instead, where there's room for it.
      description: "Publish",
      itemName: target.label,
    };
  }
  return {
    title: target.sources.length === 1 ? "Promote this source" : `Promote ${target.sources.length} sources`,
    description: `Publish ${target.sources.length === 1 ? "this source" : `${target.sources.length} sources`} into a shared workspace.`,
    itemName: null,
  };
}

export function PromoteModal(props: PromoteModalProps) {
  return props.target.kind === "sources" ? <SourcePromotionModal {...props} /> : <TopicPromoteModal {...props} />;
}
function TopicPromoteModal({ target, oceans, currentGraphId, token, onClose, onPromoted }: PromoteModalProps) {
  const queryClient = useQueryClient();
  const [chosenOceans, setChosenOceans] = useState<string[]>([]);
  const [coralQ, setCoralQ] = useState("");
  const [promoting, setPromoting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Per-coral selection for a single-reef promote, keyed by atom id (not by
  // filtered-list index) so it survives the search box narrowing/widening the
  // visible rows. All corals start selected — that's the widening-friendly
  // default and matches the omitted-`coral_ids` = "copy everything" contract.
  const [selectedCoralIds, setSelectedCoralIds] = useState<Set<string>>(
    () => new Set(target.kind === "reef" ? target.atoms.map((a) => a.atom_id) : []),
  );
  // Re-seed the selection if this mounted instance is handed a different
  // target (call sites conditionally mount today, but nothing enforces that).
  // Keyed on the target's stable identity, not the object reference, so a
  // parent re-render passing a fresh literal can't wipe an in-progress pick.
  const targetIdentity =
    target.kind === "reef"
      ? `reef:${target.reef.node_id}`
      : target.kind === "reefs"
        ? `reefs:${target.reefs.map((r) => r.node_id).join(",")}`
        : target.kind === "session"
          ? `session:${target.sessionId}`
          : `sources:${target.sources.map((s) => s.id).join(",")}`;
  const seededFor = useRef(targetIdentity);
  useEffect(() => {
    if (seededFor.current === targetIdentity) return;
    seededFor.current = targetIdentity;
    setSelectedCoralIds(new Set(target.kind === "reef" ? target.atoms.map((a) => a.atom_id) : []));
  }, [targetIdentity, target]);

  // Focus trap, autofocus and Escape-to-close are handled by the kit's
  // <Modal> now — see the render below. `masterCoralCheckboxRef` is unrelated
  // to that (it drives the select-all checkbox's indeterminate state).
  const masterCoralCheckboxRef = useRef<HTMLInputElement>(null);

  // ── Async promote (feat/promote-async-batch), "reef" target only ─────────
  // `reefs`/`session`/`sources` promote through different, still-synchronous
  // endpoints (`promoteSession`/`promoteSource`) unaffected by that PR — they
  // keep using `promoting`/`error`/`confirm()` below exactly as before. Only
  // `api.reefs.promote` (the "reef" kind's per-coral picker — the endpoint
  // that timed out on a 3,129-coral reef) can now hand back a `job_id`, so
  // only this target kind needs the promoting/progress machinery.
  //
  // The batch itself — legs, persistence, polling — is NOT owned here
  // anymore (see lib/promoteJobsProvider.tsx's file-level comment for why:
  // this modal used to own it, which meant closing the modal killed the
  // poll). This component is a CONSUMER of the shared provider now: it reads
  // the current batch for its own reef, fires the `POST /promote` calls and
  // hands the result to `startBatch`, and otherwise just renders whatever
  // the provider says. `reefSubmitting` is the one bit still local — the
  // narrow "clicked Share, calls in flight, no batch yet" window before
  // there's anything to hand the provider.
  const reefNodeId = target.kind === "reef" ? target.reef.node_id : target.kind === "reefs" ? targetIdentity : null;
  const { batches, startBatch, clearBatch } = usePromoteJobs();
  const legs = reefNodeId ? batches[reefNodeId]?.legs ?? null : null;
  const [reefSubmitting, setReefSubmitting] = useState(false);
  const reefPhase = reefNodeId ? promotePanelPhase(legs, reefSubmitting) : "idle";

  // Report the outcome exactly once per attempt, the instant every leg lands
  // terminal — whether that's moments after a fresh submit or the very first
  // render after the provider hands this mount an already-finished batch
  // (resumed from storage, or one that finished while this modal was
  // closed). `attemptId` (bumped on submit) is what makes "once per
  // attempt" well-defined; without it, re-deriving "already reported" from
  // `legs` alone can't tell a freshly-terminal batch from one already
  // reported on a prior render.
  const [attemptId, setAttemptId] = useState(0);
  const reportedAttemptRef = useRef(-1);
  useEffect(() => {
    if (!reefNodeId || !legs || !allLegsTerminal(legs)) return;
    if (reportedAttemptRef.current === attemptId) return;
    reportedAttemptRef.current = attemptId;
    const outcome = summarizeLegs(legs);
    if (outcome.someSucceeded) {
      // `reefs` counts successful (reef, ocean) promotions — matches the
      // pre-async aggregation, which summed `reefs: 1` per succeeded ocean
      // rather than counting distinct reefs. That count is `succeededLegs`,
      // NOT `succeededNames.length`: a bulk promote has one leg per
      // (ocean × reef) pair, and the names are deduped for the sentence the
      // callers build out of them ("Promoted to Acme, Acme, Acme" otherwise).
      onPromoted(outcome.succeededNames, { reefs: outcome.succeededLegs, corals: outcome.coralsPromoted });
    }
    if (!outcome.allSucceeded) {
      // Pre-select exactly the failed oceans, mirroring the pre-async retry
      // UX: hitting Promote again (after `retryReef` returns to the picker)
      // retries only what didn't land. (The failure banner itself is derived
      // straight from `legs` in the render below, not from `error` state —
      // see the early-return block — so it's correct on the very first
      // render a leg lands failed, with no effect-timing gap.)
      setChosenOceans([...new Set(legs.filter((l) => l.phase === "failed").map((l) => l.oceanId))]);
    }
  }, [reefNodeId, legs, attemptId, onPromoted]);

  // Fire one `POST /promote` per chosen ocean and enter the promoting phase
  // immediately (before any of them settle) so there's no gap where the
  // modal looks idle despite a submit already in flight. Handing the result
  // to `startBatch` is what makes it survive this modal closing.
  async function confirmReef(t: Extract<PromoteTarget, { kind: "reef" | "reefs" }>) {
    if (!token || chosenOceans.length === 0 || reefPhase === "promoting" || (t.kind === "reef" && selectedCoralIds.size === 0)) return;
    const attempted = chosenOceans;
    const coralIds = allCoralsSelected ? undefined : Array.from(selectedCoralIds);
    setError(null);
    setReefSubmitting(true);
    const requests = attempted.flatMap((gid) => (t.kind === "reef" ? [t.reef] : t.reefs).map((reef) => ({ gid, reef })));
    const settled = await Promise.allSettled(requests.map(({ gid, reef }) => api.reefs.promote(token, reef.node_id, gid, t.kind === "reef" ? coralIds : undefined)));
    const nextLegs = settled.map((r, i): PromoteLeg => {
      const gid = requests[i].gid;
      const oceanName = oceans.find((o) => o.id === gid)?.name ?? "a workspace";
      const result: PromoteCallResult =
        r.status === "fulfilled"
          ? { ok: true, jobId: r.value.job_id, coralsRequested: r.value.corals_requested, targetNodeId: r.value.target_node_id }
          : {
              ok: false,
              error: r.reason instanceof ApiError ? r.reason.message : "Couldn't promote to this workspace.",
            };
      return { ...legFromPromoteResult(gid, oceanName, result), itemId: requests[i].reef.node_id, itemTitle: requests[i].reef.title };
    });
    setReefSubmitting(false);
    setAttemptId((n) => n + 1);
    startBatch(t.kind === "reef" ? t.reef.node_id : targetIdentity, t.kind === "reef" ? t.reef.title : `${t.reefs.length} topics`, nextLegs);
  }

  // Back to the picker with the failed oceans pre-selected (see the
  // reporting effect above) and the coral selection untouched — "Promote"
  // fires a brand-new `POST /promote` call, i.e. a fresh job, never a resume
  // of the failed one. `clearBatch` drops the provider's (and storage's)
  // memory of the failed attempt so the picker, not a stale error, is what
  // comes back.
  function retryReef() {
    if (reefNodeId) clearBatch(reefNodeId);
    setError(null);
  }

  const targets = targetsOf(oceans, currentGraphId);
  // The source workspace decides the "who can see this today" sentence
  // below. This modal is opened from /workspaces?id= for TEAM and ORG
  // workspaces too, so an unconditional "only you can see this" is a false
  // privacy claim on exactly the screens where it matters most.
  const sourceGraph = oceans.find((g) => g.id === currentGraphId) ?? null;
  const sourceIsOwnerOnly = sourceGraph ? isOwnerOnlyOcean(sourceGraph) : true;
  const sourceName = sourceGraph?.name ?? "this workspace";
  const { title, description, itemName } = headerFor(target);

  const corals = useMemo(() => {
    if (target.kind !== "reef") return [];
    const digest = target.digest ?? null;
    const previewByAtomId = target.previewByAtomId ?? new Map<string, string>();
    return target.atoms.map((a) => ({ atomId: a.atom_id, ...atomLabel(a, digest, previewByAtomId) }));
  }, [target]);
  const filteredCorals = coralQ
    ? corals.filter((c) => `${c.body} ${c.meta}`.toLowerCase().includes(coralQ.toLowerCase()))
    : corals;
  const allCoralIds = useMemo(() => corals.map((c) => c.atomId), [corals]);
  const allCoralsSelected = selectedCoralIds.size === allCoralIds.length;

  // Native checkbox has no "some but not all" state of its own — reflect it
  // visually via `indeterminate` (DOM-only, not a React prop).
  useEffect(() => {
    if (masterCoralCheckboxRef.current) {
      masterCoralCheckboxRef.current.indeterminate = selectedCoralIds.size > 0 && !allCoralsSelected;
    }
  }, [selectedCoralIds, allCoralsSelected]);

  function toggleOcean(id: string) {
    setChosenOceans((cur) => (cur.includes(id) ? cur.filter((x) => x !== id) : [...cur, id]));
  }

  function toggleCoral(atomId: string) {
    setSelectedCoralIds((cur) => {
      const next = new Set(cur);
      if (next.has(atomId)) next.delete(atomId);
      else next.add(atomId);
      return next;
    });
  }
  function selectAllCorals() {
    setSelectedCoralIds(new Set(allCoralIds));
  }
  function selectNoCorals() {
    setSelectedCoralIds(new Set());
  }

  const summary =
    chosenOceans.length === 0
      ? "select at least one workspace"
      : target.kind === "reef"
        ? selectedCoralIds.size === 0
          ? `Select at least one supporting item → ${chosenOceans.length} workspace${chosenOceans.length > 1 ? "s" : ""}`
          : `1 topic + ${selectedCoralIds.size} item${selectedCoralIds.size === 1 ? "" : "s"} → ${chosenOceans.length} workspace${chosenOceans.length > 1 ? "s" : ""}`
        : target.kind === "reefs"
          ? `${target.reefs.length} topic${target.reefs.length === 1 ? "" : "s"} → ${chosenOceans.length} workspace${chosenOceans.length > 1 ? "s" : ""}`
          : target.kind === "session"
            ? `1 session + ${target.coralCount} item${target.coralCount === 1 ? "" : "s"} → ${chosenOceans.length} workspace${chosenOceans.length > 1 ? "s" : ""}`
            : `${target.sources.length} source${target.sources.length === 1 ? "" : "s"} → ${chosenOceans.length} workspace${chosenOceans.length > 1 ? "s" : ""}`;

  // Promotes to every chosen ocean in parallel and does NOT treat "some
  // succeeded" as done: a partial failure keeps the modal open, reports
  // success/failure per ocean, and leaves only the failed ones selected so
  // hitting Promote again retries exactly those (every call here is
  // idempotent, so a retry is always safe). It only closes itself once every
  // chosen ocean has fully succeeded.
  //
  // "reef" targets are handled entirely by `confirmReef` above instead (the
  // one target kind whose `api.reefs.promote` calls can now return a
  // `job_id` to poll) — this function is never invoked for them; see the
  // Button's `onClick` in the render below.
  async function confirm() {
    if (!token || chosenOceans.length === 0 || promoting) return;
    const attempted = chosenOceans;
    setPromoting(true);
    setError(null);

    type OceanResult = {
      gid: string;
      failed: boolean;
      reefs: number;
      corals: number;
      /** Session promotes only — see `onPromoted`'s `sessionCopied`. */
      sessionCopied?: boolean;
    };
    let results: OceanResult[];

    if (target.kind === "session") {
      const t = target;
      // ONE call per ocean, not a per-checkpoint fan-out: the backend route
      // owns the session's atomicity (container first, then all corals, then
      // the reefs warn-and-continue), so a partial result is reported by the
      // response rather than reassembled here from N settled promises.
      const settled = await Promise.allSettled(
        attempted.map((gid) => api.reefs.promoteSession(token, t.graphId, t.sessionId, gid)),
      );
      results = settled.map((r, i) => ({
        gid: attempted[i],
        failed: r.status === "rejected",
        reefs: r.status === "fulfilled" ? r.value.promoted_count : 0,
        corals: r.status === "fulfilled" ? r.value.corals_copied : 0,
        sessionCopied: r.status === "fulfilled" ? r.value.session_copied : false,
      }));
    } else {
      // Unreachable: the Share button dispatches "reef" targets to
      // `confirmReef` instead (see the render below), never here. Kept as an
      // explicit branch (rather than falling through to the `sources` arm
      // above) so this stays a compile-time-checked exhaustive switch over
      // `PromoteTarget["kind"]`, not a silent reef→sources misroute if that
      // dispatch ever changes.
      setPromoting(false);
      return;
    }

    setPromoting(false);
    const succeeded = results.filter((r) => !r.failed);
    if (succeeded.length > 0) void invalidate(queryClient, invalidationFor.promote(succeeded.map((r) => r.gid)));
    const failed = results.filter((r) => r.failed);
    const nameOf = (gid: string) => oceans.find((o) => o.id === gid)?.name ?? "a workspace";
    const succeededNames = succeeded.map((r) => nameOf(r.gid));
    const failedNames = failed.map((r) => nameOf(r.gid));

    if (failedNames.length === 0) {
      onPromoted(succeededNames, {
        reefs: succeeded.reduce((a, r) => a + r.reefs, 0),
        corals: succeeded.reduce((a, r) => a + r.corals, 0),
        // `every`, not `some`: if any succeeding ocean didn't get the
        // container, the honest summary is the weaker one.
        sessionCopied:
          target.kind === "session" ? succeeded.every((r) => r.sessionCopied) : undefined,
      });
      onClose(true);
      return;
    }
    if (succeeded.length > 0) {
      onPromoted(succeededNames, {
        reefs: succeeded.reduce((a, r) => a + r.reefs, 0),
        corals: succeeded.reduce((a, r) => a + r.corals, 0),
        // `every`, not `some`: if any succeeding ocean didn't get the
        // container, the honest summary is the weaker one.
        sessionCopied:
          target.kind === "session" ? succeeded.every((r) => r.sessionCopied) : undefined,
      });
    }
    setChosenOceans((cur) => cur.filter((id) => failed.some((r) => r.gid === id)));
    setError(
      succeeded.length > 0
        ? `Promoted to ${succeededNames.join(", ")}. Couldn't promote to ${failedNames.join(", ")} — please retry.`
        : `Couldn't promote to ${failedNames.join(", ")} — please retry.`,
    );
  }

  // The handoff (1d Share) is emphatic that promoting out of a workspace is
  // explicit and never implicit: nothing here is pre-checked. `targets`
  // starts empty and stays that way until the person picks a destination —
  // the "to" panel below reads "choose a workspace" until `chosenOceans` is
  // non-empty, exactly the "visible target, deliberate act" the mock draws.
  const chosenTargets = targets.filter((o) => chosenOceans.includes(o.id));

  // ── Promoting / done / error — "reef" target only ─────────────────────────
  // Replaces the whole picker with a dedicated panel once a submit is in
  // flight or has landed, instead of layering a banner over still-visible
  // (and now stale) "to"/coral-picker controls. Same <Modal> instance
  // throughout (`open` never toggles), so the focus trap doesn't fight
  // itself across the phase change.
  if (reefNodeId && reefPhase !== "idle") {
    const rows: { oceanId: string; oceanName: string; leg: PromoteLeg | null }[] = legs
      ? legs.map((l) => ({ oceanId: l.oceanId, oceanName: l.oceanName, leg: l }))
      : chosenOceans.map((gid) => ({ oceanId: gid, oceanName: oceans.find((o) => o.id === gid)?.name ?? "a workspace", leg: null }));
    const outcome = legs ? summarizeLegs(legs) : null;
    // Derived straight from `legs`/`outcome`, not the `error` state var — the
    // reporting effect above updates `error` a render late (effects run
    // after commit), which would otherwise flash an empty banner for one
    // frame on the exact render a leg first lands failed.
    const failureMessage = outcome ? promoteFailureMessage(outcome) : null;

    return (
      <Modal
        open
        onClose={() => onClose(reefPhase === "done" ? true : undefined)}
        width={640}
        title={
          reefPhase === "promoting" ? (
            "Promoting…"
          ) : reefPhase === "done" ? (
            "Promoted"
          ) : (
            "Couldn't finish promoting"
          )
        }
        description={
          reefPhase === "promoting" ? (
            <>
              Publishing evidence and processing <em className="text-brand">{itemName ?? "selected topics"}</em> — this can take a few
              minutes for a large topic. Safe to close: it keeps going in the background, and reopening
              Promote here picks the progress back up.
            </>
          ) : reefPhase === "done" ? (
            <>Every item behind <em className="text-brand">{itemName ?? "selected topics"}</em> that was requested has landed, and its processing jobs have finished.</>
          ) : (
            <>Some of what you asked for didn&apos;t make it across — nothing already promoted was undone.</>
          )
        }
        footer={
          reefPhase === "promoting" ? (
            <>
              <span className="mr-auto font-mono text-[10.5px] text-ink-3">promoting continues in the background</span>
              <Button variant="secondary" onClick={() => onClose()}>
                Close
              </Button>
            </>
          ) : reefPhase === "done" ? (
            <Button variant="brand" onClick={() => onClose(true)}>
              Done
            </Button>
          ) : (
            <>
              <span className="mr-auto font-mono text-[10.5px] text-ink-3">{failureMessage}</span>
              <Button variant="secondary" onClick={() => onClose()}>
                Close
              </Button>
              <Button variant="brand" onClick={retryReef}>
                Retry
              </Button>
            </>
          )
        }
      >
        {reefPhase === "done" && outcome && (
          <Notice tone="ok">{promoteSuccessMessage(itemName ?? "selected topics", outcome)}</Notice>
        )}
        {reefPhase === "error" && failureMessage && <ErrorNote>{failureMessage}</ErrorNote>}

        {reefPhase !== "done" && (
          <PromoteProgressList
            rows={rows.map(({ oceanId, oceanName, leg }) => ({
              oceanId,
              oceanName,
              itemTitle: target.kind === "reefs" ? leg?.itemTitle : undefined,
              leg,
            }))}
          />
        )}
      </Modal>
    );
  }

  return (
    <Modal
      open
      onClose={() => {
        if (!promoting) onClose();
      }}
      width={640}
      title={
        <>
          {title}
          {itemName && (
            <>
              {" "}
              <em className="text-brand">{itemName}</em>
            </>
          )}
        </>
      }
      description={
        <>
          {description !== "Publish" && `${description} `}
          {itemName ? "into a shared workspace. " : ""}
          {sourceIsOwnerOnly
            ? "Right now only you can see this."
            : `Right now this is limited to ${sourceName}.`}{" "}
          Sharing copies it — and the items behind it — into the workspace you
          choose below.
        </>
      }
      footer={
        <>
          <span className="mr-auto font-mono text-[10.5px] text-ink-3">{error ?? summary}</span>
          <Button variant="secondary" onClick={() => onClose()} disabled={promoting}>
            Cancel
          </Button>
          <Button
            variant="brand"
            onClick={(target.kind === "reef" || target.kind === "reefs") ? () => void confirmReef(target) : confirm}
            disabled={chosenOceans.length === 0 || promoting || (target.kind === "reef" && selectedCoralIds.size === 0)}
          >
            {promoting
              ? "Sharing…"
              : chosenTargets.length === 1
                ? `Share with ${chosenTargets[0].name}`
                : chosenTargets.length > 1
                  ? `Share with ${chosenTargets.length} workspaces`
                  : "Share"}
          </Button>
        </>
      }
    >
      {error && <ErrorNote className="mb-[16px]">{error}</ErrorNote>}

      {/* ── to: the explicit, visible destination ─────────────────────── */}
      <div className="mb-[18px]">
        <div className="mb-[9px] font-mono text-[9.5px] uppercase tracking-[0.13em] text-ink-3">to</div>
        {targets.length === 0 ? (
          <p className="font-mono text-[11px] text-ink-3">No shared workspaces available to share into yet.</p>
        ) : (
          <div className="flex flex-col gap-[6px]">
            {targets.map((o) => {
              const on = chosenOceans.includes(o.id);
              const hue = workspaceHue(o.id);
              return (
                <button
                  type="button"
                  key={o.id}
                  onClick={() => toggleOcean(o.id)}
                  disabled={promoting}
                  className={`flex w-full items-center gap-[10px] rounded-card border px-[14px] py-[11px] text-left transition-colors disabled:cursor-not-allowed disabled:opacity-60 ${
                    on ? "border-ink bg-bg shadow-card" : "border-line bg-bg hover:border-ink-3"
                  }`}
                >
                  <Dot hue={hue} size={9} />
                  <span className="min-w-0 flex-1">
                    <span className="block text-[13.5px] font-medium text-ink">
                      {o.name}{" "}
                      <span className="font-mono text-[9.5px] uppercase tracking-[0.08em] text-ink-3">{o.kind}</span>
                    </span>
                    <span className="mt-[2px] block font-mono text-[10.5px] text-ink-3">
                      created {fmtDate(o.created_at)}
                    </span>
                  </span>
                  <span
                    className={`grid h-[18px] w-[18px] flex-none place-items-center rounded-badge border text-[10px] text-white ${
                      on ? "border-ink bg-ink" : "border-line bg-bg"
                    }`}
                  >
                    {on && "✓"}
                  </span>
                </button>
              );
            })}
          </div>
        )}
      </div>

      {/* ── what's travelling ──────────────────────────────────────────── */}
      {target.kind === "reef" && (
        <div className="overflow-hidden rounded-card border border-line-2">
          <div className="flex items-center gap-[9px] bg-bg-subtle px-[13px] py-[10px]">
            {target.atoms.length > 0 ? (
              <>
                {/* Select-all/none affordance: master checkbox mirrors
                    the row checkboxes below (checked = every coral
                    selected, indeterminate = a partial subset, click
                    toggles between all and none). */}
                <input
                  ref={masterCoralCheckboxRef}
                  type="checkbox"
                  checked={allCoralsSelected}
                  onChange={() => (allCoralsSelected ? selectNoCorals() : selectAllCorals())}
                  aria-label={allCoralsSelected ? "Deselect all items" : "Select all items"}
                  className="h-4 w-4 flex-none accent-brand"
                />
                <span className="text-[12.5px] text-ink-2">
                  Includes {selectedCoralIds.size} of {target.atoms.length} item{target.atoms.length === 1 ? "" : "s"}{" "}
                  <span className="font-mono text-[10px] text-ink-3">provenance travels with the topic</span>
                  <button
                    type="button"
                    onClick={selectAllCorals}
                    disabled={allCoralsSelected}
                    className="ml-[10px] font-mono text-[10px] text-brand-2 hover:underline disabled:cursor-default disabled:text-ink-3 disabled:no-underline"
                  >
                    select all
                  </button>
                  <span className="mx-[6px] text-ink-3">·</span>
                  <button
                    type="button"
                    onClick={selectNoCorals}
                    disabled={selectedCoralIds.size === 0}
                    className="font-mono text-[10px] text-brand-2 hover:underline disabled:cursor-default disabled:text-ink-3 disabled:no-underline"
                  >
                    none
                  </button>
                </span>
              </>
            ) : (
              <span className="font-mono text-[10px] text-ink-3">This topic needs supporting evidence before it can be promoted.</span>
            )}
          </div>
          {target.atoms.length > 0 && (
            <div className="flex max-h-[240px] flex-col gap-1 overflow-auto border-t border-line-2 bg-bg px-[10px] py-2">
              <div className="mx-[3px] mb-[6px] mt-[2px]">
                <SearchInput
                  icon={<IconAsk width={13} height={13} />}
                  value={coralQ}
                  onChange={(e) => setCoralQ(e.target.value)}
                  placeholder="Search items by text, author, source…"
                />
              </div>
              {/* Selectable — every coral defaults checked; unchecking a
                  row narrows what's copied along with the reef. Selection
                  is keyed by atom id, so it's unaffected by the search
                  box filtering `filteredCorals` down to a subset of rows. */}
              {filteredCorals.length === 0 ? (
                <div className="px-4 py-4 text-center text-[12px] text-ink-3">No items match.</div>
              ) : (
                filteredCorals.map((c) => (
                  <label
                    key={c.atomId}
                    className="flex cursor-pointer items-center gap-[9px] rounded-row px-[9px] py-[7px] hover:bg-bg-hover"
                  >
                    <input
                      type="checkbox"
                      checked={selectedCoralIds.has(c.atomId)}
                      onChange={() => toggleCoral(c.atomId)}
                      className="h-[15px] w-[15px] flex-none accent-brand"
                    />
                    <span className="flex-none" style={{ color: `var(--hue-${c.hue})` }}>
                      <SourceIcon kind={c.icon} size={12} />
                    </span>
                    <span className="min-w-0 flex-1">
                      <span className="block truncate text-[12.5px] leading-[1.4] text-ink">{c.body}</span>
                      <span className="mt-[1px] block truncate font-mono text-[9.5px] text-ink-3">{c.meta}</span>
                    </span>
                  </label>
                ))
              )}
            </div>
          )}
        </div>
      )}

      {target.kind === "reefs" && (
        <div className="flex max-h-[280px] flex-col gap-1 overflow-auto rounded-card border border-line-2 bg-bg-subtle px-[10px] py-2">
          {target.reefs.map((r) => (
            <div key={r.node_id} className="flex items-center gap-[9px] rounded-row px-[9px] py-[7px]">
              <Chip>{r.type}</Chip>
              <span className="min-w-0 flex-1 truncate text-[12.5px] leading-[1.4] text-ink">{r.title}</span>
            </div>
          ))}
        </div>
      )}

      {target.kind === "session" && (
        <div className="overflow-hidden rounded-card border border-line-2">
          <div className="flex items-start gap-[9px] bg-bg-subtle px-[13px] py-[10px]">
            <span className="mt-[2px] flex-none text-hue-plum">
              <SourceIcon kind="code" size={13} />
            </span>
            <span className="min-w-0 flex-1">
              <span className="block truncate text-[12.5px] leading-[1.4] text-ink">{target.label}</span>
              <span className="mt-[2px] block font-mono text-[10px] text-ink-3">
                {target.coralCount} item{target.coralCount === 1 ? "" : "s"}
              </span>
            </span>
          </div>
          <div className="border-t border-line-2 bg-bg px-[13px] py-[10px] text-[12.5px] leading-[1.5] text-ink-2">
            The whole session travels: its items, every topic they grew, and the session
            itself — so it appears under Sessions in the target workspace, not as loose items.
          </div>
        </div>
      )}

      {target.kind === "sources" && (
        <div className="overflow-hidden rounded-card border border-line-2">
          <div className="bg-bg-subtle px-[13px] py-[10px] text-[12.5px] text-ink-2">
            Publishes every topic grown from the source, with its items.
          </div>
          <div className="flex max-h-[240px] flex-col gap-1 overflow-auto border-t border-line-2 bg-bg px-[10px] py-2">
            {target.sources.map((s) => (
              <div key={s.id} className="truncate rounded-row px-[9px] py-[7px] text-[12.5px] text-ink">
                ▸ {s.label}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── D6 UI, first half: "Taking it back" ─────────────────────────
          The original sync-chooser design draws this as a row in its "what changes" panel, and D6 fixes its
          wording: `delete the file -> retract`, with "any time" and any
          implication of a full cascade STRUCK because FM-3's 14-day
          rewrite window and FM-4's ex-member case both make them false.
          This modal is that artboard's shipped counterpart
          (docs/plans/capture/07-ui-track.md's screen→component table) and
          carried no such row at all — it described what sharing copies and
          said nothing about how a shared copy comes back, on the one
          dialog where the decision is actually taken. Copy lives in
          lib/retraction.ts so `retraction.test.ts` asserts on the exact
          strings (this repo has no component-render harness). */}
      <div className="mt-[14px] rounded-card border border-line-2 bg-bg-subtle px-[13px] py-[10px]">
        <div className="flex flex-wrap items-baseline gap-[7px]">
          <span className="text-[12.5px] font-medium text-ink">{SYNC_TAKE_IT_BACK_LABEL}</span>
          <span className="font-mono text-[10.5px] text-ink-3">{SYNC_TAKE_IT_BACK_BEFORE}</span>
          <span className="font-mono text-[10.5px] text-ink-4">→</span>
          <span className="font-mono text-[10.5px] text-ink">{SYNC_TAKE_IT_BACK_AFTER}</span>
        </div>
        <p className="mt-[5px] text-[11.5px] leading-[1.55] text-ink-2">{SYNC_TAKE_IT_BACK_NOTE}</p>
      </div>

      <p className="mt-[14px] font-mono text-[10.5px] text-ink-3">copy-on-promote · widening only</p>
    </Modal>
  );
}
