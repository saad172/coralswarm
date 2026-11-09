"use client";

// Shared renderer for the async machine-written meeting summary (overview,
// decisions, action items) plus its lifecycle states (pending/running, done,
// failed, blocked). Used by the meeting summary view
// (app/(app)/meeting/page.tsx).
//
// The machine voice is SANS (Geist) — serif italic is reserved for human-voice
// titles. Provenance (model credit, timestamps, .lbl eyebrows) is mono.
// Decisions carry a coral (--brand) left border — coral is the decision hue.

import { useCallback, useEffect, useRef, useState } from "react";
import type { MeetingSummaryBody, MeetingSummaryStatus } from "@/lib/api";
import { Loading, ErrorNote, Eyebrow, Dot, Button } from "@/components/ui";

/** `mm:ss` from a millisecond offset. */
export function mmss(ms: number): string {
  const s = Math.floor(ms / 1000);
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}

/** Deep-link plumbing shared by both transcript views: register a ref per
 *  transcript segment row, then `onCite(ts_ms)` scrolls to the nearest segment
 *  (by |start_ms - ts_ms|) and briefly highlights it. `hot` is the index to
 *  render highlighted; mirror the cite-hot idiom (brand border + brand-3 wash). */
export function useCiteDeepLink(segments: { start_ms: number }[] | null | undefined) {
  const refs = useRef<(HTMLElement | null)[]>([]);
  const [hot, setHot] = useState<number | null>(null);
  const timer = useRef<number | null>(null);

  const setRef = useCallback(
    (i: number) => (el: HTMLElement | null) => {
      refs.current[i] = el;
    },
    [],
  );

  const onCite = useCallback(
    (tsMs: number | null) => {
      if (tsMs == null || !segments || segments.length === 0) return;
      let best = 0;
      let bestDist = Infinity;
      segments.forEach((seg, i) => {
        const d = Math.abs(seg.start_ms - tsMs);
        if (d < bestDist) {
          bestDist = d;
          best = i;
        }
      });
      refs.current[best]?.scrollIntoView({ behavior: "smooth", block: "center" });
      setHot(best);
      if (timer.current) window.clearTimeout(timer.current);
      timer.current = window.setTimeout(() => setHot(null), 1500);
    },
    [segments],
  );

  useEffect(
    () => () => {
      if (timer.current) window.clearTimeout(timer.current);
    },
    [],
  );

  return { setRef, onCite, hot };
}

/** Highlight style for the currently-cited transcript row (transient). */
export const CITE_HOT_STYLE = {
  borderColor: "var(--brand)",
  background: "var(--brand-soft)",
} as const;

/** A cite chip showing `mm:ss`, deep-linking to the nearest transcript segment.
 *  Mirrors the ask-view cite idiom (span + onClick). Null `ts_ms` renders an
 *  inert em-dash chip. */
function CiteChip({ tsMs, onCite }: { tsMs: number | null; onCite: (ts: number | null) => void }) {
  if (tsMs == null) {
    return (
      <span className="cite" style={{ cursor: "default" }} aria-hidden>
        —
      </span>
    );
  }
  return (
    <button
      type="button"
      className="cite"
      onClick={() => onCite(tsMs)}
      title="jump to this moment"
      aria-label={`Jump to ${mmss(tsMs)} in transcript`}
    >
      {mmss(tsMs)}
    </button>
  );
}

/** The three synthesis sections (summary · decisions · action items). */
function SummarySections({
  summary,
  model,
  onCite,
}: {
  summary: MeetingSummaryBody;
  model: string | null;
  onCite: (ts: number | null) => void;
}) {
  return (
    <div className="flex flex-col gap-[24px]">
      {/* ── summary ─────────────────────────────────────────────────────────── */}
      <section>
        <Eyebrow className="mb-[8px]">summary</Eyebrow>
        <p className="text-[14.5px] leading-[1.65] text-ink-2">{summary.overview}</p>
      </section>

      {/* ── decisions (coral hue) ───────────────────────────────────────────── */}
      {summary.decisions.length > 0 && (
        <section>
          <Eyebrow className="mb-[8px]">decisions</Eyebrow>
          <div className="flex flex-col gap-[8px]">
            {summary.decisions.map((d, i) => (
              <div
                key={`d-${i}`}
                className="flex flex-wrap items-center gap-[12px] rounded-card border border-line bg-bg-2 px-[16px] py-[12px]"
              >
                <Dot hue="coral" size={7} />
                <span className="min-w-0 flex-1 text-[13.5px] leading-[1.5] text-ink">{d.text}</span>
                <CiteChip tsMs={d.ts_ms} onCite={onCite} />
              </div>
            ))}
          </div>
        </section>
      )}

      {/* ── action items ────────────────────────────────────────────────────── */}
      {summary.action_items.length > 0 && (
        <section>
          <Eyebrow className="mb-[8px]">action items</Eyebrow>
          <div className="flex flex-col gap-[8px]">
            {summary.action_items.map((a, i) => (
              <div
                key={`a-${i}`}
                className="flex flex-wrap items-center gap-[12px] rounded-card border border-line bg-bg-2 px-[16px] py-[12px]"
              >
                <span className="min-w-0 flex-1 text-[13.5px] leading-[1.5]">
                  {a.owner ? (
                    <span className="font-medium text-ink">{a.owner}</span>
                  ) : (
                    <span className="text-ink-3">unassigned</span>
                  )}
                  <span className="mx-[8px] font-mono text-[11px] text-ink-3">·</span>
                  <span className="text-ink-2">{a.text}</span>
                </span>
                <CiteChip tsMs={a.ts_ms} onCite={onCite} />
              </div>
            ))}
          </div>
        </section>
      )}

      {model && <Eyebrow>synthesized by {model}</Eyebrow>}
    </div>
  );
}

/** Full summary surface: renders the right affordance for the current
 *  `summary_status`. The transcript is rendered separately by each view. */
export function MeetingSummaryView({
  status,
  summary,
  model,
  error,
  capReached = false,
  busy = false,
  onGenerate,
  onCite,
}: {
  status: MeetingSummaryStatus;
  summary: MeetingSummaryBody | null;
  model: string | null;
  error: string | null;
  /** Polling gave up while still pending — offer a manual generate affordance. */
  capReached?: boolean;
  busy?: boolean;
  /** (Re-)enqueue synthesis. `allowCloud` false = retry/local generate; true =
   *  the per-meeting cloud opt-in from the `blocked` state. */
  onGenerate: (allowCloud: boolean) => void;
  onCite: (ts: number | null) => void;
}) {
  if (status === "done" && summary) {
    return <SummarySections summary={summary} model={model} onCite={onCite} />;
  }

  if (status === "failed") {
    return (
      <div className="flex flex-col gap-[12px]">
        <ErrorNote>
          {error?.trim()
            ? error
            : "We couldn't synthesize this meeting. You can try again."}
        </ErrorNote>
        <div>
          <Button variant="secondary" size="sm" disabled={busy} onClick={() => onGenerate(false)}>
            {busy ? "retrying…" : "Retry"}
          </Button>
        </div>
      </div>
    );
  }

  if (status === "blocked") {
    return (
      <div className="rounded-card border border-line bg-bg-2 p-[16px]">
        <Eyebrow className="mb-[8px]">summary</Eyebrow>
        <p className="mb-[12px] text-[14px] leading-[1.6] text-ink-2">
          Summaries need cloud synthesis — generate one for this meeting?
        </p>
        <Button variant="primary" size="sm" disabled={busy} onClick={() => onGenerate(true)}>
          {busy ? "starting…" : "Generate summary"}
        </Button>
      </div>
    );
  }

  // pending / running.
  if (capReached) {
    return (
      <div className="rounded-card border border-dashed border-line bg-bg px-[16px] py-[16px]">
        <p className="mb-[12px] font-mono text-[12px] leading-[1.6] text-ink-3">
          This meeting doesn&rsquo;t have a summary yet.
        </p>
        <Button variant="secondary" size="sm" disabled={busy} onClick={() => onGenerate(false)}>
          {busy ? "starting…" : "Generate summary"}
        </Button>
      </div>
    );
  }
  return <Loading what="summary on its way" />;
}
