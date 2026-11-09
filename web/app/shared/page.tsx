"use client";

// Public, read-only meeting share view (CS-uxp3b). Lives OUTSIDE the `(app)`
// route group — no auth, no app chrome — and reads its token from `?t=` rather
// than a dynamic segment. `useSearchParams` needs a Suspense boundary, hence
// the wrapper below (mirrors app/(app)/meeting/page.tsx's `?source_id=`
// pattern).
//
// Tries the PUBLIC endpoint first (works for anyone with a "link" share, no
// sign-in). If that 404s (revoked, unknown token, or a "team"-only share) and
// the viewer happens to be signed in to CoralSwarm, retries the AUTHED
// endpoint, which also resolves "team" shares for members of the owning ocean.

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { Empty, Loading, Wordmark, Eyebrow } from "@/components/ui";
import { useApp } from "@/lib/providers";
import { sharedMeetingQueryOptions } from "@/lib/appQueries";
import { fmtDurationMs, fmtTimestampMs, fmtWhen } from "@/lib/meetingFormat";

type ViewState = "loading" | "ok" | "unavailable";

function SharedInner() {
  const params = useSearchParams();
  const shareToken = params.get("t") ?? "";
  const { ready, getAuthToken, queryScope } = useApp();

  // Waits for auth to resolve so a signed-in viewer's authed retry only fires
  // once, instead of racing a second fetch mid-render.
  const query = useQuery({
    ...sharedMeetingQueryOptions({ getAuthToken, scope: queryScope }, shareToken),
    enabled: ready && !!shareToken,
  });
  const data = query.data ?? null;
  const status: ViewState = !shareToken ? "unavailable" : query.isPending ? "loading" : data ? "ok" : "unavailable";

  if (status === "loading") {
    return (
      <div className="reef-bg flex min-h-screen items-center justify-center bg-bg px-6">
        <Loading what="surfacing this share" />
      </div>
    );
  }

  if (status === "unavailable" || !data) {
    return (
      <div className="reef-bg flex min-h-screen flex-col items-center justify-center gap-8 bg-bg px-6">
        <Wordmark size={22} />
        <Empty
          title="This link isn't available"
          hint="It may have been revoked, or the link isn't valid. Ask whoever shared it for a fresh one."
        />
      </div>
    );
  }

  const duration = fmtDurationMs(data.duration_ms);
  // Structured summary (cs-uxp3a): overview + decisions + action items. Every
  // part is optional/best-effort, so render only the pieces that carry content.
  const summary = data.summary;
  const overview = summary?.overview?.trim() || "";
  const decisions = (summary?.decisions ?? []).filter((d) => d.text?.trim());
  const actionItems = (summary?.action_items ?? []).filter((a) => a.text?.trim());
  const hasSummary = Boolean(overview) || decisions.length > 0 || actionItems.length > 0;
  const stamp = (ts: number | null | undefined) =>
    typeof ts === "number" && Number.isFinite(ts) ? fmtTimestampMs(ts) : null;

  return (
    <div className="reef-bg min-h-screen bg-bg px-6 py-10">
      <div className="mx-auto flex max-w-[720px] flex-col gap-6">
        <div className="flex items-center justify-between">
          <Wordmark size={20} />
          <span className="font-mono text-[10px] uppercase tracking-[0.14em] text-ink-3">read-only share</span>
        </div>

        <div>
          <h1 className="font-display text-[30px] italic leading-[1.1] text-ink">
            {data.title || "Untitled meeting"}
          </h1>
          <p className="mt-[6px] font-mono text-[11px] text-ink-3">
            {fmtWhen(data.occurred_at)}
            {duration && <span> · {duration}</span>}
          </p>
        </div>

        {hasSummary && (
          <section className="flex flex-col gap-4">
            {overview && (
              <div>
                <Eyebrow className="mb-2">summary</Eyebrow>
                <p className="text-[14px] leading-[1.6] text-ink-2">{overview}</p>
              </div>
            )}
            {decisions.length > 0 && (
              <div>
                <Eyebrow className="mb-2">decisions</Eyebrow>
                <ul className="flex flex-col gap-[6px]">
                  {decisions.map((d, i) => {
                    const ts = stamp(d.ts_ms);
                    return (
                      <li key={i} className="flex gap-2 text-[14px] leading-[1.6] text-ink-2">
                        <span aria-hidden className="select-none text-ink-3">
                          ·
                        </span>
                        <span className="flex-1">
                          {d.text}
                          {ts && <span className="ml-2 font-mono text-[11px] text-ink-3">[{ts}]</span>}
                        </span>
                      </li>
                    );
                  })}
                </ul>
              </div>
            )}
            {actionItems.length > 0 && (
              <div>
                <Eyebrow className="mb-2">action items</Eyebrow>
                <ul className="flex flex-col gap-[6px]">
                  {actionItems.map((a, i) => {
                    const ts = stamp(a.ts_ms);
                    const owner = a.owner?.trim();
                    return (
                      <li key={i} className="flex gap-2 text-[14px] leading-[1.6] text-ink-2">
                        <span aria-hidden className="select-none text-ink-3">
                          ☐
                        </span>
                        <span className="flex-1">
                          {owner && <span className="font-medium text-ink">{owner}: </span>}
                          {a.text}
                          {ts && <span className="ml-2 font-mono text-[11px] text-ink-3">[{ts}]</span>}
                        </span>
                      </li>
                    );
                  })}
                </ul>
              </div>
            )}
          </section>
        )}

        <section>
          <Eyebrow className="mb-2">transcript</Eyebrow>
          <pre className="max-h-[70vh] overflow-auto whitespace-pre-wrap rounded-card border border-line bg-bg-2 p-4 font-mono text-[12px] leading-[1.6] text-ink-2">
            {data.transcript || "No transcript."}
          </pre>
        </section>

        <p className="pb-6 pt-2 text-center font-mono text-[10px] uppercase tracking-[0.14em] text-ink-3">
          shared read-only · coralswarm
        </p>
      </div>
    </div>
  );
}

export default function SharedPage() {
  return (
    <Suspense
      fallback={
        <div className="reef-bg flex min-h-screen items-center justify-center bg-bg px-6">
          <Loading what="surfacing this share" />
        </div>
      }
    >
      <SharedInner />
    </Suspense>
  );
}
