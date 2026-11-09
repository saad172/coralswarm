"use client";

import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Eyebrow } from "@/components/ui";
import { NO_EDIT_MESSAGE, NO_EDIT_TITLE, writeErrorMessage } from "@/lib/editAccess";
import { api, type Reef } from "@/lib/api";
import { topicCandidatesQueryOptions } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { fmtDate, srcName } from "@/lib/coral";

/* Suggested corals — vector-similar atoms not yet linked to this topic
   (curation). "Attach" links them via POST /v1/nodes/{id}/atoms; "Dismiss" is
   client-only (the API has no persistent dismiss for candidates, only add —
   ported from archive's CandidatesControl, which offered no dismiss either).
   400s when the topic has no atoms to seed retrieval from — treated as "no
   suggestions", matching the archive's behaviour. */
/** Collapsed count, shared with TopicPromoteSources so the two capped
 *  sections read as one rhythm rather than two arbitrary limits. Enough to be
 *  useful, few enough that the rail stays shorter than the timeline. */
const COLLAPSED = 4;

export function TopicCandidates({
  reef,
  token,
  onLinked,
  canEdit = true,
}: {
  reef: Reef;
  token: string | null;
  onLinked: () => void;
  /** Server-derived (`ReefDetail.can_edit`). Attaching is a write; when false
   *  it's disabled with a reason. Dismiss is client-only, so it stays. */
  canEdit?: boolean;
}) {
  const { authReady, getAuthToken, queryScope } = useApp();
  const qc = useQueryClient();
  // The bearer is read inside the query, never depended on: it rotates about
  // once a minute and keyed on it this re-read the suggestions every time.
  const options = topicCandidatesQueryOptions({ getAuthToken, scope: queryScope }, reef.node_id);
  const query = useQuery({ ...options, enabled: authReady });
  const cands = query.data ?? [];
  const loaded = query.isSuccess;
  const [dismissed, setDismissed] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState<string | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  async function attach(atomId: string) {
    if (!token) return;
    if (!canEdit) {
      setErr(NO_EDIT_MESSAGE);
      return;
    }
    setBusy(atomId);
    setErr(null);
    try {
      await api.reefs.linkAtom(token, reef.node_id, atomId);
      qc.setQueryData(options.queryKey, (cs) => cs?.filter((c) => c.atom_id !== atomId));
      onLinked();
    } catch (e) {
      // Non-fatal, but say so: a 404 here means the server refused the write.
      setErr(writeErrorMessage(e, "Couldn't attach that item"));
    } finally {
      setBusy(null);
    }
  }

  const shown = cands.filter((c) => !dismissed.has(c.atom_id));
  if (!loaded || shown.length === 0) return null;

  // Only a few at a time. Eight of these used to render at full height, which
  // made the rail roughly twice the main column and left a tall empty gutter
  // beside it — and the section is a side-offer, not the point of the page.
  const visible = expanded ? shown : shown.slice(0, COLLAPSED);
  const more = shown.length - visible.length;

  return (
    <div>
      <div className="mb-[3px] flex items-center gap-[8px]">
        <Eyebrow>suggested items</Eyebrow>
        <span className="h-px flex-1 bg-line-2" />
        <span className="font-mono text-[9.5px] text-ink-4">{shown.length}</span>
      </div>
      {/* The label alone reads as jargon — say what it is and what the number
          on each row means. */}
      <p className="mb-[9px] text-[11.5px] leading-[1.5] text-ink-3">
        Items that read as related to this topic but aren&rsquo;t linked to it yet. The percentage
        is how closely each one matches.
      </p>

      <div className="flex flex-col gap-[4px]">
        {visible.map((c) => (
          <div
            key={c.atom_id}
            className="group rounded-control border border-line-2 bg-bg-subtle px-[10px] py-[7px] transition-colors hover:border-line hover:bg-bg"
          >
            <div className="flex items-center gap-[7px]">
              <span className="flex-none font-mono text-[9.5px] text-ink-4">
                {(c.similarity * 100).toFixed(0)}%
              </span>
              <span className="min-w-0 flex-1 truncate font-mono text-[9.5px] uppercase tracking-[0.08em] text-ink-4">
                {srcName(c.source_type)}
                {c.timestamp ? ` · ${fmtDate(c.timestamp)}` : ""}
              </span>
              {/* Actions sit on the meta line rather than a third row: two rows
                  per candidate instead of three, across up to eight of them. */}
              <button
                type="button"
                onClick={() => void attach(c.atom_id)}
                disabled={busy !== null || !canEdit}
                title={canEdit ? undefined : NO_EDIT_TITLE}
                className="flex-none rounded-badge px-[5px] py-[1px] text-[11px] font-medium text-ink-2 transition-colors hover:bg-ink hover:text-white disabled:opacity-40"
              >
                {busy === c.atom_id ? "…" : "Attach"}
              </button>
              <button
                type="button"
                onClick={() => setDismissed((d) => new Set(d).add(c.atom_id))}
                aria-label="Dismiss suggestion"
                className="flex-none rounded-badge px-[4px] text-[13px] leading-none text-ink-4 transition-colors hover:text-ink"
              >
                ×
              </button>
            </div>
            <div className="mt-[3px] truncate text-[12.5px] leading-[1.4] text-ink">
              {c.title?.trim() || `${srcName(c.source_type)} item`}
            </div>
            {c.snippet?.trim() && (
              <div className="mt-[2px] line-clamp-2 text-[12px] leading-[1.4] text-ink-2">{c.snippet.trim()}</div>
            )}
          </div>
        ))}
      </div>

      {err && <p className="mt-[7px] font-mono text-[11px] text-brand-2">{err}</p>}

      {more > 0 && (
        <button
          type="button"
          onClick={() => setExpanded(true)}
          className="mt-[7px] font-mono text-[10px] text-ink-3 transition-colors hover:text-ink"
        >
          show {more} more ↓
        </button>
      )}
    </div>
  );
}
