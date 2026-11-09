"use client";

import { useState } from "react";
import Link from "next/link";
import { readP6NodeAtoms } from "@/lib/p6EvidenceApi";
import type { NodeMeetingAtomsPage } from "@/lib/api";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { useLibraryRead } from "@/lib/useLibraryRead";
import { meetingEvidenceHref, sourceEvidenceHref } from "@/lib/provenance";
import { sourceMeta } from "@/components/library/sourceKind";
import { representationLabel } from "@/components/sources/SourceSnapshotMetadata";
import { Button, ErrorNote } from "@/components/ui";

function supportingPassageHref(atom: NodeMeetingAtomsPage["items"][number], graphId: string) {
  if (atom.source_item_id) return sourceEvidenceHref(atom.source_item_id, graphId, atom.source_version_id, atom.atom_id, atom.passages[0]?.passage_id);
  if (atom.source_type === "meeting" && typeof atom.source_id === "string" && atom.source_id.length > 0 && Array.from(atom.source_id).length <= 1024 && !/[\u0000-\u001f\u007f]/.test(atom.source_id)) return meetingEvidenceHref(atom.source_id, graphId, atom.source_version_id, atom.atom_id, atom.passages[0]?.passage_id);
  return null;
}

/** The reverse path from a canonical node to versioned source evidence. */
export function NodeSourceAtoms({ graphId, nodeId, getAuthToken: _getAuthToken }: { graphId: string; nodeId: string; getAuthToken: () => string | null | Promise<string | null> }) {
  const admission = useEvidenceAdmission();
  const scopeKey = JSON.stringify([admission.key, graphId, nodeId]);
  const [paging, setPaging] = useState<{ scopeKey: string; cursor: string } | null>(null);
  const cursor = paging?.scopeKey === scopeKey ? paging.cursor : undefined;
  const read = useLibraryRead(JSON.stringify([scopeKey, cursor]), admission.ready, (signal) => readP6NodeAtoms(admission.getToken(), { graphId, nodeId, ...(cursor ? { before: cursor } : {}) }, signal));
  const page = read.data;
  const error = read.error ?? read.refreshError;
  const busy = read.loading;
  function loadMore() { if (page?.next_cursor) setPaging({ scopeKey, cursor: page.next_cursor }); }

  return (
    <section aria-labelledby="node-source-atoms" className="mb-8 space-y-3">
      <div>
        <h2 id="node-source-atoms" className="font-display text-2xl font-normal text-ink">Source atoms</h2>
        <p className="mt-1 max-w-prose text-sm leading-6 text-ink-2">Follow this node back to its supporting passages. Preserved versions may describe earlier decisions.</p>
      </div>
      {error && <ErrorNote action={<Button size="sm" onClick={() => read.refresh()}>Retry</Button>}>{error}</ErrorNote>}
      {!page && !error && <p role="status" className="text-sm text-ink-2">Loading source atoms…</p>}
      {page?.items.length === 0 && <p className="text-sm text-ink-2">No preserved source atoms are linked to this node yet.</p>}
      {!!page?.items.length && <ul className="divide-y divide-line border-y border-line">
        {page.items.map((atom) => {
          const href = supportingPassageHref(atom, graphId);
          return <li key={atom.atom_id} className="space-y-2 py-3">
            <div className="flex flex-wrap gap-x-3 gap-y-1 font-mono text-xs text-ink-2"><span>{atom.kind}</span><span>{atom.is_legacy_baseline ? "Preserved" : "Captured"} {new Date(atom.captured_at).toLocaleDateString()}</span>{atom.source_type && <span>{sourceMeta(atom.source_type).label} · {representationLabel(atom.representation)}</span>}</div>
            <p className="break-words text-sm leading-6 text-ink">{atom.statement}</p>
            {href ? <Link href={href} className="inline-block rounded-row text-sm text-brand-2 underline underline-offset-4 hover:text-ink focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand">Open supporting passage</Link> : <p className="text-sm text-ink-2">Exact retained source address unavailable.</p>}
          </li>;
        })}
      </ul>}
      {page?.next_cursor && <Button onClick={() => void loadMore()} disabled={busy}>{busy ? "Loading atoms…" : "More source atoms"}</Button>}
    </section>
  );
}
