"use client";

import { useState } from "react";
import Link from "next/link";
import type { SourceItemsPage } from "@/lib/api";
import { readP6SourceItems } from "@/lib/p6EvidenceApi";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { useLibraryRead } from "@/lib/useLibraryRead";
import { sourceEvidenceHref } from "@/lib/provenance";
import { sourceMeta } from "@/components/library/sourceKind";
import { Button, Empty, ErrorNote, Loading } from "@/components/ui";
import { representationLabel, sourceTitle } from "./SourceSnapshotMetadata";

/** Both the inventory and a chunk's exact verified membership use this bounded list.
 * The latter never guesses a version from the chunk's native source ID. */
export function SourceInventory({ graphId, getAuthToken: _getAuthToken, atomId, supplied }: { graphId: string; getAuthToken: () => string | null | Promise<string | null>; atomId?: string; supplied?: { page: SourceItemsPage | null; refresh: () => void } }) {
  const admission = useEvidenceAdmission();
  const scopeKey = JSON.stringify([admission.key, graphId, atomId]);
  const [paging, setPaging] = useState<{ scopeKey: string; cursor: string } | null>(null);
  const cursor = paging?.scopeKey === scopeKey ? paging.cursor : undefined;
  const read = useLibraryRead(JSON.stringify([scopeKey, cursor]), admission.ready && (!supplied || !!cursor), (signal) => readP6SourceItems(admission.getToken(), { graphId, ...(atomId ? { atomId } : {}), ...(cursor ? { before: cursor } : {}) }, signal));
  const page = cursor || !supplied ? read.data : supplied.page;
  const error = (cursor || !supplied ? read.error ?? read.refreshError : null) ?? (supplied && !cursor && !page ? "Preserved sources are unavailable." : null);
  const busy = read.loading;
  function loadMore() { if (page?.next_cursor) setPaging({ scopeKey, cursor: page.next_cursor }); }

  return <section aria-label={atomId ? "Preserved versions containing this item" : "Preserved source items"} className="mb-8 min-w-0 space-y-4">
    <div>
      <h2 className="font-display text-2xl font-normal text-ink">{atomId ? "Preserved source evidence" : "Preserved source items"}</h2>
      <p className="mt-2 max-w-prose text-sm leading-6 text-ink-2">{atomId ? "Open the saved version that contains this item to explore its passages, atoms, and connected nodes." : "Explore captured emails, messages, meetings, and sessions. Each item keeps its saved versions and supporting passages together."}</p>
    </div>
    {error && <ErrorNote action={<Button size="sm" onClick={() => { if (!cursor && supplied) supplied.refresh(); else read.refresh(); }}>Retry sources</Button>}>{error}</ErrorNote>}
    {!page && !error && <Loading what="loading preserved sources" />}
    {page?.items.length === 0 && <Empty title={atomId ? "No verified preserved version" : "No preserved source items yet"} hint={atomId ? "This item has no verified link to a saved source version. Its content remains available below." : "New captures appear here. Earlier content remains available in source groups below."} />}
    {!!page?.items.length && <ul className="divide-y divide-line border-y border-line">
      {page.items.map((item) => {
        const versionId = atomId ? item.mapped_version_id : item.current_version_id;
        const title = atomId ? item.source_id : sourceTitle(item.source_metadata, item.source_id);
        const captured = item.captured_at ? new Date(item.captured_at) : null;
        const timeLabel = item.is_legacy_baseline === true ? "Preserved" : item.is_legacy_baseline === false ? "Captured" : "Retained";
        return <li key={item.source_item_id} className="min-w-0 py-4">
          {versionId ? <Link href={sourceEvidenceHref(item.source_item_id, graphId, versionId)} className="block rounded-row text-ink hover:text-brand-2 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand">
            <span className="block max-w-prose break-words text-base font-medium">{title}</span>
            <span className="mt-1 block text-xs leading-5 text-ink-2">{sourceMeta(item.source_type).label}{!atomId && <> · {representationLabel(item.representation)}</>}{!atomId ? ` · ${timeLabel} ${captured && Number.isFinite(captured.getTime()) ? captured.toLocaleDateString() : "time unknown"}` : ""}</span>
            <span className="mt-2 block text-sm text-brand-2">{atomId ? "Open verified source version" : "Explore versions and atoms"}</span>
          </Link> : <p className="text-sm text-ink-2">{title} · No preserved version available</p>}
        </li>;
      })}
    </ul>}
    {page?.next_cursor && <Button disabled={busy} onClick={() => void loadMore()}>{busy ? "Loading sources…" : "More sources"}</Button>}
  </section>;
}
