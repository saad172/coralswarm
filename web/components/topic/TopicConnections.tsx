"use client";

import Link from "next/link";
import { scopeRefused } from "@/lib/api";
import { type LibraryNeighborhood, readLibraryNeighborhood } from "@/lib/libraryHistoryApi";
import type { LibraryScope } from "@/lib/libraryMemoryApi";
import { nodeLabel } from "@/lib/topicLabels";
import { sourceEvidenceHref } from "@/lib/provenance";
import { useLibraryRead } from "@/lib/useLibraryRead";
import { edgeEnded, groupEdgesByPredicate, validityLabel } from "@/lib/graphSurfaces";
import { Button, Chip, Empty, ErrorNote, Loading, Verdict } from "@/components/ui";

export function TopicConnections({ identity, getToken, scope, nodeId, after, supplied, onPage, onNode, onUnavailable }: {
  supplied?: { data: LibraryNeighborhood | null; retry: () => void };
  identity: string;
  getToken: () => string | null;
  scope: LibraryScope;
  nodeId: string;
  after?: string | null;
  onPage: (cursor: string | null) => void;
  onNode: (id: string, graph: string) => void;
  onUnavailable?: (message: string) => void;
}) {
  const remote = useLibraryRead(JSON.stringify([identity, scope, nodeId, after]), !!getToken() && (!supplied || !!after), async (signal) => {
    const refuse = (message: string): never => {
      if (!signal.aborted) onUnavailable?.(message);
      throw new Error(message);
    };
    const token = getToken();
    if (!token) return refuse("Verify the current Library account.");
    const result = await readLibraryNeighborhood(token, { scope, node_id: nodeId, page_size: 20, ...(after ? { after } : {}) }, signal).catch((error: unknown) => {
      if (!signal.aborted && scopeRefused(error)) onUnavailable?.(error.message);
      throw error;
    });
    return getToken() ? result : refuse("The Library account changed. Retry connections.");
  });
  const read = supplied && !after ? { ...remote, data: supplied.data ?? undefined, loading: false,
    error: supplied.data ? undefined : "This topic section is unavailable.", refreshError: undefined, refresh: supplied.retry } : remote;
  const graph = scope.graph_ids[0];
  const nextCursor = read.data?.next_cursor;
  return <section aria-label="Supported node connections" className="space-y-4 border-t border-line pt-6">
    <h2 className="font-display text-2xl font-normal text-ink">Supported connections</h2>
    <p className="max-w-prose text-sm leading-6 text-ink-2">Recorded typed relationships between this topic and other topics, grouped by relationship, in this readable retained context. A current capture pointer does not establish that a relationship is valid now; stated validity dates are shown where the source gave them.</p>
    {read.loading && <Loading what="verifying supported connections" />}
    {(read.error ?? read.refreshError) && <ErrorNote action={<Button onClick={read.refresh}>Retry connections</Button>}>{read.error ?? read.refreshError}</ErrorNote>}
    {read.data?.items.length === 0 && <Empty title="No supported typed connections returned" hint="This is the accessible relationship set for this scope, not a list of same-type suggestions." />}
    {!!read.data?.items.length && <div className="space-y-5">
      {groupEdgesByPredicate(read.data.items, nodeId).map((group) => {
        const rows = [...group.edges.filter(({ edge }) => !edgeEnded(edge.valid_until)), ...group.edges.filter(({ edge }) => edgeEnded(edge.valid_until))];
        const headingId = `connections-${group.key.replace(/[^a-z0-9]+/gi, "-")}`;
        return <section key={group.key} aria-labelledby={headingId} className="min-w-0">
          <h3 id={headingId} className="flex flex-wrap items-baseline gap-x-2 border-b border-line-2 pb-2 text-sm text-ink">
            <span className="sr-only">{group.direction === "out" ? `This topic ${group.label}, ${group.edges.length} ${group.edges.length === 1 ? "topic" : "topics"}` : `${group.label} this topic, from ${group.edges.length} ${group.edges.length === 1 ? "topic" : "topics"}`}</span>
            {group.direction === "in" && <span aria-hidden="true" className="font-mono text-[11px] text-ink-3">topic →</span>}
            {group.direction === "out" && <span aria-hidden="true" className="font-mono text-[11px] text-ink-3">this topic →</span>}
            <span aria-hidden="true" className="font-medium">{group.label}</span>
            {group.direction === "in" && <span aria-hidden="true" className="font-mono text-[11px] text-ink-3">→ this topic</span>}
            <span aria-hidden="true" className="font-mono text-[10.5px] text-ink-3">· {group.edges.length}</span>
          </h3>
          <ul className="divide-y divide-line-2">
            {rows.map(({ edge, other }) => {
              const validity = validityLabel(edge.valid_from, edge.valid_until);
              const ended = edgeEnded(edge.valid_until);
              return <li key={edge.edge_id} className="min-w-0 space-y-2 py-3">
                <div className="flex flex-wrap items-center gap-x-3 gap-y-2 text-sm">
                  <button type="button" className={`min-w-0 max-w-full rounded-row break-words text-left underline underline-offset-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand ${ended ? "text-ink-2" : "text-brand-2"}`} onClick={() => onNode(other.node_id, graph)}>{nodeLabel(other.title)}</button>
                  {other.type && <span className="font-mono text-[10.5px] text-ink-3">{other.type}</span>}
                  {validity && <span className="font-mono text-[10.5px] text-ink-2">{validity}</span>}
                  {ended && <Verdict tone="muted" glyph="↺" title="This relationship's stated validity has ended.">ended</Verdict>}
                </div>
                <div className="flex flex-wrap gap-2"><Chip>{edge.origin}</Chip><Chip>{edge.status}</Chip></div>
                <details>
                  <summary className="cursor-pointer rounded-row text-sm text-brand-2 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand">Inspect relationship support · {edge.support.length}{edge.support_truncated ? " shown" : ""}</summary>
                  <p className="mt-2 text-xs text-ink-3">Recorded confidence: {edge.confidence}. {edge.reviewed_by ? "Review attribution recorded." : "No reviewer attribution recorded."}</p>
                  {edge.reviewed_by && <p className="mt-1 break-all font-mono text-xs text-ink-3">Reviewer: {edge.reviewed_by}</p>}
                  <ul className="mt-3 space-y-2 text-sm">
                    {edge.support.map((support, index) => <li key={"kind" in support ? `${support.graph_id}:${support.atom_id}` : `${support.source_item_id}:${support.source_version_id}`}>{"kind" in support ? <Link className="rounded-row text-brand-2 underline underline-offset-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand" href={`/coral?${new URLSearchParams({ id: support.atom_id, graph_id: support.graph_id })}`}>Legacy chunk support {index + 1} · independent of semantic source count</Link> : <Link className="rounded-row text-brand-2 underline underline-offset-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand" href={sourceEvidenceHref(support.source_item_id, graph, support.source_version_id)}>Retained version support {index + 1} · {support.source_version_is_current === undefined ? "capture pointer status unknown" : support.source_version_is_current ? "current capture" : "earlier capture"}</Link>}</li>)}
                  </ul>
                  {edge.support_truncated && <p className="mt-2 text-xs text-ink-3">Additional permitted support references are omitted from this bounded response.</p>}
                </details>
              </li>;
            })}
          </ul>
        </section>;
      })}
    </div>}
    {read.data?.truncated && <p className="text-xs text-ink-3">Additional permitted edges are outside this bounded page.</p>}
    <div className="flex flex-wrap gap-2">
      {nextCursor && <Button onClick={() => onPage(nextCursor)}>Next connections</Button>}
      {after && <Button variant="ghost" onClick={() => onPage(null)}>First connections</Button>}
    </div>
  </section>;
}
