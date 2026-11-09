"use client";

import { useEffect } from "react";
import { scopeRefused } from "@/lib/api";
import { type LibraryNodeSessions, readLibraryNodeSessions } from "@/lib/libraryHistoryApi";
import type { LibraryScope } from "@/lib/libraryMemoryApi";
import { useLibraryRead } from "@/lib/useLibraryRead";
import { Button, Empty, ErrorNote, Loading } from "@/components/ui";

const dateLabel = (value: string) => Number.isFinite(Date.parse(value)) ? new Date(value).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "Unknown";

export function TopicSessions({ identity, getToken, scope, nodeId, after, supplied, onPage, onSettled, onUnavailable }: {
  supplied?: { data: LibraryNodeSessions | null; retry: () => void };
  identity: string;
  getToken: () => string | null;
  scope: LibraryScope;
  nodeId: string;
  after?: string | null;
  onPage: (cursor: string | null) => void;
  /** Fires on data or error, so Back restoration never waits on a failed side read. */
  onSettled?: () => void;
  onUnavailable?: (message: string) => void;
}) {
  const remote = useLibraryRead(JSON.stringify([identity, scope, nodeId, after]), !!getToken() && (!supplied || !!after), async (signal) => {
    const refuse = (message: string): never => {
      if (!signal.aborted) onUnavailable?.(message);
      throw new Error(message);
    };
    const token = getToken();
    if (!token) return refuse("Verify the current Library account.");
    const result = await readLibraryNodeSessions(token, { scope, node_id: nodeId, page_size: 20, ...(after ? { after } : {}) }, signal).catch((error: unknown) => {
      if (!signal.aborted && scopeRefused(error)) onUnavailable?.(error.message);
      throw error;
    });
    return getToken() ? result : refuse("The Library account changed. Retry sessions.");
  });
  const read = supplied && !after ? { ...remote, data: supplied.data ?? undefined, loading: false,
    error: supplied.data ? undefined : "This topic section is unavailable.", refreshError: undefined, refresh: supplied.retry } : remote;
  const settled = !!read.data || !!read.error;
  useEffect(() => { if (settled) onSettled?.(); }, [settled, onSettled]);
  const nextCursor = read.data?.next_cursor;
  return <section aria-label="Sessions supporting this node" className="space-y-4 border-t border-line pt-6">
    <h2 className="font-display text-2xl font-normal text-ink">Supporting sessions{read.data ? ` · ${read.data.total.toLocaleString()}` : ""}</h2>
    <p className="max-w-prose text-sm leading-6 text-ink-2">Captured sessions whose readable retained versions support this node in this scope, newest supporting capture first.</p>
    {read.loading && <Loading what="verifying supporting sessions" />}
    {(read.error ?? read.refreshError) && <ErrorNote action={<Button onClick={read.refresh}>Retry sessions</Button>}>{read.error ?? read.refreshError}</ErrorNote>}
    {read.data?.items.length === 0 && <Empty title="No captured session supports this node here" hint="Support from sources without a captured session is counted on the node, not listed here." />}
    {!!read.data?.items.length && <ul className="divide-y divide-line border-y border-line">
      {read.data.items.map((s) => <li key={`${s.provider_namespace}\u0000${s.session_id}`} className="min-w-0 py-3">
        <span data-library-focus={`node-session:${s.latest_version_id}`} className="block break-words text-sm text-ink [overflow-wrap:anywhere]">{s.title || s.session_id}</span>
        <p className="mt-1 break-words text-xs text-ink-3 [overflow-wrap:anywhere]">{s.provider_namespace} · {s.session_id} · {s.version_count.toLocaleString()} supporting {s.version_count === 1 ? "version" : "versions"} · Latest captured {dateLabel(s.last_captured_at)}</p>
      </li>)}
    </ul>}
    {read.data?.truncated && <p className="text-xs text-ink-3">More supporting sessions are outside this bounded page.</p>}
    <div className="flex flex-wrap gap-2">
      {nextCursor && <Button data-library-focus="node-sessions-next" onClick={() => onPage(nextCursor)}>Next sessions</Button>}
      {after && <Button data-library-focus="node-sessions-first" variant="ghost" onClick={() => onPage(null)}>First sessions</Button>}
    </div>
  </section>;
}
