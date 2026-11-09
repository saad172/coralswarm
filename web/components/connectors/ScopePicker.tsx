"use client";
import { verifiedScopeItems } from "@/lib/connectorChoiceProof";
import { connectorActionRecoveryMs, SCOPE_PAGES_PER_ACTION } from "@/lib/connectorActionPolicy";
import { useEffect, useState, useRef } from "react";
import { Button, Card, Checkbox, Seg, SearchInput, CountPill } from "@/components/ui";
import { connectorsApi, type SourceSummary, type ScopeSelection, type ScopeItem } from "@/lib/connectorsApi";

export function ScopePicker({ getAuthToken, provider, source, value, onChange, onReady, preserveSelection = false, actionDeadline }: {
  getAuthToken: () => string | null; provider: string; source: SourceSummary; value: ScopeSelection;
  actionDeadline?: number;
  preserveSelection?: boolean; onChange: (value: ScopeSelection) => void; onReady?: (ready: boolean) => void;
}) {
  const latestValue = useRef(value);
  latestValue.current = value;
  const [items, setItems] = useState<ScopeItem[]>([]);
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(false);
  // Load all pages before editing so saving cannot silently discard selected
  // items outside the visible page. Search is local over that complete set.
  const [q, setQ] = useState("");
  const [retry, setRetry] = useState(0);
  const checkpoint = useRef<{ identity: string; items: ScopeItem[]; cursor?: string; seen: Set<string> } | null>(null);
  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    const identity = JSON.stringify([provider, source.key]);
    if (checkpoint.current?.identity !== identity) checkpoint.current = { identity, items: [], seen: new Set() };
    const progress = checkpoint.current;
    const deadline = retry === 0 && actionDeadline !== undefined ? actionDeadline : Date.now() + connectorActionRecoveryMs();
    const expiry = setTimeout(() => { controller.abort(); if (!cancelled) { setError(true); setLoading(false); onReady?.(false); } }, Math.max(0, deadline - Date.now()));
    if (source.scope_shape === "everything_only") { clearTimeout(expiry); onReady?.(true); return; }
    setLoading(true); setError(false); onReady?.(false);
    (async () => {
      const all = progress.items;
      let cursor = progress.cursor;
      let pages = 0;
      do {
        if (controller.signal.aborted || Date.now() >= deadline || pages >= SCOPE_PAGES_PER_ACTION) throw new Error("Source choices need an explicit retry.");
        const page = verifiedScopeItems(await connectorsApi.scopeItems(getAuthToken() ?? "", provider, source.key, { cursor }, controller.signal));
        if (controller.signal.aborted || Date.now() >= deadline) throw new Error("Source choices expired.");
        if (page.next_cursor !== null && (progress.seen.has(page.next_cursor) || page.next_cursor === cursor)) throw new Error("Source cursor repeated.");
        pages++; all.push(...page.items); cursor = page.next_cursor ?? undefined;
        if (cursor) progress.seen.add(cursor);
        progress.cursor = cursor;
      } while (cursor);
      if (!cancelled) {
        setItems(all);
        if (!preserveSelection) onChange({ ...latestValue.current, item_ids: all.filter(i => i.selected).map(i => i.id) });
        onReady?.(true);
      }
    })().catch(() => { if (!cancelled) setError(true); }).finally(() => { clearTimeout(expiry); if (!cancelled) setLoading(false); });
    return () => { cancelled = true; clearTimeout(expiry); controller.abort(); };
    // Selection changes must not reload and overwrite edits.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [provider, source.key, retry]);
  const visible = items.filter(i => i.label.toLowerCase().includes(q.toLowerCase()));
  return <div className="space-y-4">
    <Seg className="max-w-full flex-wrap" options={[
      ...(source.scope_shape !== "everything_only" ? [{ key: "pick" as const, label: "Only what I pick" }] : []),
      ...(source.scope_shape !== "pick_only" ? [{ key: "everything" as const, label: "Everything I can see" }] : []),
    ]} value={value.mode} onChange={mode => onChange({ ...value, mode })} />
    {value.mode === "everything" && <Card><h3 className="mb-2 font-medium">This includes</h3><p className="text-sm text-ink-3">{provider === "github" ? "Captures the currently granted repositories selected for this connection. Later repository grants require selection." : /calendar/.test(source.key) ? "Captures calendar events available to this connected account, including the automatic history window." : /gmail|outlook|mail/.test(source.key) ? "Captures mail available through this connected account’s Everything mode." : source.scope_shape === "everything_only" ? `Captures ${source.label.toLowerCase()} across this connected workspace with its granted access.` : `Captures all accessible ${source.label.toLowerCase()} for this connection.`}</p></Card>}
    {value.mode === "pick" && /gmail|outlook|mail/.test(source.key) && <p className="text-sm text-ink-3">Follow threads from the CoralSwarm add-in in Gmail or Outlook.</p>}
    {loading && <p role="status">Loading sources…</p>}
    {error && <div role="alert">Couldn’t load the available items. <Button onClick={() => setRetry(retry + 1)}>Retry</Button></div>}
    {value.mode === "pick" && !loading && !error && <>
      <SearchInput aria-label="Search items" placeholder="Search items…" className="w-full rounded-control border border-line bg-bg p-2" value={q} onChange={e => setQ(e.target.value)} />
      <CountPill>{value.item_ids?.length ?? 0} selected</CountPill>
      <Button onClick={() => onChange({ ...value, item_ids: [...new Set([...(value.item_ids ?? []), ...visible.map(i => i.id)])] })}>Select all visible</Button>
      {!visible.length && <p>No items found. You can continue with nothing selected.</p>}
      {visible.map(i => <div key={i.id}><Checkbox label={i.label} checked={value.item_ids?.includes(i.id) ?? false} onChange={checked => onChange({ ...value, item_ids: checked ? [...(value.item_ids ?? []), i.id] : value.item_ids?.filter(id => id !== i.id) })} /></div>)}
    </>}
  </div>;
}
