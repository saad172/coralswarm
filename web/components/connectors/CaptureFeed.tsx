"use client";
import { useState } from "react";
import { useInfiniteQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { connectorsApi, type CapturePage, type SourceSummary } from "@/lib/connectorsApi";
import { Button, Card, Seg } from "@/components/ui";
export function CaptureFeed({ provider, sources, initialPage, computedAt }: { provider: string; sources: SourceSummary[]; initialPage: CapturePage; computedAt: string }) {
 const { getAuthToken, queryScope, authReady } = useApp();
 const [source, setSource] = useState("");
 const q = useInfiniteQuery({ queryKey: ["connector-captures", queryScope, provider, computedAt, source], enabled: authReady && !!source, initialData: !source ? { pages: [initialPage], pageParams: [undefined] } : undefined, staleTime: Infinity, initialPageParam: undefined as string | undefined, queryFn: ({ pageParam }) => connectorsApi.captures(getAuthToken() ?? "", provider, { source: source || undefined, cursor: pageParam }), getNextPageParam: p => p.next_cursor ?? undefined });
 const items = [...new Map(q.data?.pages.flatMap(p => p.items).map(i => [i.atom_id, i])).values()];
 return <section className="space-y-4"><h2 className="font-display text-2xl">Recent captures</h2><Seg className="max-w-full flex-wrap" value={source} onChange={setSource} options={[{ key: "", label: "All sources" }, ...sources.map(s => ({ key: s.key, label: s.label }))]} />
 {q.isPending && <p role="status">Loading captures…</p>}{q.isError && <p role="alert">Couldn’t load captures. <Button onClick={() => void q.refetch()}>Retry</Button></p>}
 {!q.isPending && !q.isError && !items.length && <p>No captures yet.</p>}
 <Card className="!p-0 overflow-hidden"><div className="divide-y divide-line">{items.map(i => <details key={i.atom_id} className="p-4"><summary className="cursor-pointer"><span className="mb-1 block text-sm text-ink-3">{sources.find(s => s.key === i.source)?.label ?? i.source} · {i.author || "Unknown author"} · {new Date(i.captured_at).toLocaleString()}</span><span className="font-medium">{i.title || "Untitled capture"}</span></summary><div className="mt-3 space-y-3"><p className="whitespace-pre-wrap">{i.excerpt || "No preview available."}</p><div aria-label="Capture topics" className="flex flex-wrap gap-2">{i.topics.map(t => <span key={t.id} className="rounded-pill border border-line px-2 py-1">{t.label}</span>)}</div></div></details>)}</div></Card>
 {q.hasNextPage && <Button disabled={q.isFetching} onClick={() => void q.fetchNextPage()}>Load more</Button>}</section>;
}
