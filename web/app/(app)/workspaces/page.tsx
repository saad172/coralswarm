"use client";

import { Suspense, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useApp } from "@/lib/providers";
import { useQuery } from "@tanstack/react-query";
import { api, type Reef } from "@/lib/api";
import { workspaceReefsQueryOptions } from "@/lib/appQueries";
import { isOwnerOnlyOcean } from "@/lib/askScope";
import { Badge, Empty, ErrorNote, FacetRow, Loading, Notice, SearchInput } from "@/components/ui";
import { IconAsk } from "@/components/icons";
import { TopicCard } from "@/components/workspaces/TopicCard";
import { SelectionBar } from "@/components/workspaces/SelectionBar";
import { WorkspaceIndex } from "@/components/workspaces/WorkspaceIndex";
import { WorkspaceMembersSummary } from "@/components/workspaces/members/WorkspaceMembersSummary";
import { REEF_KIND_META, REEF_TYPES } from "@/components/workspaces/reefKinds";
import { PAGE_SIZE } from "@/components/library/libraryTopics";
import { PromoteModal, type PromoteTarget } from "@/components/PromoteModal";
import type { ReefType } from "@/lib/api";

/* /workspaces serves two screens off one route:

     no `?id=`   → the workspace INDEX (components/workspaces/WorkspaceIndex)
     `?id=<gid>` → screen 1e, that workspace's topic grid

   The index is not in the mock. The handoff only ever draws a single
   workspace, reached from the sidebar's "your workspaces" sub-list, so the
   top-level rail item had no destination of its own — it used to default into
   the personal ocean's topic grid, which reads as a narrower copy of Library.

   Data flow for the grid is ported from
   archive/web-legacy-ui/screens/reefs.page.tsx (api.reefs.listAll), trimmed to
   what a topic grid + facet filter + bulk promote actually needs. */
function WorkspaceScreenInner() {
  const params = useSearchParams();
  const idParam = params.get("id");
  // Set by `WorkspaceMembers`'s redirect after a successful archive (see its
  // `onArchived` handler) — the workspace name, carried across the redirect
  // since the manage page it archived is unreachable the instant this lands.
  // Read once and rendered only on the index branch below; a real Next.js
  // route change (not history replace) drops it the moment the user
  // navigates anywhere else, so it can't linger and re-show on a later visit.
  const archivedName = params.get("archived");
  const { token, authReady, getAuthToken, queryScope, graphs } = useApp();

  // Only resolve a workspace when one was actually asked for. With no `?id=`
  // this route is the INDEX (WorkspaceIndex below) — defaulting to the personal
  // ocean here made the top-level nav item land in one arbitrary workspace's
  // topic grid, which just reads as a narrower Library.
  const graph = idParam ? (graphs.find((g) => g.id === idParam) ?? null) : null;

  const graphId = graph?.id;

  const [q, setQ] = useState("");
  const [kind, setKind] = useState<ReefType | "all">("all");
  const [selected, setSelected] = useState<string[]>([]);
  const [promoteTarget, setPromoteTarget] = useState<PromoteTarget | null>(null);
  const [promoteMsg, setPromoteMsg] = useState<string | null>(null);

  /* Paged, for the same reason Library is: `listAll` drains every page until
     exhausted, and a workspace here holds 1,500+ topics. The kind filter goes
     to the SERVER so it can't hide matches that fell outside the loaded page.
     The first page is a cached query (a return paints instantly); "show more"
     pages are appended locally and belong to one (workspace, kind) only. */
  const query = useQuery({
    ...workspaceReefsQueryOptions({ getAuthToken, scope: queryScope }, graphId ?? "", kind === "all" ? null : kind, PAGE_SIZE),
    enabled: authReady && !!graphId,
  });
  const listKey = `${queryScope}|${graphId ?? ""}|${kind}|${query.data?.computedAt ?? "pending"}`;
  const [more, setMore] = useState<{ key: string; rows: Reef[]; page: number; hasMore: boolean } | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const moreController = useRef<AbortController | null>(null);
  const extra = query.data !== undefined && more?.key === listKey && !query.isPlaceholderData ? more : null;
  const reefs = useMemo(() => {
    const first = query.data?.data ?? [];
    if (!extra) return first;
    const seen = new Set(first.map((r) => r.node_id));
    return [...first, ...extra.rows.filter((r) => !seen.has(r.node_id))];
  }, [query.data, extra]);
  const hasMore = extra ? extra.hasMore : (query.data?.hasMore ?? false);
  const page = extra?.page ?? 1;
  const loading = !query.error && query.data === undefined;
  // A failed background refetch keeps the grid it already has.
  const err = query.isError && !query.data ? "Couldn't load this workspace's topics." : null;

  // A new workspace or kind starts a fresh selection, and any "show more"
  // read for the previous one is dropped.
  useEffect(() => {
    const controller = moreController;
    setSelected([]);
    setLoadingMore(false);
    return () => controller.current?.abort();
  }, [listKey]);

  async function loadMore() {
    const token = getAuthToken();
    if (!token || !graph || !query.data || loadingMore) return;
    const controller = new AbortController();
    moreController.current?.abort();
    moreController.current = controller;
    const key = listKey;
    setLoadingMore(true);
    try {
      const next = page + 1;
      const { data, hasMore } = await api.reefs.list(
        token,
        graph.id,
        kind === "all" ? undefined : kind,
        next,
        PAGE_SIZE,
        controller.signal,
      );
      if (controller.signal.aborted) return;
      setMore((cur) => ({ key, rows: [...(cur?.key === key ? cur.rows : []), ...data], page: next, hasMore }));
    } catch {
      /* best-effort: keep what's already shown */
    } finally {
      if (!controller.signal.aborted) setLoadingMore(false);
    }
  }

  const filtered = useMemo(() => {
    // `kind` is applied by the server (see the loading effect); only the text box narrows
    // further, and only across what's been loaded.
    let list = reefs;
    if (q.trim()) {
      const needle = q.trim().toLowerCase();
      list = list.filter((r) => r.title.toLowerCase().includes(needle));
    }
    return [...list].sort((a, b) => +new Date(b.updated_at) - +new Date(a.updated_at));
  }, [reefs, q]);

  const toggle = (id: string) =>
    setSelected((cur) => (cur.includes(id) ? cur.filter((x) => x !== id) : [...cur, id]));

  const ownerOnly = graph ? isOwnerOnlyOcean(graph) : false;
  const selectedReefs = reefs.filter((r) => selected.includes(r.node_id));
  const selectedSourceCount = selectedReefs.reduce((n, r) => n + r.atom_count, 0);

  // No `?id=` → the index. An `?id=` that matches nothing (a stale link, or a
  // workspace you've lost access to) also falls through to it rather than
  // 404ing, so the user lands somewhere they can act.
  if (!graph) {
    return (
      <>
        {archivedName && (
          <Notice className="mb-[16px]">
            <b className="font-medium text-ink">{archivedName}</b> archived. Nothing inside was touched —
            an account owner or organization admin can restore it within 120 days.
          </Notice>
        )}
        <WorkspaceIndex />
      </>
    );
  }

  return (
    <>
      <div className="mb-[14px] flex flex-wrap items-center gap-[8px]">
        <Link href="/workspaces" className="text-[13px] text-ink-3 transition-colors hover:text-ink">
          Workspaces
        </Link>
        <span className="text-ink-4">/</span>
        <span className="truncate text-[13px] text-ink">{graph.name}</span>
      </div>

      <div className="mb-[16px] flex items-center gap-[11px]">
        <span className="text-[15px] font-semibold tracking-[-0.01em] text-ink">{graph.name}</span>
        <Badge tone={ownerOnly ? "brand" : "ink"} outline>
          {graph.kind}
        </Badge>
        <span className="text-[13px] text-ink-3">
          {loading ? "Loading topics" : err ? "Topics unavailable" : `${reefs.length}${hasMore ? "+" : ""} topic${reefs.length === 1 ? "" : "s"}`}
          {ownerOnly ? " · only you" : ""}
        </span>
      </div>

      {/* Compact, read-only members summary — a "workspace" IS an ocean, and
          a SHARED one is kind === "team" (see the vocabulary note atop this
          file). Personal, member, and org oceans never render this:
          personal/member have no roster to manage, and org membership is the
          whole account, handled from Settings → Organization → Members
          instead. Every mutation (rename, add/remove, roles, leave, history)
          now lives on its own page — see /workspaces/manage?id= — so this
          screen only ever links there. */}
      {graph.kind === "team" && <WorkspaceMembersSummary graph={graph} />}

      <div className="mb-[16px]">
        <div className="mb-[8px] max-w-[280px]">
          <SearchInput
            icon={<IconAsk width={13} height={13} />}
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Filter topics…"
          />
        </div>
        <FacetRow
          options={[
            { key: "all", label: "All" },
            ...REEF_TYPES.map((t) => ({
              key: t,
              label: REEF_KIND_META[t].facet,
              dot: <span className="h-[6px] w-[6px] rounded-pill" style={{ background: `var(--hue-${REEF_KIND_META[t].hue})` }} />,
            })),
          ]}
          value={kind}
          onChange={setKind}
        />
      </div>

      {err && <ErrorNote className="mb-[16px]">{err} <button type="button"
        onClick={() => void query.refetch()} className="underline">Retry topics</button></ErrorNote>}
      {promoteMsg && <div className="mb-[16px] font-mono text-[11px] text-ok">{promoteMsg}</div>}

      {loading ? (
        <Loading what="surfacing topics" />
      ) : err ? null : filtered.length === 0 ? (
        <Empty
          title={reefs.length === 0 ? "Nothing here yet" : "No matches"}
          hint={
            reefs.length === 0
              ? "Ask a question or add a source and topics will start forming here."
              : "Try a different filter or search term."
          }
        />
      ) : (
        <div className="grid grid-cols-1 gap-[12px] pb-[24px] sm:grid-cols-2">
          {filtered.map((r) => (
            <TopicCard key={r.node_id} reef={r} selected={selected.includes(r.node_id)} onToggle={() => toggle(r.node_id)} />
          ))}
        </div>
      )}

      {hasMore && !q.trim() && (
        <div className="mt-[14px] flex justify-center">
          <button
            type="button"
            onClick={() => void loadMore()}
            disabled={loadingMore}
            className="rounded-pill border border-line bg-bg px-[16px] py-[7px] font-mono text-[11px] text-ink-2 transition-colors hover:border-ink-3 hover:text-ink disabled:opacity-50"
          >
            {loadingMore ? "loading…" : `show ${PAGE_SIZE} more ↓`}
          </button>
        </div>
      )}

      {selected.length > 0 && (
        <div className="sticky bottom-[16px] z-10 mt-[8px]">
          <SelectionBar
            count={selected.length}
            sourceCount={selectedSourceCount}
            onClear={() => setSelected([])}
            onShare={() => {
              setPromoteMsg(null);
              setPromoteTarget({ kind: "reefs", reefs: selectedReefs });
            }}
          />
        </div>
      )}

      {promoteTarget && (
        <PromoteModal
          target={promoteTarget}
          oceans={graphs}
          currentGraphId={graph.id}
          token={token}
          onClose={() => setPromoteTarget(null)}
          onPromoted={(oceanNames, summary) => {
            setSelected([]);
            setPromoteMsg(`Shared ${summary.reefs ?? 0} topic(s) with ${oceanNames.join(", ")}.`);
          }}
        />
      )}
    </>
  );
}

export default function WorkspacesPage() {
  return (
    <Suspense fallback={<Loading what="loading workspaces" />}>
      <WorkspaceScreenInner />
    </Suspense>
  );
}
