"use client";

/* Graph — unlisted per-ocean canvas (3.2 you-hub / co-occur / anchors).

   Not in the rail. Founder: too laggy, too much FA2 movement to ship as a
   place. Typed URL still works; `NEXT_PUBLIC_GRAPH_PAGE=true` lists it in
   nav for local inspect. Backend atlas extras stay on for Ask/Atlas JSON.

   Rendering is Sigma.js (WebGL) behind components/graph/GraphCanvas.tsx,
   loaded client-only because WebGL needs a browser and this route is part of
   the build-time prerender. This file
   owns the chrome around it: ocean switcher, layer toggles, search, the node /
   edge detail panels, keyboard shortcuts and the review calls.

   OCEAN SELECTION IS PAGE-LOCAL (`?ocean=`), defaulting to the boot-time
   active ocean. It deliberately does NOT call `setCurrentGraphId`: the topics
   page documents that setter has zero callers by design, and flipping the
   global active ocean from here would silently re-scope Chats and Today.

   `?focus=<nodeId>` deep-links to a node; the fly-to waits for the layout
   worker to settle so it lands on the node's final position rather than
   its seed. */

import dynamic from "next/dynamic";
import { Suspense, useCallback, useEffect, useMemo, useRef, useState, type ComponentType, type RefAttributes } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { api, type AtlasGraph, type AtlasNodeType } from "@/lib/api";
import { writeErrorMessage } from "@/lib/editAccess";
import { atlasQueryOptions, invalidate, invalidationFor } from "@/lib/appQueries";
import { Button, Empty, ErrorNote, Loading } from "@/components/ui";
import { buildGraphModel, applyEdgeStatus, type BuiltGraph } from "@/components/graph/toGraphology";
import { readPalette } from "@/components/graph/palette";
import type { GraphCanvasHandle, GraphCanvasProps } from "@/components/graph/GraphCanvas";
import {
  EdgePanel,
  GraphSearch,
  LayersPanel,
  NodePanel,
  OceanMenu,
  type EdgeReviewState,
  type SearchHandle,
} from "@/components/graph/GraphChrome";

const GraphCanvas = dynamic(() => import("@/components/graph/GraphCanvas"), {
  ssr: false,
  loading: () => (
    <div className="grid h-full place-items-center">
      <Loading what="warming up the canvas" />
    </div>
  ),
}) as ComponentType<GraphCanvasProps & RefAttributes<GraphCanvasHandle>>;

const ALL_VISIBLE: Record<AtlasNodeType, boolean> = {
  ocean: true,
  reef: true,
  decision: true,
  coral: true,
  source: true,
  person: true,
};

function GraphScreenInner() {
  const { token, graphs, currentGraph, authReady, getAuthToken, queryScope } = useApp();
  const params = useSearchParams();
  const router = useRouter();
  const qc = useQueryClient();

  /* ── Ocean: `?ocean=` if it names a reachable ocean, else the active one. */
  const oceanParam = params.get("ocean");
  const ocean = useMemo(
    () => (oceanParam && graphs.find((g) => g.id === oceanParam)) || currentGraph,
    [oceanParam, graphs, currentGraph],
  );
  const pickOcean = useCallback(
    (id: string) => {
      const q = new URLSearchParams(params.toString());
      q.set("ocean", id);
      q.delete("focus");
      router.replace(`/graph?${q.toString()}`);
    },
    [params, router],
  );

  /* ── Data. The Clerk token refreshes about once a minute; keeping it in a
     ref (not a dependency) means a refresh never refetches the atlas and
     snaps the camera/selection back mid-exploration. */
  const tokenRef = useRef(token);
  tokenRef.current = token;
  const hasToken = !!token;
  // null = let the server pick (2, bump to 3 on the edge budget).
  const [cooccurOverride, setCooccurOverride] = useState<number | null>(null);
  const [showAllAnchors, setShowAllAnchors] = useState(false);
  // Cached per ocean and knob setting; the bearer is read inside the query,
  // so a refresh never refetches the atlas and snaps the camera back.
  const atlasQuery = useQuery({
    ...atlasQueryOptions({ getAuthToken, scope: queryScope }, ocean?.id ?? "", { cooccur: cooccurOverride, allAnchors: showAllAnchors }),
    enabled: authReady && !!ocean,
  });
  const raw: AtlasGraph | null = atlasQuery.data ?? null;
  const loading = !atlasQuery.error && atlasQuery.data === undefined;
  const err = atlasQuery.error ? (atlasQuery.error instanceof Error ? atlasQuery.error.message : "failed to load the graph") : null;

  const palette = useMemo(readPalette, []);
  const built: BuiltGraph | null = useMemo(() => (raw ? buildGraphModel(raw, palette) : null), [raw, palette]);
  // Bumped after out-of-band graphology mutations (confirm/reject) so the
  // panels, which read the graph directly, re-render.
  const [rev, setRev] = useState(0);

  /* ── Selection / view state. */
  const [visible, setVisible] = useState(ALL_VISIBLE);
  // The halo of unlinked reefs/decisions is drawn (faded) by default — it is
  // real data — but it is the first thing to switch off on a busy ocean.
  const [showUnlinked, setShowUnlinked] = useState(true);
  const [focusId, setFocusId] = useState<string | null>(null);
  const [edgeKey, setEdgeKey] = useState<string | null>(null);
  const [hoverId, setHoverId] = useState<string | null>(null);
  const canvasRef = useRef<GraphCanvasHandle | null>(null);
  const searchRef = useRef<SearchHandle | null>(null);

  // Reset selection when the ocean changes; the graph underneath is new.
  useEffect(() => {
    setFocusId(null);
    setEdgeKey(null);
    setHoverId(null);
  }, [ocean?.id]);

  const jump = useCallback((id: string) => {
    setEdgeKey(null);
    setFocusId(id);
    canvasRef.current?.flyTo(id);
  }, []);

  /* ── `?focus=` deep link: honoured once, after the layout settles for this
     graph, then dropped from the URL so a reload doesn't re-fly. */
  const focusParam = params.get("focus");
  const pendingFocus = useRef<string | null>(focusParam);
  useEffect(() => {
    if (focusParam) pendingFocus.current = focusParam;
  }, [focusParam]);
  const onSettled = useCallback(() => {
    const id = pendingFocus.current;
    if (!id || !built?.graph.hasNode(id)) return;
    pendingFocus.current = null;
    jump(id);
    if (typeof window !== "undefined") {
      const url = new URL(window.location.href);
      url.searchParams.delete("focus");
      window.history.replaceState(null, "", `${url.pathname}${url.search}`);
    }
  }, [built, jump]);

  /* ── Keyboard: "/" search, "f" fit, Escape clears. Ignored while typing;
     never hijacks Cmd/Ctrl chords (⌘K, ⌘F). */
  const fit = useCallback(() => {
    setFocusId(null);
    setEdgeKey(null);
    canvasRef.current?.fit();
  }, []);
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        setFocusId(null);
        setEdgeKey(null);
        searchRef.current?.close();
        return;
      }
      const target = e.target as HTMLElement | null;
      const tag = target?.tagName;
      if (target?.isContentEditable || tag === "INPUT" || tag === "TEXTAREA") return;
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      if (e.key === "/") {
        e.preventDefault();
        searchRef.current?.focus();
      } else if (e.key === "f" || e.key === "F") {
        fit();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [fit]);

  /* ── Edge review. Optimistic: restyle the edge (or drop it) immediately,
     revert on failure. Rejected edges are never emitted by the atlas, so
     dropping matches what a reload would show. */
  const [review, setReview] = useState<Record<string, EdgeReviewState>>({});
  const setEdgeState = (k: string, s: EdgeReviewState | null) =>
    setReview((r) => {
      const next = { ...r };
      if (s) next[k] = s;
      else delete next[k];
      return next;
    });

  const confirmEdge = useCallback(
    async (key: string) => {
      const t = tokenRef.current;
      if (!t || !built || !built.graph.hasEdge(key)) return;
      const edgeId = built.graph.getEdgeAttribute(key, "edgeId");
      if (!edgeId) return;
      setEdgeState(key, { busy: true, error: null });
      applyEdgeStatus(built.graph, key, "confirmed", palette);
      const sg = canvasRef.current?.sigmaGraph();
      if (sg?.hasEdge(key)) applyEdgeStatus(sg, key, "confirmed", palette);
      canvasRef.current?.refresh();
      setRev((n) => n + 1);
      try {
        await api.relationships.confirm(edgeId, t);
        setEdgeState(key, null);
        // The canvas already shows it; mark the cached atlas stale (without
        // refetching it under the camera) so the next visit is not pre-review.
        if (ocean) void invalidate(qc, invalidationFor.edgeReviewed(ocean.id), { refetchType: "none" });
      } catch (e) {
        applyEdgeStatus(built.graph, key, "suggested", palette);
        const sg = canvasRef.current?.sigmaGraph();
        if (sg?.hasEdge(key)) applyEdgeStatus(sg, key, "suggested", palette);
        canvasRef.current?.refresh();
        setRev((n) => n + 1);
        setEdgeState(key, { busy: false, error: writeErrorMessage(e, "couldn’t confirm") });
      }
    },
    [built, palette, ocean, qc],
  );

  const rejectEdge = useCallback(
    async (key: string) => {
      const t = tokenRef.current;
      if (!t || !built || !built.graph.hasEdge(key)) return;
      const g = built.graph;
      const edgeId = g.getEdgeAttribute(key, "edgeId");
      if (!edgeId) return;
      const [s, d] = g.extremities(key);
      const attrs = g.getEdgeAttributes(key);
      setEdgeState(key, { busy: true, error: null });
      g.dropEdge(key);
      const sg = canvasRef.current?.sigmaGraph();
      if (sg?.hasEdge(key)) sg.dropEdge(key);
      if (edgeKey === key) setEdgeKey(null);
      canvasRef.current?.refresh();
      setRev((n) => n + 1);
      try {
        await api.relationships.reject(edgeId, t);
        setEdgeState(key, null);
        if (ocean) void invalidate(qc, invalidationFor.edgeReviewed(ocean.id), { refetchType: "none" });
      } catch (e) {
        if (!g.hasEdge(key)) g.addEdgeWithKey(key, s, d, attrs);
        const sg = canvasRef.current?.sigmaGraph();
        if (sg && !sg.hasEdge(key)) sg.addEdgeWithKey(key, s, d, attrs);
        canvasRef.current?.refresh();
        setRev((n) => n + 1);
        setEdgeState(key, { busy: false, error: writeErrorMessage(e, "couldn’t reject") });
      }
    },
    [built, edgeKey, ocean, qc],
  );

  const toggleLayer = (k: AtlasNodeType) => setVisible((v) => ({ ...v, [k]: !v[k] }));

  /* ── Render. */
  if (!ocean) {
    return (
      <div className="grid h-full place-items-center p-[40px]">
        <Empty title="No workspace selected" hint="Pick a workspace to see its knowledge graph." />
      </div>
    );
  }

  const empty = built !== null && built.counts.total <= 1;

  return (
    <div className="flex h-full min-h-0 flex-col">
      {/* toolbar */}
      <div className="flex flex-none flex-wrap items-center gap-[12px] border-b border-line-2 bg-bg px-[20px] py-[10px]">
        <OceanMenu graphs={graphs} current={ocean} onPick={pickOcean} />
        {built && (
          <span className="whitespace-nowrap font-mono text-[11px] text-ink-3">
            {built.counts.total} nodes · {built.counts.reef} topics · {built.counts.decision} decisions ·{" "}
            {built.counts.coral} items
            {built.counts.unlinked > 0 ? ` · ${built.counts.unlinked} unlinked` : ""}
            {built.truncated ? " · items sampled (newest 100)" : ""}
          </span>
        )}
        <div className="ml-auto flex flex-wrap items-center gap-[10px]">
          {built && !empty && <GraphSearch ref={searchRef} built={built} onJump={jump} />}
          <Button size="sm" variant="secondary" onClick={fit} title="Fit to view (f)">
            Fit
          </Button>
        </div>
      </div>

      {/* stage */}
      <div className="cs-graph-stage relative min-h-0 flex-1 bg-bg">
        {loading && !built ? (
          <div className="grid h-full place-items-center">
            <Loading what="charting the workspace" />
          </div>
        ) : err && !built ? (
          <div className="grid h-full place-items-center p-[40px]">
            <ErrorNote action={<Button size="sm" onClick={() => void atlasQuery.refetch()}>Retry</Button>}>
              Couldn’t load the graph: {err}
            </ErrorNote>
          </div>
        ) : built && empty ? (
          <div className="grid h-full place-items-center p-[40px]">
            <Empty
              title="This workspace is empty"
              hint="Capture some items and let topics form to see the graph fill in."
            />
          </div>
        ) : built ? (
          <>
            <div className="absolute inset-0">
              <GraphCanvas
                ref={canvasRef}
                built={built}
                visible={visible}
                showUnlinked={showUnlinked}
                focusId={focusId}
                focusEdge={edgeKey}
                hoverId={hoverId}
                onPickNode={(id) => {
                  setEdgeKey(null);
                  setFocusId(id);
                }}
                onPickEdge={(k) => {
                  if (k) setFocusId(null);
                  setEdgeKey(k);
                }}
                onHover={setHoverId}
                onSettled={onSettled}
              />
            </div>

            {/* layers — hidden on phones, where the panel would cover the canvas */}
            <div className="absolute left-[14px] top-[14px] z-[60] hidden md:block">
              <LayersPanel
                counts={built.counts}
                visible={visible}
                onToggle={toggleLayer}
                showUnlinked={showUnlinked}
                onToggleUnlinked={() => setShowUnlinked((v) => !v)}
                cooccur={cooccurOverride ?? built.cooccur}
                onCooccur={setCooccurOverride}
                showAllAnchors={showAllAnchors}
                onToggleAnchors={() => setShowAllAnchors((v) => !v)}
              />
            </div>

            {/* hint */}
            <div className="pointer-events-none absolute bottom-[14px] left-1/2 z-[55] hidden -translate-x-1/2 items-center gap-[12px] whitespace-nowrap rounded-pill bg-ink/85 px-[14px] py-[6px] font-mono text-[10.5px] tracking-[0.04em] text-bg shadow-[var(--shadow-2)] md:flex">
              <span>hover to trace</span>
              <span className="opacity-40">·</span>
              <span>click to focus</span>
              <span className="opacity-40">·</span>
              <span>scroll to zoom</span>
              <span className="opacity-40">·</span>
              <span>drag to pan</span>
              <span className="opacity-40">·</span>
              <span>/ to search</span>
            </div>

            {/* detail — stops short of the zoom controls in the bottom-right corner */}
            {(focusId || edgeKey) && (
              <div className="absolute bottom-[14px] right-[14px] top-[14px] z-[70] w-[min(320px,calc(100%-28px))] md:bottom-[124px]">
                {edgeKey ? (
                  <EdgePanel
                    edgeKey={edgeKey}
                    built={built}
                    review={review}
                    rev={rev}
                    onClose={() => setEdgeKey(null)}
                    onJump={jump}
                    onConfirm={confirmEdge}
                    onReject={rejectEdge}
                    canReview={hasToken}
                  />
                ) : focusId ? (
                  <NodePanel
                    id={focusId}
                    built={built}
                    graphs={graphs}
                    review={review}
                    rev={rev}
                    onClose={() => setFocusId(null)}
                    onJump={jump}
                    onPickEdge={(k) => {
                      setFocusId(null);
                      setEdgeKey(k);
                    }}
                    onConfirm={confirmEdge}
                    onReject={rejectEdge}
                    canReview={hasToken}
                  />
                ) : null}
              </div>
            )}
          </>
        ) : null}
      </div>
    </div>
  );
}

export default function GraphPage() {
  return (
    <Suspense fallback={<Loading what="charting the workspace" />}>
      <GraphScreenInner />
    </Suspense>
  );
}
