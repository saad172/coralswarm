"use client";

/* Graph canvas — the Sigma.js (WebGL) renderer behind /graph.

   Division of labour: graphology owns the graph data (built once per ocean by
   toGraphology.ts), Sigma owns drawing, ForceAtlas2 owns positions (in a web
   worker, for a bounded time, then frozen), and React owns ONLY the chrome
   around it. Nothing graph-sized ever passes through React state: hover /
   focus / layer visibility reach Sigma through its node/edge reducers, which
   are pure functions re-installed when those inputs change.

   Layout is deterministic in practice: seeds come from an id hash
   (toGraphology.ts), the ocean hub is pinned, and the worker is stopped after
   a fixed budget — so the same ocean converges to the same picture on every
   load and never drifts under the user's cursor.

   Client-only: WebGL needs a browser, so the page imports this through
   `next/dynamic(..., { ssr: false })`. That also keeps Sigma out of the
   build-time prerender. */

import "@react-sigma/core/lib/style.css";
import {
  Component,
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
  type MutableRefObject,
  type ReactNode,
} from "react";
import type { Sigma } from "sigma";
import type { Settings } from "sigma/settings";
import { EdgeRectangleProgram, NodeCircleProgram } from "sigma/rendering";
import EdgeCurveProgram from "@sigma/edge-curve";
import { createNodeBorderProgram } from "@sigma/node-border";
import {
  ControlsContainer,
  SigmaContainer,
  ZoomControl,
  useCamera,
  useLoadGraph,
  useRegisterEvents,
  useSetSettings,
  useSigma,
} from "@react-sigma/core";
import { useWorkerLayoutForceAtlas2 } from "@react-sigma/layout-forceatlas2";
import { MiniMap } from "@react-sigma/minimap";
import { inferSettings } from "graphology-layout-forceatlas2";
import type { AtlasNodeType } from "@/lib/api";
import {
  neighborhood,
  placeHalo,
  shortLabel,
  visibleBBox,
  type BuiltGraph,
  type GraphEdgeAttrs,
  type GraphNodeAttrs,
  type Palette,
} from "./toGraphology";
import { readPalette, withAlpha } from "./palette";

let webglCached: boolean | null = null;
function webglAvailable(): boolean {
  if (typeof document === "undefined") return false;
  if (webglCached !== null) return webglCached;
  try {
    const c = document.createElement("canvas");
    webglCached = !!(c.getContext("webgl2") || c.getContext("webgl") || c.getContext("experimental-webgl"));
  } catch {
    webglCached = false;
  }
  return webglCached;
}

/* ── Public surface ──────────────────────────────────────────────────────── */

export interface GraphCanvasHandle {
  /** Ease the camera onto a node (zooming in as needed). */
  flyTo: (id: string) => void;
  /** Ease the camera back to the whole-ocean view. */
  fit: () => void;
  /** Redraw after an out-of-band attribute change (e.g. an edge's status). */
  refresh: () => void;
  /** Sigma's own graphology instance — `useLoadGraph` copies into this. */
  sigmaGraph: () => import("graphology").default<GraphNodeAttrs, GraphEdgeAttrs> | undefined;
}

export interface GraphCanvasProps {
  built: BuiltGraph;
  /** Per node-type visibility (layer toggles). The ocean hub is always shown. */
  visible: Record<AtlasNodeType, boolean>;
  /** Draw the halo of unlinked reefs/decisions (faded, unlabelled). */
  showUnlinked: boolean;
  focusId: string | null;
  /** A picked edge: its two endpoints and the edge itself stay lit. */
  focusEdge?: string | null;
  hoverId: string | null;
  onPickNode: (id: string | null) => void;
  onPickEdge: (key: string | null) => void;
  onHover: (id: string | null) => void;
  /** Fires once the layout worker has been stopped for this `built`. */
  onSettled?: () => void;
  className?: string;
}

/** Nodes whose RENDERED size is below this carry no label. At zoom 1 that
 *  is every unlinked reef/decision (≤5), every coral (2.4–3.6) and people
 *  (6): only connected reefs are named in the overview, and the rest surface
 *  their labels as you zoom in. */
const LABEL_THRESHOLD = 6.5;

/** Wall-clock budget for ForceAtlas2, scaled by node count and capped. A
 *  frozen layout is the point: after this the picture stops moving. */
export function layoutBudgetMs(order: number): number {
  return Math.min(5000, 900 + order * 1.1);
}

const GraphCanvas = forwardRef<GraphCanvasHandle, GraphCanvasProps>(function GraphCanvas(
  {
    built,
    visible,
    showUnlinked,
    focusId,
    focusEdge = null,
    hoverId,
    onPickNode,
    onPickEdge,
    onHover,
    onSettled,
    className,
  },
  ref,
) {
  const palette = useMemo(readPalette, []);
  const sigmaRef = useRef<Sigma<GraphNodeAttrs, GraphEdgeAttrs> | null>(null);
  const cameraRef = useRef<{ flyTo: (id: string) => void; fit: () => void } | null>(null);

  useImperativeHandle(
    ref,
    () => ({
      flyTo: (id) => cameraRef.current?.flyTo(id),
      fit: () => cameraRef.current?.fit(),
      refresh: () => sigmaRef.current?.refresh(),
      sigmaGraph: () => sigmaRef.current?.getGraph(),
    }),
    [],
  );

  /* Sigma settings are fixed for the component's life: programs, label
     typography, camera bounds. Reducers are installed separately (below)
     because they change with hover/focus/layers. */
  const settings = useMemo<Partial<Settings<GraphNodeAttrs, GraphEdgeAttrs>>>(
    () => ({
      allowInvalidContainer: true,
      renderLabels: true,
      renderEdgeLabels: false,
      labelFont: palette.sans,
      labelSize: 12,
      labelWeight: "500",
      labelColor: { color: palette.ink },
      labelRenderedSizeThreshold: LABEL_THRESHOLD,
      // Sparser label grid than Sigma's default: one label per ~130px cell,
      // biggest node wins. Truncation (reducer below) keeps them from
      // colliding horizontally across cells.
      labelDensity: 0.05,
      labelGridCellSize: 130,
      zIndex: true,
      enableEdgeEvents: true,
      defaultNodeType: "circle",
      defaultEdgeType: "line",
      nodeProgramClasses: {
        circle: NodeCircleProgram,
        // Promoted reefs + sources: a ring in `borderColor` around the fill.
        bordered: createNodeBorderProgram({
          borders: [
            { size: { value: 0.16 }, color: { attribute: "borderColor", defaultValue: palette.ink2 } },
            { size: { fill: true }, color: { attribute: "color" } },
          ],
        }),
      },
      edgeProgramClasses: {
        // Rectangle (not the 1px gl.LINES program) so `size` is honoured —
        // the confirmed/suggested width difference is part of the encoding.
        line: EdgeRectangleProgram,
        curve: EdgeCurveProgram,
      },
      minCameraRatio: 0.02,
      maxCameraRatio: 6,
      stagePadding: 48,
      // Nodes render at their pixel size regardless of zoom, but shrink a
      // little when zoomed far out so the constellation reads as a whole.
      zoomToSizeRatioFunction: (ratio) => Math.pow(ratio, 0.6),
    }),
    [palette],
  );

  if (!webglAvailable()) {
    return (
      <div
        className={className}
        style={{ width: "100%", height: "100%" }}
        role="status"
      >
        <div className="grid h-full place-items-center px-[24px] text-center font-mono text-[12px] text-ink-3">
          This browser cannot draw the graph — WebGL is unavailable. The layers
          and search still work.
        </div>
      </div>
    );
  }

  return (
    <CanvasBoundary>
    <SigmaContainer<GraphNodeAttrs, GraphEdgeAttrs>
      ref={sigmaRef}
      settings={settings}
      className={className}
      style={{ width: "100%", height: "100%", background: "transparent" }}
    >
      <GraphLoader built={built} />
      <LayoutRunner built={built} visible={visible} showUnlinked={showUnlinked} onSettled={onSettled} />
      <Framing built={built} visible={visible} showUnlinked={showUnlinked} />
      <Reducers
        built={built}
        visible={visible}
        showUnlinked={showUnlinked}
        focusId={focusId}
        focusEdge={focusEdge}
        hoverId={hoverId}
        palette={palette}
      />
      <Events onPickNode={onPickNode} onPickEdge={onPickEdge} onHover={onHover} />
      <CameraBridge target={cameraRef} />
      <ControlsContainer position="bottom-right" className="cs-graph-controls">
        <ZoomControl labels={{ zoomIn: "Zoom in", zoomOut: "Zoom out", reset: "Fit" }} />
      </ControlsContainer>
      <ControlsContainer position="bottom-left" className="cs-graph-minimap">
        <SafeMiniMap />
      </ControlsContainer>
    </SigmaContainer>
    </CanvasBoundary>
  );
});

class CanvasBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  render() {
    if (!this.state.failed) return this.props.children;
    return (
      <div className="grid h-full place-items-center px-[24px] text-center font-mono text-[12px] text-ink-3">
        The graph canvas failed to start. Layers and search still work.
      </div>
    );
  }
}

export default GraphCanvas;

/* ── Children (must live inside SigmaContainer) ──────────────────────────── */

/** Replace Sigma's graph whenever a new ocean model arrives. `loadGraph`
 *  clears and re-imports INTO Sigma's own graphology instance, so the layout
 *  supervisor and reducers bound to that instance stay valid. */
function GraphLoader({ built }: { built: BuiltGraph }) {
  const loadGraph = useLoadGraph<GraphNodeAttrs, GraphEdgeAttrs>();
  useEffect(() => {
    // eslint-disable-next-line no-restricted-syntax -- Sigma's in-memory graph import of data already fetched, not a network read
    loadGraph(built.graph, true);
  }, [built, loadGraph]);
  return null;
}

/** ForceAtlas2 in a worker, run for a bounded budget, then frozen. Restarts
 *  only when `built` changes (a new ocean), never on token refresh or chrome
 *  re-renders. */
function LayoutRunner({
  built,
  visible,
  showUnlinked,
  onSettled,
}: {
  built: BuiltGraph;
  visible: Record<AtlasNodeType, boolean>;
  showUnlinked: boolean;
  onSettled?: () => void;
}) {
  const sigma = useSigma<GraphNodeAttrs, GraphEdgeAttrs>();
  const order = built.graph.order;
  // Read at settle time, not captured: a layer toggled during the run must
  // not restart the layout, but the final framing should honour it.
  const framingRef = useRef({ visible, showUnlinked });
  framingRef.current = { visible, showUnlinked };
  const fa2 = useMemo(() => {
    const inferred = inferSettings(order);
    return {
      settings: {
        ...inferred,
        // Mild gravity: enough to keep the connected core compact around the
        // pinned centre, weak enough that the edgeless halo (seeded outside)
        // stays outside for the length of the run.
        // Hub-aware FA2 (connectivity §9.1): strong gravity, high
        // scalingRatio, outbound attraction so a leftover hub cannot collapse
        // the constellation into a star.
        gravity: 0.3,
        strongGravityMode: true,
        scalingRatio: 60,
        outboundAttractionDistribution: true,
        // No node overlap correction: it is O(n²)-ish and the seeded corals
        // already sit in a disc around their reef.
        adjustSizes: false,
        // Edges pull in proportion to their `weight` attribute: typed
        // currents 1, coral membership 0.5, attribution 0.25, hub spokes 0 —
        // so structure, not "belongs to this ocean", decides where things go.
        edgeWeightInfluence: 1,
      },
      getEdgeWeight: "weight" as const,
    };
  }, [order]);
  const { start, stop } = useWorkerLayoutForceAtlas2(fa2);
  const settledRef = useRef(onSettled);
  settledRef.current = onSettled;

  useEffect(() => {
    // The supervisor respawns its worker on the next tick after the graph
    // import fires its add events; start after that so the first iteration
    // sees the whole graph.
    const t0 = window.setTimeout(() => start(), 60);
    const t1 = window.setTimeout(() => {
      stop();
      // The halo has no edges, so the layout never had an opinion about it:
      // ring it around the core we just measured, then frame what's shown.
      placeHalo(sigma.getGraph());
      const { visible: v, showUnlinked: u } = framingRef.current;
      frameVisible(sigma, v, u, 350);
      settledRef.current?.();
    }, 60 + layoutBudgetMs(order));
    return () => {
      window.clearTimeout(t0);
      window.clearTimeout(t1);
      stop();
    };
    // `start`/`stop` are stable per supervisor; re-run only for a new graph.
  }, [built, order, start, stop, sigma]);
  return null;
}

/** Point Sigma's coordinate frame at the visible nodes and ease the camera
 *  to the whole of it. Every later reset (Fit button, zoom-control reset,
 *  `f`) then frames the same box, since Sigma's `animatedReset` goes to the
 *  custom bbox when one is set. */
function frameVisible(
  sigma: Sigma<GraphNodeAttrs, GraphEdgeAttrs>,
  visible: Record<AtlasNodeType, boolean>,
  showUnlinked: boolean,
  duration: number,
) {
  const box = visibleBBox(sigma.getGraph(), visible, showUnlinked);
  sigma.setCustomBBox(box);
  sigma.getCamera().animatedReset({ duration });
}

/** Re-frame when a layer is switched on/off. Skips the first render (the
 *  layout runner frames once it has positions) and hover/focus changes
 *  (those don't touch this component's props). */
function Framing({
  built,
  visible,
  showUnlinked,
}: {
  built: BuiltGraph;
  visible: Record<AtlasNodeType, boolean>;
  showUnlinked: boolean;
}) {
  const sigma = useSigma<GraphNodeAttrs, GraphEdgeAttrs>();
  const first = useRef(true);
  const lastBuilt = useRef(built);
  useEffect(() => {
    if (first.current || lastBuilt.current !== built) {
      // New graph or first mount: LayoutRunner owns the framing.
      first.current = false;
      lastBuilt.current = built;
      return;
    }
    frameVisible(sigma, visible, showUnlinked, 300);
  }, [built, visible, showUnlinked, sigma]);
  return null;
}

/** Hover/focus dimming + layer visibility, expressed as Sigma reducers. */
function Reducers({
  built,
  visible,
  showUnlinked,
  focusId,
  focusEdge,
  hoverId,
  palette,
}: {
  built: BuiltGraph;
  visible: Record<AtlasNodeType, boolean>;
  showUnlinked: boolean;
  focusId: string | null;
  focusEdge: string | null;
  hoverId: string | null;
  palette: Palette;
}) {
  const setSettings = useSetSettings<GraphNodeAttrs, GraphEdgeAttrs>();
  const sigma = useSigma<GraphNodeAttrs, GraphEdgeAttrs>();
  const [decisionDots, setDecisionDots] = useState(() => sigma.getCamera().ratio >= 0.7);
  const activeId = hoverId ?? focusId;

  useEffect(() => {
    const cam = sigma.getCamera();
    const onUpdated = () => {
      const next = cam.ratio >= 0.7;
      setDecisionDots((prev) => (prev === next ? prev : next));
    };
    onUpdated();
    cam.on("updated", onUpdated);
    return () => {
      cam.off("updated", onUpdated);
    };
  }, [sigma]);

  useEffect(() => {
    const graph = built.graph;
    // The lit set: a node's neighbourhood, or — with no active node — the two
    // endpoints of a picked edge.
    const edgeLit = !activeId && focusEdge && graph.hasEdge(focusEdge) ? graph.extremities(focusEdge) : null;
    const lit =
      activeId && graph.hasNode(activeId) ? neighborhood(graph, activeId) : edgeLit ? new Set(edgeLit) : null;
    // Only force labels on a small lit set — a hub with 800 neighbours would
    // otherwise paint 800 labels on top of each other.
    const forceLabels = lit !== null && lit.size <= 24;
    const dimNode = (c: string) => withAlpha(c, 0.16);
    const dimEdge = (c: string) => withAlpha(c, 0.12);
    const isHalo = (d: GraphNodeAttrs) => !d.linked && (d.nodeType === "reef" || d.nodeType === "decision");

    // Sigma REPLACES display data with what a reducer returns, so each
    // branch spreads `data` first and overrides on top.
    setSettings({
      nodeReducer: (node, data) => {
        if (data.nodeType !== "ocean" && !visible[data.nodeType]) {
          return { ...data, hidden: true };
        }
        if (!showUnlinked && isHalo(data)) {
          return { ...data, hidden: true };
        }
        // Decisions collapse to dots at fit zoom (high camera ratio) so
        // 7k claims don't paint as a sentence halo. Full nodes once you
        // zoom in, or when the decision is the active one.
        const decisionDot = data.nodeType === "decision" && decisionDots && node !== activeId;
        const label = decisionDot ? "" : node === activeId ? data.label : shortLabel(data.label);
        if (decisionDot && !lit) {
          return { ...data, label, size: 2.2 };
        }
        if (!lit) return { ...data, label };
        if (lit.has(node)) {
          return {
            ...data,
            label,
            zIndex: (data.zIndex ?? 0) + 20,
            forceLabel: forceLabels,
            highlighted: node === activeId || edgeLit !== null,
          };
        }
        return {
          ...data,
          color: dimNode(data.color),
          // The promoted-reef ring lives in `borderColor`; dim it with the fill
          // or it stays at full strength as a floating hoop.
          borderColor: data.borderColor ? dimNode(data.borderColor) : undefined,
          label: "",
          zIndex: 0,
        };
      },
      edgeReducer: (edge, data) => {
        const [s, t] = graph.extremities(edge);
        const touchesActive =
          (activeId !== null && (s === activeId || t === activeId)) || (edgeLit !== null && edge === focusEdge);
        // Attribution edges (coral→source, coral→author) only appear for the
        // active node; drawn always they would be pure hairball.
        if (data.attribution && !touchesActive) {
          return { ...data, hidden: true };
        }
        if (!lit) return data;
        if (touchesActive) {
          return {
            ...data,
            size: data.size * 1.7,
            zIndex: (data.zIndex ?? 0) + 20,
            color: data.status === "suggested" ? palette.amber : palette.ink2,
          };
        }
        return { ...data, color: dimEdge(data.color), zIndex: 0 };
      },
    });
  }, [built, visible, showUnlinked, activeId, focusEdge, palette, setSettings, decisionDots]);
  return null;
}

/** Sigma → page. Registered once; callbacks are read through refs so a
 *  re-render of the page never re-binds listeners. */
function Events({
  onPickNode,
  onPickEdge,
  onHover,
}: Pick<GraphCanvasProps, "onPickNode" | "onPickEdge" | "onHover">) {
  const register = useRegisterEvents<GraphNodeAttrs, GraphEdgeAttrs>();
  const cb = useRef({ onPickNode, onPickEdge, onHover });
  cb.current = { onPickNode, onPickEdge, onHover };

  useEffect(() => {
    register({
      clickNode: ({ node }) => cb.current.onPickNode(node),
      clickEdge: ({ edge }) => cb.current.onPickEdge(edge),
      clickStage: () => {
        cb.current.onPickNode(null);
        cb.current.onPickEdge(null);
      },
      enterNode: ({ node }) => cb.current.onHover(node),
      leaveNode: () => cb.current.onHover(null),
    });
  }, [register]);
  return null;
}

/** The minimap is a SECOND Sigma instance, and Sigma throws if its container
 *  has no height at construction. That happens on a Fast Refresh remount and
 *  can happen on any first paint where the control strip isn't laid out yet.
 *  Two guards: mount one frame late (container measured), and fence it with
 *  an error boundary so a minimap failure never takes the canvas down — the
 *  minimap is a convenience, the canvas is the feature. */
function SafeMiniMap() {
  const [ready, setReady] = useState(false);
  useEffect(() => {
    const raf = requestAnimationFrame(() => setReady(true));
    return () => cancelAnimationFrame(raf);
  }, []);
  if (!ready) return <div style={{ width: 160, height: 110 }} />;
  return (
    <MiniMapBoundary>
      <MiniMap width="160px" height="110px" />
    </MiniMapBoundary>
  );
}

class MiniMapBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };
  static getDerivedStateFromError() {
    return { failed: true };
  }
  render() {
    return this.state.failed ? null : this.props.children;
  }
}

/** Exposes camera moves to the imperative handle on the outer component. */
function CameraBridge({
  target,
}: {
  target: MutableRefObject<{ flyTo: (id: string) => void; fit: () => void } | null>;
}) {
  const sigma = useSigma<GraphNodeAttrs, GraphEdgeAttrs>();
  const { gotoNode, reset } = useCamera({ duration: 450 });
  const flyTo = useCallback(
    (id: string) => {
      if (!sigma.getGraph().hasNode(id)) return;
      // Land close enough that the node's label is legible even for a coral.
      const ratio = Math.min(sigma.getCamera().ratio, 0.35);
      const pos = sigma.getNodeDisplayData(id);
      if (pos) sigma.getCamera().animate({ x: pos.x, y: pos.y, ratio }, { duration: 450 });
      else gotoNode(id);
    },
    [sigma, gotoNode],
  );
  const fit = useCallback(() => reset({ duration: 350 }), [reset]);
  useEffect(() => {
    target.current = { flyTo, fit };
    return () => {
      target.current = null;
    };
  }, [target, flyTo, fit]);
  return null;
}
