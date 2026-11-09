"use client";

/* Chrome around the Graph canvas: the legend glyphs, the layers panel, node
   search, the node / edge detail panels and the ocean switcher. All React,
   all design-system primitives; none of it touches Sigma directly — the page
   wires their callbacks to the canvas handle. */

import Link from "next/link";
import { forwardRef, useEffect, useImperativeHandle, useMemo, useRef, useState, type ReactNode } from "react";
import type { AtlasNodeType, Graph } from "@/lib/api";
import { oceanBadge } from "@/lib/askScope";
import { predicateVerb } from "@/lib/predicates";
import { Badge, Button, Chip, Dot, MenuItem, Popover, cn, type Hue } from "@/components/ui";
import { connectionsOf, type BuiltGraph, type Connection, type GraphNodeAttrs, type HueName } from "./toGraphology";

export const TYPE_NAME: Record<AtlasNodeType, string> = {
  ocean: "Workspace",
  reef: "Topic",
  decision: "Decision",
  coral: "Item",
  source: "Source",
  person: "Person",
};

export const LAYERS: { key: Exclude<AtlasNodeType, "ocean">; label: string; sub: string; hue: Hue }[] = [
  { key: "reef", label: "Topics", sub: "entities · threads · references", hue: "pink" },
  { key: "decision", label: "Decisions", sub: "called out", hue: "coral" },
  { key: "coral", label: "Items", sub: "atoms", hue: "teal" },
  { key: "source", label: "Sources", sub: "platforms", hue: "amber" },
  { key: "person", label: "People", sub: "authors", hue: "plum" },
];

const LINK_VERB: Record<string, string> = {
  "ocean-reef": "in workspace",
  "ocean-decision": "in workspace",
  "reef-coral": "holds item",
  "coral-source": "cited from",
  authored: "authored by",
  "decision-reef": "concerns",
  supports: "supported by",
  member: "member of",
  "reef-reef": "linked topic",
  "co-occurs": "share items",
};

const OCEAN_HUE: Record<Graph["kind"], Hue> = { personal: "coral", team: "teal", org: "plum", member: "pink" };

function hueCss(h: HueName | "ink" | Hue): string {
  return h === "ink" ? "var(--ink)" : `var(--hue-${h})`;
}

/* ── Glyph ───────────────────────────────────────────────────────────────── */

/** Tiny legend mark per node type, in the node's hue. Mirrors the canvas
 *  encoding: reef = ring, decision = diamond, coral = dot, source = tile,
 *  person = filled disc. */
export function Glyph({ type, hue, size = 18 }: { type: AtlasNodeType; hue: HueName | "ink" | Hue; size?: number }) {
  const c = hueCss(hue);
  const box = { width: size, height: size };
  const inner = (style: React.CSSProperties) => (
    <span className="grid flex-none place-items-center" style={box}>
      <span style={style} />
    </span>
  );
  switch (type) {
    case "reef":
      return inner({
        width: size * 0.72,
        height: size * 0.72,
        borderRadius: "50%",
        background: `color-mix(in srgb, ${c} 30%, var(--bg))`,
        border: `2px solid ${c}`,
      });
    case "decision":
      return inner({ width: size * 0.6, height: size * 0.6, background: c, transform: "rotate(45deg)", borderRadius: 2 });
    case "coral":
      return inner({ width: size * 0.44, height: size * 0.44, borderRadius: "50%", background: `color-mix(in srgb, ${c} 55%, var(--bg))` });
    case "source":
      return inner({
        width: size * 0.78,
        height: size * 0.78,
        borderRadius: 4,
        background: `color-mix(in srgb, ${c} 12%, var(--bg))`,
        border: `1.5px solid ${c}`,
      });
    case "ocean":
      return inner({ width: size * 0.9, height: size * 0.9, borderRadius: "50%", background: c });
    case "person":
    default:
      return inner({
        width: size * 0.78,
        height: size * 0.78,
        borderRadius: "50%",
        background: `color-mix(in srgb, ${c} 50%, var(--bg))`,
        border: `1.5px solid ${c}`,
      });
  }
}

/* ── Floating panel shell ────────────────────────────────────────────────── */

export function Float({ className, children }: { className?: string; children: ReactNode }) {
  return (
    <div
      className={cn(
        "rounded-card border border-line bg-bg/90 shadow-[var(--shadow-2)] backdrop-blur-[8px]",
        className,
      )}
    >
      {children}
    </div>
  );
}

/* ── Layers ──────────────────────────────────────────────────────────────── */

export function LayersPanel({
  counts,
  visible,
  onToggle,
  showUnlinked,
  onToggleUnlinked,
  cooccur,
  onCooccur,
  showAllAnchors,
  onToggleAnchors,
}: {
  counts: BuiltGraph["counts"];
  visible: Record<AtlasNodeType, boolean>;
  onToggle: (k: AtlasNodeType) => void;
  showUnlinked: boolean;
  onToggleUnlinked: () => void;
  cooccur: number;
  onCooccur: (n: number) => void;
  showAllAnchors: boolean;
  onToggleAnchors: () => void;
}) {
  return (
    <Float className="w-[212px] p-[10px]">
      <div className="mb-[8px] px-[6px] font-mono text-[10px] uppercase tracking-[0.16em] text-ink-3">layers</div>
      <div className="flex flex-col gap-[2px]">
        {LAYERS.map((t) => (
          <LayerRow
            key={t.key}
            on={visible[t.key]}
            onClick={() => onToggle(t.key)}
            glyph={<Glyph type={t.key} hue={t.hue} />}
            label={t.label}
            sub={t.sub}
            count={counts[t.key]}
          />
        ))}
        {counts.unlinked > 0 && (
          <>
            <div className="mx-[6px] my-[4px] border-t border-line" />
            <LayerRow
              on={showUnlinked}
              onClick={onToggleUnlinked}
              glyph={
                <span className="grid flex-none place-items-center" style={{ width: 18, height: 18 }}>
                  <span
                    className="rounded-full"
                    style={{ width: 7, height: 7, background: "color-mix(in srgb, var(--hue-pink) 42%, var(--bg))" }}
                  />
                </span>
              }
              label="Unlinked"
              sub="no currents yet · the outer halo"
              count={counts.unlinked}
            />
          </>
        )}
        <div className="mx-[6px] my-[4px] border-t border-line" />
        <label className="flex flex-col gap-[6px] px-[6px] py-[6px]">
          <span className="text-[13px] font-medium text-ink">Co-occurrence</span>
          <span className="font-mono text-[9.5px] text-ink-3">share at least {cooccur} item{cooccur === 1 ? "" : "s"}</span>
          <input
            type="range"
            min={1}
            max={5}
            step={1}
            value={cooccur}
            onChange={(e) => onCooccur(Number(e.target.value))}
            className="h-[4px] w-full cursor-pointer appearance-none rounded-pill bg-line accent-brand"
            aria-label="Co-occurrence threshold"
          />
        </label>
        <LayerRow
          on={showAllAnchors}
          onClick={onToggleAnchors}
          glyph={
            <span className="grid flex-none place-items-center" style={{ width: 18, height: 18 }}>
              <span className="rounded-badge" style={{ width: 8, height: 8, background: "var(--hue-coral)" }} />
            </span>
          }
          label="Show all anchors"
          sub="top-2 by default · decisions → topics"
          count={counts.decision}
        />
      </div>
    </Float>
  );
}

function LayerRow({
  on,
  onClick,
  glyph,
  label,
  sub,
  count,
}: {
  on: boolean;
  onClick: () => void;
  glyph: React.ReactNode;
  label: string;
  sub: string;
  count: number;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={on}
      onClick={onClick}
      className={cn(
        "flex w-full items-center gap-[10px] rounded-row px-[6px] py-[6px] text-left transition-opacity hover:bg-bg-hover",
        !on && "opacity-45",
      )}
    >
      {glyph}
      <span className="min-w-0 flex-1">
        <span className="block text-[13px] font-medium text-ink">{label}</span>
        <span className="block font-mono text-[9.5px] text-ink-3">{sub}</span>
      </span>
      <span className="font-mono text-[11px] text-ink-3">{count}</span>
      <span
        className={cn(
          "flex h-[16px] w-[28px] flex-none rounded-pill p-[2px]",
          on ? "justify-end bg-brand" : "justify-start bg-bg-3",
        )}
      >
        <span className="h-[12px] w-[12px] rounded-full bg-bg shadow-[var(--shadow-1)]" />
      </span>
    </button>
  );
}

/* ── Search ──────────────────────────────────────────────────────────────── */

export interface SearchHandle {
  focus: () => void;
  close: () => void;
}

export const GraphSearch = forwardRef<SearchHandle, { built: BuiltGraph; onJump: (id: string) => void }>(
  function GraphSearch({ built, onJump }, ref) {
    const [q, setQ] = useState("");
    const [open, setOpen] = useState(false);
    const boxRef = useRef<HTMLDivElement | null>(null);
    const inputRef = useRef<HTMLInputElement | null>(null);

    useEffect(() => {
      const h = (e: MouseEvent) => {
        if (boxRef.current && !boxRef.current.contains(e.target as Node)) setOpen(false);
      };
      document.addEventListener("mousedown", h);
      return () => document.removeEventListener("mousedown", h);
    }, []);

    const results = useMemo(() => {
      const s = q.trim().toLowerCase();
      if (!s) return [] as { id: string; a: GraphNodeAttrs }[];
      const out: { id: string; a: GraphNodeAttrs }[] = [];
      built.graph.forEachNode((id, a) => {
        if (out.length >= 9 || a.nodeType === "ocean") return;
        if (a.label.toLowerCase().includes(s)) out.push({ id, a });
      });
      return out;
    }, [q, built]);

    useImperativeHandle(ref, () => ({
      focus: () => {
        inputRef.current?.focus();
        inputRef.current?.select();
      },
      close: () => {
        setOpen(false);
        inputRef.current?.blur();
      },
    }));

    const pick = (id: string) => {
      onJump(id);
      setOpen(false);
    };

    return (
      <div ref={boxRef} className="relative w-[240px]">
        <div className="flex items-center gap-[8px] rounded-pill border border-line bg-bg px-[12px] py-[7px]">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" className="text-ink-3">
            <circle cx="11" cy="11" r="7" />
            <path d="m20 20-3.5-3.5" />
          </svg>
          <input
            ref={inputRef}
            value={q}
            onChange={(e) => {
              setQ(e.target.value);
              setOpen(true);
            }}
            onFocus={() => setOpen(true)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && results.length > 0) {
                e.preventDefault();
                pick(results[0].id);
              }
            }}
            placeholder="Find a node…"
            aria-label="Find a node"
            className="flex-1 bg-transparent text-[13px] text-ink outline-none placeholder:text-ink-3"
          />
        </div>
        {open && results.length > 0 && (
          <div className="absolute left-0 right-0 top-[calc(100%+6px)] z-[80] max-h-[320px] overflow-y-auto rounded-card border border-line bg-bg p-[6px] shadow-[var(--shadow-3)]">
            {results.map(({ id, a }) => (
              <button
                key={id}
                type="button"
                onClick={() => pick(id)}
                className="flex w-full items-center gap-[10px] rounded-row px-[10px] py-[7px] text-left hover:bg-bg-hover"
              >
                <Glyph type={a.nodeType} hue={a.hue} />
                <span className="min-w-0 flex-1 truncate text-[13px] text-ink">{a.label}</span>
                <span className="font-mono text-[9.5px] uppercase tracking-[0.06em] text-ink-3">{TYPE_NAME[a.nodeType]}</span>
              </button>
            ))}
          </div>
        )}
      </div>
    );
  },
);

/* ── Ocean switcher ──────────────────────────────────────────────────────── */

export function OceanMenu({
  graphs,
  current,
  onPick,
}: {
  graphs: Graph[];
  current: Graph;
  onPick: (id: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const trig = useRef<HTMLButtonElement | null>(null);
  return (
    <div className="relative">
      <button
        ref={trig}
        type="button"
        onClick={() => setOpen((v) => !v)}
        aria-haspopup="menu"
        aria-expanded={open}
        className="flex items-center gap-[9px] rounded-pill border border-line bg-bg px-[13px] py-[7px] shadow-[var(--shadow-1)] hover:bg-bg-hover"
      >
        <Dot hue={OCEAN_HUE[current.kind]} size={9} />
        <span className="text-[13.5px] font-medium text-ink">{current.name}</span>
        <span className="font-mono text-[9px] uppercase tracking-[0.08em] text-ink-3">{oceanBadge(current)}</span>
        <span className="font-mono text-[11px] text-ink-3">▾</span>
      </button>
      <Popover open={open} onClose={() => setOpen(false)} triggerRef={trig} className="min-w-[240px]">
        {graphs.map((g) => (
          <MenuItem
            key={g.id}
            active={g.id === current.id}
            onClick={() => {
              onPick(g.id);
              setOpen(false);
            }}
          >
            <Dot hue={OCEAN_HUE[g.kind]} size={8} />
            <span className="min-w-0 flex-1 truncate">{g.name}</span>
            <span className="font-mono text-[9px] uppercase tracking-[0.08em] text-ink-3">{oceanBadge(g)}</span>
          </MenuItem>
        ))}
      </Popover>
    </div>
  );
}

/* ── Edge review row (shared by both panels) ─────────────────────────────── */

export type EdgeReviewState = { busy: boolean; error: string | null };

function StatusPill({ status }: { status?: string }) {
  if (status === "suggested") return <Badge tone="amber">suggested</Badge>;
  if (status === "confirmed") return <Badge tone="teal">confirmed</Badge>;
  return null;
}

function pct(c?: number): string | null {
  if (c == null || Number.isNaN(c)) return null;
  return `${Math.round(Math.max(0, Math.min(1, c)) * 100)}%`;
}

function shareCopy(shared?: number): string {
  const n = shared ?? 0;
  return n === 1 ? "share 1 item" : `share ${n} items`;
}

function isTypedCurrent(kind: string): boolean {
  return kind === "reef-reef" || kind === "decision-reef" || kind === "co-occurs";
}

export function EdgeReviewActions({
  edgeKey,
  status,
  state,
  onConfirm,
  onReject,
  canReview,
}: {
  edgeKey: string;
  status?: string;
  state: EdgeReviewState | undefined;
  onConfirm: (edgeKey: string) => void;
  onReject: (edgeKey: string) => void;
  canReview: boolean;
}) {
  if (status !== "suggested" || !canReview) return null;
  return (
    <div className="mt-[6px] flex items-center gap-[6px]">
      <Button size="sm" variant="primary" disabled={state?.busy} onClick={() => onConfirm(edgeKey)}>
        Confirm
      </Button>
      <Button size="sm" variant="ghost" disabled={state?.busy} onClick={() => onReject(edgeKey)}>
        Reject
      </Button>
      {state?.error && <span className="font-mono text-[10px] text-bad">{state.error}</span>}
    </div>
  );
}

/* ── Node detail ─────────────────────────────────────────────────────────── */

const GROUP_ORDER: AtlasNodeType[] = ["ocean", "decision", "reef", "coral", "source", "person"];

export function NodePanel({
  id,
  built,
  graphs,
  review,
  onClose,
  onJump,
  onPickEdge,
  onConfirm,
  onReject,
  canReview,
  rev,
}: {
  id: string;
  built: BuiltGraph;
  graphs: Graph[];
  review: Record<string, EdgeReviewState>;
  onClose: () => void;
  onJump: (id: string) => void;
  onPickEdge: (key: string) => void;
  onConfirm: (edgeKey: string) => void;
  onReject: (edgeKey: string) => void;
  canReview: boolean;
  /** Bumped by the page after out-of-band graph mutations. */
  rev: number;
}) {
  const g = built.graph;
  const node = g.hasNode(id) ? g.getNodeAttributes(id) : null;
  const conns = useMemo(() => {
    void rev;
    return connectionsOf(g, id);
  }, [g, id, rev]);
  if (!node) return null;

  const typed = conns.filter((c) => isTypedCurrent(c.attrs.kind));
  const others = conns.filter((c) => !isTypedCurrent(c.attrs.kind));
  const groups: Partial<Record<AtlasNodeType, Connection[]>> = {};
  for (const c of others) {
    const t = g.getNodeAttribute(c.other, "nodeType");
    (groups[t] ??= []).push(c);
  }
  const promotedFrom = node.promotedFrom ? graphs.find((x) => x.id === node.promotedFrom) : null;
  const openHref =
    node.nodeType === "reef" || node.nodeType === "decision"
      ? `/topics?id=${encodeURIComponent(id)}`
      : node.nodeType === "coral"
        ? `/coral?id=${encodeURIComponent(id)}`
        : null;

  return (
    <Float className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-line px-[16px] pb-[12px] pt-[14px]">
        <div className="mb-[10px] flex items-center justify-between">
          <span className="inline-flex items-center gap-[8px] font-mono text-[9.5px] uppercase tracking-[0.14em] text-ink-3">
            <Glyph type={node.nodeType} hue={node.hue} />
            {node.nodeType === "reef" ? `${node.reefKind ?? "entity"} topic` : TYPE_NAME[node.nodeType]}
          </span>
          <button type="button" onClick={onClose} aria-label="Close" className="px-[4px] text-[18px] leading-none text-ink-3 hover:text-ink">
            ×
          </button>
        </div>
        <div className="font-display text-[22px] leading-[1.15] tracking-[-0.01em] text-ink">{node.label}</div>
        <div className="mt-[6px] font-mono text-[11px] text-ink-3">
          {node.sub}
          {node.count != null ? ` · ${node.count} items` : ""}
        </div>
        <div className="mt-[10px] flex flex-wrap items-center gap-[6px]">
          {openHref && (
            <Link href={openHref} className="inline-flex">
              <Button size="sm" variant="secondary">
                {node.nodeType === "coral" ? "Open item" : "Open topic"}
              </Button>
            </Link>
          )}
          {node.promotedFrom && (
            <Chip hue="plum" className="max-w-full">
              promoted from {promotedFrom ? promotedFrom.name : "another workspace"}
            </Chip>
          )}
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto px-[16px] pb-[16px] pt-[12px]">
        {typed.length > 0 && (
          <section className="mb-[16px]">
            <div className="mb-[7px] font-mono text-[9.5px] uppercase tracking-[0.1em] text-ink-3">
              Currents · {typed.length}
            </div>
            <div className="flex flex-col gap-[6px]">
              {typed.map((c) => {
                const otherLabel = g.getNodeAttribute(c.other, "label");
                const verb =
                  c.attrs.origin === "derived"
                    ? (LINK_VERB[c.attrs.kind] ?? "share items")
                    : predicateVerb(c.attrs.predicate);
                const conf = pct(c.attrs.confidence);
                return (
                  <div key={c.edgeKey} className="rounded-row border border-line bg-bg px-[10px] py-[8px]">
                    <button
                      type="button"
                      onClick={() => onPickEdge(c.edgeKey)}
                      className="block w-full text-left text-[12.5px] leading-[1.4] text-ink hover:underline"
                    >
                      {c.outgoing ? (
                        <>
                          <span className="text-ink-3">{node.label}</span> {verb} <b className="font-medium">{otherLabel}</b>
                        </>
                      ) : (
                        <>
                          <b className="font-medium">{otherLabel}</b> {verb} <span className="text-ink-3">{node.label}</span>
                        </>
                      )}
                    </button>
                    <div className="mt-[6px] flex flex-wrap items-center gap-[6px] font-mono text-[10px] text-ink-3">
                      {c.attrs.origin === "derived" ? (
                        <span>{shareCopy(c.attrs.shared)}</span>
                      ) : (
                        <>
                          <StatusPill status={c.attrs.status} />
                          {c.attrs.origin && <span>{c.attrs.origin}</span>}
                          {conf && <span>{conf}</span>}
                        </>
                      )}
                      <button type="button" onClick={() => onJump(c.other)} className="ml-auto text-ink-2 hover:text-ink">
                        go to →
                      </button>
                    </div>
                    {c.attrs.origin !== "derived" && (
                      <EdgeReviewActions
                        edgeKey={c.edgeKey}
                        status={c.attrs.status}
                        state={review[c.edgeKey]}
                        onConfirm={onConfirm}
                        onReject={onReject}
                        canReview={canReview}
                      />
                    )}
                  </div>
                );
              })}
            </div>
          </section>
        )}

        <div className="mb-[10px] font-mono text-[10px] uppercase tracking-[0.14em] text-ink-3">
          {conns.length} connections
        </div>
        {GROUP_ORDER.map((t) => {
          const list = groups[t];
          if (!list) return null;
          return (
            <section key={t} className="mb-[14px]">
              <div className="mb-[6px] font-mono text-[9.5px] uppercase tracking-[0.1em] text-ink-3">
                {TYPE_NAME[t]}s · {list.length}
              </div>
              <div className="flex flex-col gap-[4px]">
                {list.slice(0, 14).map((c) => {
                  const a = g.getNodeAttributes(c.other);
                  return (
                    <button
                      key={c.edgeKey}
                      type="button"
                      onClick={() => onJump(c.other)}
                      className="flex items-center gap-[9px] rounded-row border border-line bg-bg px-[9px] py-[6px] text-left hover:bg-bg-hover"
                      style={{ borderLeft: `2px solid ${hueCss(a.hue)}` }}
                    >
                      <span className="min-w-0 flex-1 truncate text-[12.5px] leading-[1.35] text-ink">{a.label}</span>
                      <span className="whitespace-nowrap font-mono text-[9px] text-ink-3">{LINK_VERB[c.attrs.kind] ?? ""}</span>
                    </button>
                  );
                })}
                {list.length > 14 && (
                  <span className="pl-[9px] font-mono text-[10px] text-ink-3">+{list.length - 14} more</span>
                )}
              </div>
            </section>
          );
        })}
        {conns.length === 0 && <p className="text-[12.5px] text-ink-3">Nothing links here yet.</p>}
      </div>
    </Float>
  );
}

/* ── Edge detail ─────────────────────────────────────────────────────────── */

export function EdgePanel({
  edgeKey,
  built,
  review,
  onClose,
  onJump,
  onConfirm,
  onReject,
  canReview,
  rev,
}: {
  edgeKey: string;
  built: BuiltGraph;
  review: Record<string, EdgeReviewState>;
  onClose: () => void;
  onJump: (id: string) => void;
  onConfirm: (edgeKey: string) => void;
  onReject: (edgeKey: string) => void;
  canReview: boolean;
  rev: number;
}) {
  const g = built.graph;
  void rev;
  if (!g.hasEdge(edgeKey)) return null;
  const e = g.getEdgeAttributes(edgeKey);
  const [s, t] = g.extremities(edgeKey);
  const src = g.getNodeAttributes(s);
  const dst = g.getNodeAttributes(t);
  const typed = isTypedCurrent(e.kind);
  const verb =
    e.origin === "derived" || e.kind === "co-occurs"
      ? (LINK_VERB[e.kind] ?? "share items")
      : typed
        ? predicateVerb(e.predicate)
        : (LINK_VERB[e.kind] ?? "linked to");
  const conf = pct(e.confidence);

  return (
    <Float className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-line px-[16px] pb-[12px] pt-[14px]">
        <div className="mb-[10px] flex items-center justify-between">
          <span className="font-mono text-[9.5px] uppercase tracking-[0.14em] text-ink-3">
            {typed ? "current" : "link"} · {e.kind}
          </span>
          <button type="button" onClick={onClose} aria-label="Close" className="px-[4px] text-[18px] leading-none text-ink-3 hover:text-ink">
            ×
          </button>
        </div>
        <div className="text-[16px] leading-[1.35] text-ink">
          <button type="button" onClick={() => onJump(s)} className="font-medium hover:underline">
            {src.label}
          </button>{" "}
          <span className="text-ink-2">{verb}</span>{" "}
          <button type="button" onClick={() => onJump(t)} className="font-medium hover:underline">
            {dst.label}
          </button>
        </div>
        <div className="mt-[10px] flex flex-wrap items-center gap-[6px] font-mono text-[10.5px] text-ink-3">
          {e.origin === "derived" ? (
            <span>{shareCopy(e.shared)}</span>
          ) : (
            <>
              <StatusPill status={e.status} />
              {e.origin && <span>{e.origin === "stated" ? "stated in items" : "inferred"}</span>}
              {conf && <span>confidence {conf}</span>}
            </>
          )}
        </div>
        {e.origin !== "derived" && (
          <EdgeReviewActions
            edgeKey={edgeKey}
            status={e.status}
            state={review[edgeKey]}
            onConfirm={onConfirm}
            onReject={onReject}
            canReview={canReview}
          />
        )}
      </div>
      <div className="px-[16px] py-[12px] text-[12.5px] leading-[1.55] text-ink-2">
        {e.origin === "derived" ? (
          <p>
            {e.kind === "decision-reef"
              ? "A derived anchor: this decision and topic share items. It is not a stated current, so there is nothing to confirm or reject."
              : "A derived current: these topics share items. It is not a stated current, so there is nothing to confirm or reject."}
          </p>
        ) : typed ? (
          e.status === "suggested" ? (
            <p>
              The relationship engine inferred this from how the two topics co-occur. Confirming keeps it as a fact
              answers can rely on; rejecting removes it from the graph.
            </p>
          ) : (
            <p>A confirmed current. Answers may cite this relationship directly.</p>
          )
        ) : (
          <p>A structural link: how this workspace&apos;s material hangs together, not a claim about the world.</p>
        )}
      </div>
    </Float>
  );
}
