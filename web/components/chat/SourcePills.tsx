"use client";
import { DURATION_MS, EASE } from "@/lib/motion";
import { verdictCopy } from "./answer/labels";
import { useState } from "react";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { rankByCitations, type EvidenceSource } from "./evidence";
import { sourceButtonLabel } from "./answer/labels";
import { KIND_META, type DetailKind } from "../detail/detailModel";
import { hueVar } from "@/components/ui";
export function SourcePills({
  sources,
  text,
  onSelect,
  children,
  tileKeys,
  hot,
  setHot,
}: {
  sources: EvidenceSource[];
  text: string;
  onSelect: (n: number) => void;
  children?: React.ReactNode;
  tileKeys?: string[];
  hot?: number | null;
  setHot?: (n: number | null) => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const reduced = useReducedMotion();
  const ranked = rankByCitations(text, sources);
  const extra = tileKeys
    ? ranked.filter((s) => !tileKeys.includes(s.sourceKey)).length
    : Math.max(0, ranked.length - 3);
  const button = (s: EvidenceSource, row = false) => {
    const meta =
      KIND_META[(s.sourceType && s.sourceType in KIND_META ? s.sourceType : "note") as DetailKind];
    const Icon = meta.Icon;
    return (
      <button
        onMouseEnter={() => setHot?.(s.n)}
        onMouseLeave={() => setHot?.(null)}
        aria-pressed={hot === s.n}
        data-src
        key={s.n}
        type="button"
        aria-label={sourceButtonLabel(s.n, s.title, s.verdict)}
        onClick={() => onSelect(s.n)}
        className={`inline-flex max-w-full items-center gap-2 rounded-pill border px-3 py-2 text-xs hover:bg-bg-hover ${hot === s.n ? "border-brand bg-brand-soft" : "border-line"}`}
      >
        <Icon width={13} height={13} style={{ color: hueVar(meta.hue) }} />
        <span className="truncate">{s.title}</span>
        <span>{s.n}</span>
        {row && <span>{meta.label}{s.date ? ` · ${s.date}` : ""}</span>}
        {s.verdict && <span>{verdictCopy(s.verdict)?.label}</span>}
      </button>
    );
  };
  return (
    <section aria-label="Answer sources" className="space-y-3">
      {children ?? <div className="flex flex-wrap gap-2">{ranked.slice(0, 3).map(s => button(s))}</div>}
      {extra > 0 && (
        <button
          type="button"
          aria-expanded={expanded}
          onClick={() => setExpanded(!expanded)}
          className="text-xs text-ink-3"
        >
          {expanded ? "show less" : `+${extra} more`}
          <motion.span aria-hidden="true" className="ml-1 inline-block" animate={{ rotate: expanded ? 180 : 0 }} transition={{ duration: reduced ? 0 : 0.2 }}>⌄</motion.span>
        </button>
      )}
      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: reduced ? 0 : DURATION_MS.collapse / 1000, ease: EASE.out }}
            className="overflow-hidden"
          >
            <div className="flex flex-col items-start gap-2">
              {[...sources].sort((a, b) => a.n - b.n).map(s => button(s, true))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </section>
  );
}
