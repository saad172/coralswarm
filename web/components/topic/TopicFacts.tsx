import Link from "next/link";
import { Eyebrow, StackedMeter, hueVar } from "@/components/ui";
import { ENTITY_KIND_META, entityKindOf } from "./topicMeta";
import type { ReefDetail } from "@/lib/api";
import { hueForSource, sourceMeta } from "@/components/library/sourceKind";

export function TopicFacts({ reef, graphName }: { reef: ReefDetail; graphName: string }) {
  const m = ENTITY_KIND_META[entityKindOf(reef)];
  const rows: [string, string][] = [
    ["kind", m.label],
    ["workspace", graphName],
    ["created", new Date(reef.created_at).toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" })],
    ["updated", new Date(reef.updated_at).toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" })],
    ["corals", String(reef.atom_count)],
  ];
  const lineage = reef.lineage ?? [];
  const decisions = reef.decisions ?? [];
  return (
    <div className="flex flex-col gap-[16px]">
      <div>
        <Eyebrow className="mb-[9px]">facts</Eyebrow>
        <div className="overflow-hidden rounded-control border border-line bg-bg">
          {rows.map(([k, v]) => (
            <div key={k} className="flex items-center gap-[10px] border-b border-line-3 px-[12px] py-[7px] last:border-b-0">
              <span className="w-[70px] flex-none font-mono text-[9px] uppercase tracking-[0.09em] text-ink-4">{k}</span>
              <span className="min-w-0 flex-1 truncate text-[12.5px] text-ink">{v}</span>
            </div>
          ))}
        </div>
      </div>
      {lineage.length > 0 && (
        <div>
          <Eyebrow className="mb-[9px]">also in</Eyebrow>
          <div className="overflow-hidden rounded-control border border-line bg-bg">
            {lineage.map((l) => (
              <Link
                key={l.node_id}
                href={`/topics?id=${encodeURIComponent(l.node_id)}`}
                className="flex items-center gap-[10px] border-b border-line-3 px-[12px] py-[7px] last:border-b-0 hover:bg-bg-hover"
              >
                <span className="min-w-0 flex-1 truncate text-[12.5px] text-ink">{l.title}</span>
                <span className="flex-none font-mono text-[10px] text-ink-3">{l.graph_name}</span>
              </Link>
            ))}
          </div>
        </div>
      )}
      {decisions.length > 0 && (
        <div>
          <Eyebrow className="mb-[9px]">decisions</Eyebrow>
          <div className="overflow-hidden rounded-control border border-line bg-bg">
            {decisions.map((d) => (
              <div key={d.node_id} className="border-b border-line-3 px-[12px] py-[7px] last:border-b-0">
                <p className="text-[12.5px] leading-[1.45] text-ink">{d.title}</p>
                <p className="mt-[3px] font-mono text-[10px] text-ink-3">
                  {new Date(d.created_at).toLocaleDateString(undefined, { day: "numeric", month: "short", year: "numeric" })}
                </p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export function TopicSourceMix({ atoms }: { atoms: { source_type: string }[] }) {
  if (atoms.length === 0) return null;
  const counts = new Map<string, number>();
  for (const a of atoms) counts.set(a.source_type, (counts.get(a.source_type) ?? 0) + 1);
  const rows = Array.from(counts.entries())
    .map(([type, n]) => ({ type, n, label: sourceMeta(type).label, hue: hueForSource(type) }))
    .sort((a, b) => b.n - a.n);

  return (
    <div>
      <Eyebrow className="mb-[9px]">source mix</Eyebrow>
      <div className="rounded-control border border-line bg-bg p-[12px]">
        <StackedMeter
          className="mb-[10px]"
          segments={rows.map((r) => ({ value: r.n, tone: hueVar(r.hue), label: r.label }))}
        />
        <div className="flex flex-col gap-[6px]">
          {rows.map((r) => (
            <div key={r.type} className="flex items-center gap-[7px] text-[11.5px] text-ink-2">
              <span className="h-[6px] w-[6px] flex-none rounded-pill" style={{ background: hueVar(r.hue) }} />
              <span className="min-w-0 flex-1 truncate">{r.label}</span>
              <span className="flex-none font-mono text-[10px] text-ink-3">
                {Math.round((r.n / atoms.length) * 100)}% · {r.n}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
