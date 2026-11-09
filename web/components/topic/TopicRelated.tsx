import Link from "next/link";
import { Eyebrow, hueVar } from "@/components/ui";
import type { Reef } from "@/lib/api";
import { TOPIC_META } from "./topicMeta";

/* Related topics — the nearest real proxy available: other topics of the same
   kind in this workspace, most recently updated first. No relatedness/graph-
   edge endpoint exists yet (co-occurring sources/participants would need
   per-topic atom fetches this page doesn't make) — the same documented gap
   archive/web-legacy-ui/screens/reef.page.tsx flagged. */
export function TopicRelated({ related }: { related: Reef[] }) {
  if (related.length === 0) return null;
  return (
    <div>
      <Eyebrow className="mb-[9px]">related topics</Eyebrow>
      <div className="flex flex-col gap-[6px]">
        {related.map((r) => {
          const m = TOPIC_META[r.type];
          return (
            <Link
              key={r.node_id}
              href={`/topics?id=${r.node_id}`}
              className="flex items-center gap-[9px] rounded-control border border-line bg-bg px-[11px] py-[9px] transition-colors hover:border-ink-3"
            >
              <span className="flex-none text-[12px]" style={{ color: hueVar(m.hue) }}>
                {m.glyph}
              </span>
              <div className="min-w-0 flex-1">
                <div className="truncate font-display text-[14px] leading-[1.2] text-ink">{r.title}</div>
                <div className="font-mono text-[10px] text-ink-3">
                  {m.label} · {r.atom_count} item{r.atom_count === 1 ? "" : "s"}
                </div>
              </div>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
