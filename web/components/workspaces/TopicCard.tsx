"use client";

import Link from "next/link";
import { Chip, Dot } from "@/components/ui";
import type { Reef } from "@/lib/api";
import { REEF_KIND_META, relativeAge } from "./reefKinds";

export function TopicCard({
  reef,
  selected,
  onToggle,
}: {
  reef: Reef;
  selected: boolean;
  onToggle: () => void;
}) {
  const meta = REEF_KIND_META[reef.type];
  return (
    <div
      className={`flex flex-col gap-[9px] rounded-card border bg-bg p-[14px] transition-colors ${
        selected ? "border-ink shadow-card" : "border-line hover:border-ink-3"
      }`}
    >
      <div className="flex items-center gap-[8px]">
        <button
          type="button"
          onClick={onToggle}
          aria-pressed={selected}
          aria-label={selected ? "Deselect topic" : "Select topic"}
          className={`grid h-[16px] w-[16px] flex-none place-items-center rounded-badge border text-[9px] text-white transition-colors ${
            selected ? "border-ink bg-ink" : "border-line bg-bg"
          }`}
        >
          {selected && "✓"}
        </button>
        <Chip hue={meta.hue}>
          <Dot hue={meta.hue} size={5} />
          {meta.label}
        </Chip>
        <span className="ml-auto font-mono text-[9.5px] text-ink-4">{relativeAge(reef.updated_at)}</span>
      </div>
      <Link href={`/topics?id=${encodeURIComponent(reef.node_id)}`} className="text-left">
        <span className="line-clamp-2 text-[14px] font-medium leading-[1.4] text-ink hover:text-brand-2">
          {reef.title}
        </span>
      </Link>
      <div className="flex items-center gap-[8px] border-t border-line-3 pt-[9px] font-mono text-[9.5px] text-ink-4">
        {reef.atom_count} source{reef.atom_count === 1 ? "" : "s"}
      </div>
    </div>
  );
}
