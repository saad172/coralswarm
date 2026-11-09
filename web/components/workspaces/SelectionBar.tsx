"use client";

import { Button } from "@/components/ui";

/** The dark floating bar that appears once one or more topics are selected —
 *  1e's bottom action bar. Only "Share with a workspace…" is wired to a real
 *  backend call (promote); Merge/Delete have no bulk endpoint for reefs in
 *  this codebase yet, so they're omitted rather than left as dead buttons. */
export function SelectionBar({
  count,
  sourceCount,
  onShare,
  onClear,
}: {
  count: number;
  sourceCount: number;
  onShare: () => void;
  onClear: () => void;
}) {
  if (count === 0) return null;
  return (
    <div className="flex items-center gap-[12px] rounded-card bg-ink px-[16px] py-[12px] shadow-frame">
      <span className="grid h-[24px] w-[24px] flex-none place-items-center rounded-row bg-brand font-mono text-[12px] font-semibold text-white">
        {count}
      </span>
      <span className="text-[13.5px] font-medium text-white">topic{count === 1 ? "" : "s"} selected</span>
      <span className="text-[12.5px] text-white/55">{sourceCount} source{sourceCount === 1 ? "" : "s"}</span>
      <span className="h-[22px] w-px bg-white/15" />
      <Button variant="brand" onClick={onShare}>
        Share with a workspace…
      </Button>
      <button type="button" onClick={onClear} className="ml-auto text-[12.5px] text-white/55 transition-colors hover:text-white">
        Clear ×
      </button>
    </div>
  );
}
