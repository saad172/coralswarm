import type { ReactNode } from "react";
import { Card, cn } from "@/components/ui";

/* The one row shape nearly every settings/org screen in the redesign is built
   from: a title (with an optional inline tag), a description, and a trailing
   control — a Toggle, a Button, or a static value.
   `SettingList` is the bordered card that groups rows with a hairline between
   them, matching the mock's `border-bottom:1px solid #F2F3F5` rows exactly. */
export function SettingList({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <Card flush className={cn("divide-y divide-line-3 overflow-hidden", className)}>
      {children}
    </Card>
  );
}

export function SettingRow({
  title,
  tag,
  desc,
  children,
  className,
}: {
  title: ReactNode;
  tag?: ReactNode;
  desc?: ReactNode;
  children?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("flex items-center gap-4 px-[15px] py-[12px]", className)}>
      <div className="min-w-0 flex-1">
        <span className="flex items-center gap-[8px]">
          <span className="text-[13.5px] font-medium text-ink">{title}</span>
          {tag}
        </span>
        {desc && <p className="mt-[3px] text-[12.5px] leading-[1.5] text-ink-3">{desc}</p>}
      </div>
      {children && <div className="flex-none">{children}</div>}
    </div>
  );
}

/** Small pill tag inline next to a row's title — "on-device",
 *  "off by default", "locked for members". Tone follows the mock: ok-green
 *  for capability notes, brand for the "locked" warning. */
export function RowTag({ children, tone = "ok" }: { children: ReactNode; tone?: "ok" | "brand" }) {
  const cls = tone === "ok" ? "bg-ok/[13%] text-ok" : "bg-brand-soft text-brand-2";
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-pill px-[7px] py-[1px] font-mono text-[8.5px] uppercase tracking-[0.06em]",
        cls,
      )}
    >
      {children}
    </span>
  );
}
