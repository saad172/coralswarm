"use client";

import Link from "next/link";
import { activeRow, Badge, CountPill } from "@/components/ui";

/* The two-line nav row shared by every Settings sub-page's left rail (2h/2k/
   2l/2m) and the Organization admin rail (3a-3g): a 13.5px label plus a mono
   sub-label, using the same active/inactive anatomy as the rest of the kit's
   selection controls (`activeRow`) so it never drifts from Seg/Tabs/NavRow.

   `count`: an optional trailing CountPill (CS-438, D13) — the join-request
   badge on Members is the first caller. Omitted or `0` renders nothing,
   same "no fake zeros" rule `AppSidebar`'s own `NavLink` `count` prop
   follows. */
export function SettingsNavRow({
  href,
  label,
  sub,
  tag,
  count,
  active,
}: {
  href: string;
  label: string;
  sub: string;
  tag?: string;
  count?: number;
  active: boolean;
}) {
  return (
    <Link
      href={href}
      className={`flex flex-col gap-[2px] rounded-field px-[11px] py-[9px] text-left ${activeRow(active)}`}
    >
      <span className="flex items-center gap-[7px]">
        <span className="text-[13.5px]">{label}</span>
        {tag && (
          <Badge tone="brand" className="px-[6px] py-[1px] text-[8px] tracking-[0.06em]">
            {tag}
          </Badge>
        )}
        {count ? (
          <CountPill
            className="ml-auto"
            ariaLabel={`${count} ${count === 1 ? "person" : "people"} waiting to join`}
          >
            {count}
          </CountPill>
        ) : null}
      </span>
      <span className="font-mono text-[9.5px] leading-[1.4] text-ink-4">{sub}</span>
    </Link>
  );
}
