"use client";

import type { ReactNode } from "react";
import { usePathname } from "next/navigation";
import { Eyebrow, Mono } from "@/components/ui";
import { useSettingsRole, roleSummary } from "./useSettingsRole";
import { SettingsNavRow } from "./SettingsNavRow";
import { MEMBERS_HREF, settingsNavFor } from "./settingsNav";
import { useCriticalPageReadLease } from "@/lib/pageReadRegistration";
import { useJoinRequestCount } from "@/components/org/useJoinRequestCount";

/** Exact-match only: these are sibling routes, not nested, so `/settings`
 *  must not read as active while viewing `/settings/privacy`. */
function samePath(pathname: string | null, href: string): boolean {
  const p = (pathname ?? "").replace(/\/+$/, "") || "/";
  const h = href.replace(/\/+$/, "") || "/";
  return p === h;
}

/* The one Settings rail, shared by every page under /settings (personal and
   Organization alike). What it lists depends on the role; see settingsNav.ts. */
export function SettingsShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const roleState = useSettingsRole();
  const { role, loaded, orgName } = roleState;
  useCriticalPageReadLease("settings role initialization", !loaded);
  // Admin-only and silent otherwise: null for everyone who isn't an admin.
  const joinRequestCount = useJoinRequestCount();
  const sections = settingsNavFor(role, loaded);

  return (
    <div className="grid grid-cols-[210px_minmax(0,1fr)] gap-0">
      <nav aria-label="Settings" className="flex flex-none flex-col gap-[3px] border-r border-line-2 pr-[14px]">
        {/* Which account these settings belong to, so a member and an admin
            of the same org can tell whose values they are looking at. */}
        {loaded && (
          <div className="mb-[8px] flex items-center gap-[6px] px-[10px]">
            <Eyebrow>{role === "personal" ? "personal account" : orgName ?? "organization"}</Eyebrow>
            {role === "org-member" && (
              <span className="font-mono text-[9px] uppercase tracking-[0.13em] text-ink-4">member</span>
            )}
            {role === "org-admin" && (
              <span className="font-mono text-[9px] uppercase tracking-[0.13em] text-ink-4">admin</span>
            )}
          </div>
        )}

        {sections.map((section, i) => (
          <div key={section.heading ?? i} className="flex flex-col gap-[3px]">
            {section.heading && (
              <Mono className="mt-[12px] px-[11px] pb-[4px] tracking-[0.13em] text-ink-3">{section.heading}</Mono>
            )}
            {section.items.map((item) => (
              <SettingsNavRow
                key={item.href}
                {...item}
                count={item.href === MEMBERS_HREF ? (joinRequestCount ?? undefined) : undefined}
                active={samePath(pathname, item.href)}
              />
            ))}
          </div>
        ))}

        <div className="mt-auto border-t border-line-2 pt-[11px]">
          <Mono className="leading-[1.6] text-ink-3">{roleSummary(roleState)}</Mono>
        </div>
      </nav>
      <div className="min-w-0 overflow-hidden pl-[26px]">
        <div className="max-w-[830px]">{children}</div>
      </div>
    </div>
  );
}
