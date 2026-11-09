"use client";

import type { ReactNode } from "react";
import { SettingsShell } from "@/components/settings/SettingsShell";
import { SettingsHeading } from "@/components/settings/SettingsHeading";
import { OrgGate } from "./OrgGate";

/* Content frame for every Organization page. The rail itself is the one
   shared SettingsShell; this adds the page heading and wraps the content in
   OrgGate, so a member or personal account that lands here sees the rail
   (without any org rows) plus "Managed by your organization admin" rather
   than the page. */
export function OrgSettingsShell({
  title,
  italicWord,
  lead,
  action,
  children,
}: {
  title: string;
  italicWord?: string;
  lead?: ReactNode;
  action?: ReactNode;
  children: ReactNode;
}) {
  return (
    <SettingsShell>
      <OrgGate>
        {action && <div className="mb-[10px] flex justify-end">{action}</div>}
        <SettingsHeading title={title} italicWord={italicWord}>
          {lead}
        </SettingsHeading>
        {children}
      </OrgGate>
    </SettingsShell>
  );
}
