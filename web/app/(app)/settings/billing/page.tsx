"use client";

import { Empty, Loading } from "@/components/ui";
import { BillingView } from "@/components/billing/BillingView";
import { SettingsHeading } from "@/components/settings/SettingsHeading";
import { SettingsShell } from "@/components/settings/SettingsShell";
import { useSettingsRole } from "@/components/settings/useSettingsRole";
import { ORG_BILLING_HREF } from "@/components/billing/paywall";
import Link from "next/link";

/* Billing for your own account (Free or Pro). Inside a team organization the
   plan belongs to the organization, so members see a pointer instead and
   admins are sent to Organization -> Billing. */
export default function SettingsBillingPage() {
  const { role, loaded } = useSettingsRole();
  return (
    <SettingsShell>
      {!loaded ? (
        <Loading what="billing" />
      ) : role === "personal" ? (
        <>
          <SettingsHeading title="Plan and payment">
            Your plan, credits and extra usage. Payment details and invoices are in Manage billing.
          </SettingsHeading>
          <BillingView path="/settings/billing" />
        </>
      ) : role === "org-admin" ? (
        <Empty
          title="Billing is on the organization page"
          hint={
            <>
              Your plan is paid for by your organization. <Link href={ORG_BILLING_HREF} className="underline">Open organization billing</Link>.
            </>
          }
        />
      ) : (
        <Empty
          title="Managed by your organization admin"
          hint="Your plan is paid for by your organization. Ask an admin if you need something changed."
        />
      )}
    </SettingsShell>
  );
}
