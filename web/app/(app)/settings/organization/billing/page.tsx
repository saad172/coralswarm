"use client";

import { OrgSettingsShell } from "@/components/org/OrgSettingsShell";
import { BillingView } from "@/components/billing/BillingView";
import { DeleteOrganizationZone } from "@/components/org/DeleteOrganizationZone";

/* Organization -> Billing (admin-only; OrgSettingsShell hides it from
   everyone else). Plan, seats, credits, extra usage and the card, all from
   `GET /v1/billing/summary`. Seat changes live here, not on Members. */
export default function SettingsOrganizationBillingPage() {
  return (
    <OrgSettingsShell
      title="Plan and payment"
      lead="Your plan, seats and extra usage. Payment details and invoices are in Manage billing."
    >
      <BillingView path="/settings/organization/billing" />
      <DeleteOrganizationZone />
    </OrgSettingsShell>
  );
}
