"use client";

import { Mono } from "@/components/ui";
import { OrgSettingsShell } from "@/components/org/OrgSettingsShell";
import { SettingList, SettingRow } from "@/components/settings/SettingRow";

/* What an admin can do that a member cannot, and what every member can do.

   This is a plain summary of checks the backend really makes, not a settings
   editor: roles are fixed (admin or member) and there is nothing to configure
   per capability. Every line below maps to a server-side admin check; if the
   backend stops enforcing one, delete the line. "Admin" includes the owner.

   Admin-only (backend/src):
     invites                    routes/org_invites.rs  require_org_admin
     join requests, auto-add    routes/org_join_requests.rs, org_auto_add.rs,
                                org_domains.rs         require_org_admin
     seats and billing          routes/org_slots.rs, billing_seats.rs,
                                checkout.rs            admin / owner only
     org Slack workspace        routes/oauth.rs        reject_org_slack_non_admin
     connector health, audit    routes/connector_health.rs, connector_audit.rs
     roster details             routes/teams.rs        list_org_members
   Everyone: connect your own accounts. */
const ADMIN_CAN: { name: string; desc: string }[] = [
  { name: "Invite people", desc: "Send, resend and revoke invitations by email." },
  { name: "Approve who joins", desc: "Accept or reject requests to join, and turn on joining by verified email domain." },
  { name: "Manage seats and billing", desc: "Buy or reduce seats, change a member's seat type, and subscribe." },
  { name: "Connect or disconnect the organization's Slack", desc: "Slack is connected once for the whole workspace." },
  { name: "Check connector health", desc: "See whether each connector is syncing, and read the connector audit log." },
  { name: "See member details", desc: "Invite status, last sign-in and which sources each member has connected." },
];

const MEMBER_CAN: { name: string; desc: string }[] = [
  { name: "Connect their own accounts", desc: "Their own Gmail, calendar, Microsoft and Slack accounts. These apply only to them." },
  { name: "See who is in the organization", desc: "Names, emails and roles." },
];

function CapabilityList({ items }: { items: { name: string; desc: string }[] }) {
  return (
    <SettingList className="mb-[16px]">
      {items.map((c) => (
        <SettingRow key={c.name} title={c.name} desc={c.desc} />
      ))}
    </SettingList>
  );
}

export default function SettingsOrganizationPermissionsPage() {
  return (
    <OrgSettingsShell
      title="Admins and members"
      lead="Roles are fixed: an organization has admins (including the owner) and members. This is what each can do. It is a summary, not a setting."
    >
      <Mono className="mb-[9px] block tracking-[0.13em]">only admins can</Mono>
      <CapabilityList items={ADMIN_CAN} />

      <Mono className="mb-[9px] block tracking-[0.13em]">every member can</Mono>
      <CapabilityList items={MEMBER_CAN} />
    </OrgSettingsShell>
  );
}
