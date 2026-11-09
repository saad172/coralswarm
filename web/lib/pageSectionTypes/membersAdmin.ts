import type { AutoAddSettings, JoinRequest, OrgDirectoryMember, OrgDomain, SeatTier, SlotPoolResult } from "../api";

export interface MembersAdminPayload {
  members: OrgDirectoryMember[];
  slots: SlotPoolResult;
  domains: OrgDomain[];
  join_requests: JoinRequest[];
  seat_tiers: SeatTier[];
  auto_add: AutoAddSettings;
}

export function isMembersAdminPayload(body: unknown): body is MembersAdminPayload {
  const b = body as MembersAdminPayload | null;
  return !!b && [b.members, b.domains, b.join_requests, b.seat_tiers].every(Array.isArray) && !!b.slots &&
    [b.slots.purchased, b.slots.used, b.slots.reserved, b.slots.available].every((n) => Number.isSafeInteger(n) && n >= 0) &&
    Array.isArray(b.slots.tier_breakdown) && Array.isArray(b.slots.tiers) &&
    !!b.auto_add && typeof b.auto_add.enabled === "boolean" && typeof b.auto_add.tier === "string";
}
