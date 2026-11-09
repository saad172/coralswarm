import type { MembersAdminPayload } from "../pageSectionTypes/membersAdmin";
import type { Stored } from "../pageSectionWire";

/** Complete wire fixtures for the one MembersAdmin request. They are not
 * adapters around the individual legacy read clients. */
export function membersAdminFixture(changes: Partial<MembersAdminPayload> = {}): Stored<MembersAdminPayload> {
  return { model: "members_admin", version: 1, computed_at: "2026-10-31T00:00:00Z", stale: false, refresh_eta: null,
    payload: { members: [], slots: { purchased: 0, used: 0, reserved: 0, available: 0, tier_breakdown: [], tiers: [] },
      domains: [], join_requests: [], seat_tiers: [], auto_add: { enabled: false, tier: "standard" }, ...changes } };
}
