// Pure canonical declarations shared with the browser budget recorder.
export const PLAIN_LOAD_PATHS = {
  me: "/v1/me",
  bootstrap: "/v1/bootstrap",
  deleteAccount: "/v1/me/delete-account",
  billingSummary: "/v1/billing/summary",
  trialBanner: "/v1/billing/trial-banner",
  seatTiers: "/v1/billing/seat-tiers",
  billingMe: "/v1/billing/me",
  graphs: "/v1/graphs",
  sharedMeeting: "/v1/meetings/shares/view/{token}",
  retractions: "/v1/retractions",
  teams: "/v1/teams",
  people: "/v1/teams/members",
  archivedTeams: "/v1/teams/archived",
  teamRoster: "/v1/teams/{team_id}/members",
  teamEvents: "/v1/teams/{team_id}/events",
  orgSlots: "/v1/orgs/slots",
  provisioning: "/v1/orgs/{org_id}/provisioning",
  orgDeletion: "/v1/orgs/{org_id}/deletion",
  orgDomains: "/v1/orgs/domains",
  myJoinRequests: "/v1/me/join-requests",
  orgJoinRequests: "/v1/orgs/join-requests",
  orgJoinRequestCount: "/v1/orgs/join-requests/count",
  activity: "/v1/activity",
  oauthConnections: "/v1/oauth/connections",
  watchedThread: "/v1/connectors/{provider}/watched/{thread_id}",
  addinPairInfo: "/v1/addin/pair/info",
  microsoftLink: "/v1/addin/microsoft/link",
  addinWorkspace: "/v1/addin/workspace",
  addinTokens: "/v1/addin/tokens",
  scheduleIntent: "/v1/outlook-capture-schedule/intents/{intent_id}",
  scheduleNonce: "/v1/outlook-capture-schedule/by-nonce/{nonce}",
  scheduleEvent: "/v1/outlook-capture-schedule/by-event",
  meetCaptures: "/v1/connectors/google/meet/captures",
  publicShare: "/v1/shared/{token}",
  publicSessionShare: "/v1/shared/sessions/{token}",
  emailPrefill: "/v1/email/prefill",
  // Home launchpad: three bounded plain reads, no stored model, no polling.
  homeContinue: "/v1/home/continue",
  homeWhatsNew: "/v1/home/whats-new",
  homeAttention: "/v1/home/attention",
} as const;
export const FAN_IN_LOAD_PATHS = { oceansIndex: "/v1/load/oceans" } as const;
export const SHELL_LOAD_PATHS = [...Object.values(PLAIN_LOAD_PATHS), ...Object.values(FAN_IN_LOAD_PATHS)];
export type PlainLoadPath = (typeof PLAIN_LOAD_PATHS)[keyof typeof PLAIN_LOAD_PATHS];
export type PlainParams = Record<string, string | number | boolean | null | undefined>;

export function plainLoadPath(path: PlainLoadPath, params: PlainParams = {}): string {
  const values: Record<string, string> = Object.fromEntries(Object.entries(params)
    .filter((entry) => entry[1] != null).sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => [k, String(v)]));
  const resolved = path.replace(/\{([^}]+)\}/g, (_, key: string) => {
    const value = values[key];
    if (!value) throw new Error(`Missing ${key} for page read`);
    delete values[key];
    return encodeURIComponent(value);
  });
  const query = new URLSearchParams(values).toString();
  return `${resolved}${query ? `?${query}` : ""}`;
}

