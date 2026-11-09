// Shared step list + route map for the turn 6 org-standup wizard
// (6a–6e). 6f (recovery keys) and 6g (joining as a member) are
// deliberately NOT part of this rail — see their own pages for why.
export const ORG_STEPS = ["Organization", "Sources", "Policy", "Workspaces", "Invite"];

export const ORG_ROUTES = [
  "/onboarding/org",
  "/onboarding/org/sources",
  "/onboarding/org/policy",
  "/onboarding/org/workspaces",
  "/onboarding/org/invite",
] as const;
