import { cleanupE2eArtifacts } from "./clerk";

export default async function globalTeardown() {
  if (process.env.E2E_SKIP_CLERK_CLEANUP) return;
  const { users, orgs } = await cleanupE2eArtifacts();
  console.log(`[e2e] removed ${users} Clerk test user(s) and ${orgs} org(s) from the dev instance`);
}
