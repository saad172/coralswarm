import { clerkSetup } from "@clerk/testing/playwright";
import { API_BASE, REMOTE, TARGET, WEB_BASE, requireClerk } from "../stack/config.mjs";
import { cleanupE2eArtifacts } from "./clerk";

export default async function globalSetup() {
  const { publishableKey, secretKey } = requireClerk();
  // `@clerk/testing` reads these two names.
  process.env.CLERK_PUBLISHABLE_KEY = publishableKey;
  process.env.CLERK_SECRET_KEY = secretKey;

  for (const [what, url] of [["backend", `${API_BASE}/readyz`], ["web", `${WEB_BASE}/`]]) {
    if (what === "web" && process.env.E2E_API_ONLY === "true") continue;
    const res = await fetch(url).catch((e) => ({ ok: false, status: String(e) }) as const);
    if (!res.ok) {
      throw new Error(REMOTE
        ? `e2e: ${what} of target "${TARGET}" is not answering at ${url} (${res.status})`
        : `e2e: ${what} is not up at ${url} (${res.status}). Start the stack: node stack/up.mjs`);
    }
  }

  // A crashed or cancelled earlier run may have left users/orgs on the dev
  // instance; this removes only this run's and hour-old ones (other runs
  // share the instance, and none lives that long).
  if (!process.env.E2E_SKIP_CLERK_CLEANUP) await cleanupE2eArtifacts();

  // Fetches a Testing Token so Clerk's bot protection lets the browser in.
  await clerkSetup();
}
