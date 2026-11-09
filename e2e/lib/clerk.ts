// Clerk DEV-instance helpers: real users, real sessions, real JWTs.
// The backend verifies every token against the instance's JWKS exactly as in
// production; nothing here (or in the backend) bypasses that.

import { createClerkClient, type User } from "@clerk/backend";
import { test } from "@playwright/test";
import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { cleanupE2eArtifacts, deleteE2eUser, runId } from "../stack/clerk-cleanup.mjs";
import { withRateLimitRetry } from "../stack/clerk-client.mjs";
import { loadTestIdentity, testIdentityEmail, testUserMetadata } from "../stack/clerk-identity.mjs";
import { requireClerk } from "../stack/config.mjs";

/** How long `createUser` waits out a full dev instance, and how long any
 *  Backend API call waits out rate limiting (stack/clerk-client.mjs). */
export const QUOTA_WAIT_MS = 150_000;

const { secretKey } = requireClerk();
/** Every call retries 429 for up to QUOTA_WAIT_MS, stretching the running
 *  scenario's timeout by the time it waits. */
export const clerkApi = withRateLimitRetry(createClerkClient({ secretKey }), { waitMs: QUOTA_WAIT_MS, onWait: extendTestTimeout });

/** `+clerk_test` makes it a Clerk test address: no mail is sent and the
 *  email-code is always 424242. The run id lets cleanup delete exactly this
 *  run's users on the shared dev instance (stack/clerk-cleanup.mjs). The label
 *  is trimmed so the local part stays within the 64 characters Clerk accepts:
 *  a CI run id alone takes 17. */
export function testEmail(label: string): string {
  return testIdentityEmail(loadTestIdentity(runId()), label, randomUUID().slice(0, 8));
}

export const TEST_EMAIL_CODE = "424242";

/** The dev instance holds at most 100 users, shared by every concurrent
 *  merge-queue run, shard and laptop. Clerk answers 403 `user_quota_exceeded`
 *  when it is full; other runs free seats as their tests and teardowns finish. */
export function isUserQuotaExceeded(e: unknown): boolean {
  const errors = (e as { errors?: { code?: string }[] } | null)?.errors;
  return Array.isArray(errors) && errors.some((x) => x.code === "user_quota_exceeded");
}

/** Backoff before quota retry `attempt` (0-based): 2 s doubling to 20 s, with
 *  jitter so the runs waiting on the same seats do not retry in lockstep. */
export function quotaBackoffMs(attempt: number, random = Math.random): number {
  return Math.min(2_000 * 2 ** attempt, 20_000) * (0.5 + random() / 2);
}

let lastReclaim = 0;

/** Users this worker made; lib/fixtures deletes each scenario's at its end. */
const madeUsers = new Set<string>();
/** Register a user the backend created for this worker (an invite). */
export function adoptE2eUser(id: string) {
  madeUsers.add(id);
}
/** Forget and return the users made since `before` was taken. */
export function takeE2eUsersSince(before: ReadonlySet<string>): string[] {
  const since = [...madeUsers].filter((id) => !before.has(id));
  for (const id of since) madeUsers.delete(id);
  return since;
}
export function e2eUsersSnapshot(): ReadonlySet<string> {
  return new Set(madeUsers);
}

export async function createUser(label: string, name = `E2E ${label}`): Promise<User> {
  const [firstName, ...rest] = name.split(" ");
  const deadline = Date.now() + QUOTA_WAIT_MS;
  for (let attempt = 0; ; attempt++) {
    try {
      const user = await clerkApi.users.createUser({
        emailAddress: [testEmail(label)],
        firstName,
        lastName: rest.join(" ") || undefined,
        skipPasswordRequirement: true,
        privateMetadata: testUserMetadata(loadTestIdentity(runId())),
      });
      madeUsers.add(user.id);
      return user;
    } catch (e) {
      if (!isUserQuotaExceeded(e) || Date.now() >= deadline) throw e;
      if (attempt === 0) extendTestTimeout(QUOTA_WAIT_MS);
      // Quota pressure grants no authority over foreign or live fixtures.
      // Reclaim only this owner's expired leases; otherwise wait for owner
      // finalizers/verified maintenance, bounded by QUOTA_WAIT_MS.
      if (!process.env.E2E_SKIP_CLERK_CLEANUP && Date.now() - lastReclaim > 30_000) {
        lastReclaim = Date.now();
        await cleanupE2eArtifacts({ prefix: `${runId()}-`, mode: 'abandoned' }).catch(() => {});
      }
      await sleep(Math.min(quotaBackoffMs(attempt), Math.max(0, deadline - Date.now())));
    }
  }
}

/** Give the running test room to wait out the quota (no-op outside a test). */
function extendTestTimeout(ms: number) {
  try {
    const info = test.info();
    if (info.timeout > 0) info.setTimeout(info.timeout + ms);
  } catch {
    // Not inside a test (global setup, a script).
  }
}

export async function findUserByEmail(email: string): Promise<User | undefined> {
  const { data } = await clerkApi.users.getUserList({ emailAddress: [email] });
  return data[0];
}

/** A short-lived session JWT for API scenarios (the `request` fixture). The
 *  session is real and created through the Backend API; the token is minted
 *  by Clerk, never by the harness. */
export async function sessionToken(userId: string): Promise<string> {
  // One session per user, created on first use and reused: a session created
  // after the user joined an org can start with that org active, and these
  // API calls are the user's PERSONAL session (the org is chosen per-session,
  // in the browser, exactly as the web app does).
  let session = sessions.get(userId);
  if (!session) {
    session = clerkApi.sessions.createSession({ userId }).then((s) => s.id);
    sessions.set(userId, session);
  }
  // Reuse a token until 15 s before it expires: polling scenarios would
  // otherwise trip the Backend API's rate limit.
  const cached = tokens.get(userId);
  if (cached && cached.exp - Date.now() > 15_000) return cached.jwt;
  const token = await clerkApi.sessions.getToken(await session, undefined as unknown as string);
  const exp = JSON.parse(Buffer.from(token.jwt.split(".")[1], "base64url").toString()).exp * 1000;
  tokens.set(userId, { jwt: token.jwt, exp });
  return token.jwt;
}
const sessions = new Map<string, Promise<string>>();
const tokens = new Map<string, { jwt: string; exp: number }>();

export function primaryEmail(user: User): string {
  return user.emailAddresses.find((e) => e.id === user.primaryEmailAddressId)?.emailAddress ?? user.emailAddresses[0].emailAddress;
}

export { cleanupE2eArtifacts, deleteE2eUser };
