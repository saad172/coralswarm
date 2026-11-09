import { clerk, setupClerkTestingToken } from "@clerk/testing/playwright";
import { test as base, expect, type Page } from "@playwright/test";
import type { User } from "@clerk/backend";
import { Api } from "./api";
import { clerkApi, createUser, deleteE2eUser, e2eUsersSnapshot, primaryEmail, takeE2eUsersSince } from "./clerk";

export type SignedIn = { user: User; email: string; api: Api };

type Fixtures = {
  /** A fresh Clerk user per scenario, signed in to `page`, plus an API
   *  client acting as the same user. */
  signedIn: SignedIn;
  makeUser: (label: string, name?: string) => Promise<SignedIn>;
  /** Deletes the users a scenario made through `createUser` or an invite. */
  releaseUsers: void;
};

// Match the load recorder's finite 120s completion window. Cold Home can owe
// a real worker result (the registry-derived recovery policy is 110s), so the
// default 10s assertion timeout cannot establish landing-page completion.
const HOME_READY_TIMEOUT_MS = 120_000;

/** Sign in through Clerk and finish the authenticated landing navigation. */
export async function signIn(page: Page, email: string) {
  await setupClerkTestingToken({ page });
  // Start on the public sign-in document: `/` first visits protected Home,
  // whose signed-out redirect can otherwise race session activation.
  // Clerk readiness is checked explicitly below. The browser's broader load
  // event can be held by Clerk UI assets even after this document is ready.
  await page.goto("/login", { waitUntil: "domcontentloaded" });
  await clerk.loaded({ page });
  await clerk.signIn({ page, emailAddress: email });
  // Clerk only waits for its user/session, not WebLogin's router.replace or
  // Home's required reads. Complete that navigation before callers install a
  // recorder, switch orgs, or clear read models for a genuine cold load.
  await page.waitForURL("**/home");
  await expect(page.locator('[data-page-settled="true"]').getByRole("main")).toBeVisible({ timeout: HOME_READY_TIMEOUT_MS });
}

function tokenOrganization(jwt: string): string | null {
  const claims = JSON.parse(Buffer.from(jwt.split(".")[1], "base64url").toString()) as { org_id?: string; o?: { id?: string } };
  return claims.org_id ?? claims.o?.id ?? null;
}

/** Complete a real org switch (or verify an already-active org) before the
 * caller clears cold caches or starts recording a target document. */
export async function activateOrganization(page: Page, organization: string | null) {
  await page.waitForURL("**/home");
  await expect(page.locator('[data-page-settled="true"]').getByRole("main")).toBeVisible({ timeout: HOME_READY_TIMEOUT_MS });
  const current = await page.evaluate(() => ({
    organization: window.Clerk.organization?.id ?? null,
    sessionOrganization: window.Clerk.session?.lastActiveOrganizationId ?? null,
  }));
  const change = current.organization !== organization || current.sessionOrganization !== organization;
  const timeoutMs = 10_000;
  // A newly created Clerk session can already select this org. Calling
  // setActive again during its first navigation can strand the SDK promise.
  // Actual switches must also reach the backend under the requested JWT scope.
  const boot = change ? page.waitForResponse((response) => {
    if (new URL(response.url()).pathname !== "/v1/bootstrap" || response.request().method() !== "GET") return false;
    const bearer = response.request().headers().authorization?.replace(/^Bearer /i, "");
    try { return !!bearer && tokenOrganization(bearer) === organization; }
    catch { return false; }
  }, { timeout: timeoutMs }) : Promise.resolve(null);
  const [response, jwt] = await Promise.all([
    boot,
    page.evaluate(async ({ organization, change, timeoutMs }) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        return await Promise.race([
          (async () => {
            if (change) await window.Clerk.setActive({ organization });
            return await window.Clerk.session?.getToken({ skipCache: true }) ?? "";
          })(),
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new Error(`Clerk organization activation/token did not complete for ${organization ?? "personal"}`)), timeoutMs);
          }),
        ]);
      } finally { clearTimeout(timer); }
    }, { organization, change, timeoutMs }),
  ]);
  expect(jwt, "the active organization has no session JWT").not.toBe("");
  expect(tokenOrganization(jwt), "the fresh session JWT must name the requested organization").toBe(organization);
  await page.waitForFunction((expected) =>
    (window.Clerk.organization?.id ?? null) === expected &&
    (window.Clerk.session?.lastActiveOrganizationId ?? null) === expected,
  organization, { timeout: timeoutMs });
  if (response) expect(response.status(), "the requested organization must bootstrap successfully").toBe(200);
  await expect(page.locator('[data-page-settled="true"]').getByRole("main")).toBeVisible({ timeout: HOME_READY_TIMEOUT_MS });
}

/** A session JWT from the page's own Clerk session (carries its active org). */
export function browserToken(page: Page) {
  return async () => {
    const jwt = await page.evaluate(async () => (await window.Clerk.session?.getToken({ skipCache: true })) ?? "");
    expect(jwt, "the page has no Clerk session").not.toBe("");
    return jwt;
  };
}

export const test = base.extend<Fixtures>({
  // Worker fixtures are set up before this snapshot, so their users outlive
  // the scenario. A full run otherwise holds every scenario's users until
  // teardown and fills the shared dev instance's 100-user cap.
  releaseUsers: [async ({}, use) => {
    const before = e2eUsersSnapshot();
    await use();
    await Promise.all(takeE2eUsersSince(before).map((id) => deleteE2eUser(id, clerkApi).catch(() => {})));
  }, { auto: true }],
  makeUser: async ({ request }, use) => {
    const made: string[] = [];
    await use(async (label, name) => {
      const user = await createUser(label, name);
      made.push(user.id);
      return { user, email: primaryEmail(user), api: new Api(request, user.id) };
    });
    // Free the seats now, not at shard teardown: the dev instance's 100-user
    // cap is shared by every concurrent run. Teardown still sweeps anything
    // this misses.
    await Promise.all(made.map((id) => deleteE2eUser(id, clerkApi).catch(() => {})));
  },
  signedIn: async ({ page, makeUser }, use) => {
    const who = await makeUser("owner", "E2E Owner");
    // Provision the account (personal ocean) before the browser's own first
    // requests: concurrent first requests for a brand-new user can see the
    // account before its ocean exists (README "Known gaps").
    await who.api.graphs();
    await signIn(page, who.email);
    await use(who);
  },
});

export { expect };
