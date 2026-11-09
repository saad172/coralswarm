import { setupClerkTestingToken } from "@clerk/testing/playwright";
import { randomUUID } from "node:crypto";
import { completeStripeCheckout } from "../lib/api";
import { TEST_EMAIL_CODE, testEmail } from "../lib/clerk";
import { expect, signIn, test } from "../lib/fixtures";
import { WEB_BASE } from "../stack/config.mjs";

// Journey (b): an org admin invites a member by email, the member accepts
// (signs in from the invite link with the emailed code) and sees the org's
// shared ocean.
test("invited member accepts and sees the org ocean", async ({ page, browser, request, makeUser }) => {
  // Seed: a self-serve team org, created through the real route and paid for
  // through the real Stripe webhook; the backend's own provisioning job then
  // builds its tenant database.
  const admin = await makeUser("admin", "E2E Admin");
  const orgName = `E2E Org ${randomUUID().slice(0, 6)}`;
  const created = await admin.api.json(
    "POST",
    "/v1/orgs",
    { name: orgName, seats: 2, success_url: `${WEB_BASE}/onboarding/team/return`, cancel_url: `${WEB_BASE}/onboarding/team` },
    { "Idempotency-Key": randomUUID() },
  );
  expect(created.session_id).toBeTruthy();
  await completeStripeCheckout(request, created.session_id);
  await expect
    .poll(async () => (await admin.api.json("GET", `/v1/orgs/${created.org_id}/provisioning`)).status, { timeout: 15_000, intervals: [250] })
    .toBe("ready");

  // The admin, in the org, adds a member by email.
  await signIn(page, admin.email);
  await page.evaluate(async (org) => window.Clerk.setActive({ organization: org }), created.org_id);
  await page.goto("/settings/organization/members");
  const invitee = testEmail("member");
  await page.getByPlaceholder("name@company.com").fill(invitee);
  await page.getByRole("button", { name: "Add member" }).click();
  await expect(page.getByText(invitee, { exact: true })).toBeVisible();

  // The member follows the invite link and signs in with the emailed code.
  const memberContext = await browser.newContext();
  const member = await memberContext.newPage();
  await setupClerkTestingToken({ page: member });
  await member.goto(`/join?email=${encodeURIComponent(invitee)}&org=${encodeURIComponent(orgName)}&inviter=${encodeURIComponent("E2E Admin")}`);
  await member.getByRole("button", { name: `Continue as ${invitee}` }).click();
  await member.getByPlaceholder("123456").fill(TEST_EMAIL_CODE);
  await member.getByRole("button", { name: "Verify" }).click();
  await member.getByRole("button", { name: "Skip" }).click();
  await expect(member).toHaveURL(/\/home/);

  // Joining from the invite lands the member in the org (the account
  // switcher names it), and the org's shared ocean is listed.
  await expect(member.getByRole("button", { name: new RegExp(`^Current account: ${orgName}, an organization`) })).toBeVisible();
  await member.goto("/workspaces");
  await expect(member.getByText("Shared with others")).toBeVisible();
  await expect(member.getByRole("link", { name: /Organization/ }).first()).toBeVisible();
  await memberContext.close();
});
