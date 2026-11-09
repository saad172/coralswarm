import { waitForComputed } from "../lib/readModels";
import { expect, test } from "@playwright/test";
import crypto from "node:crypto";
import { addMember, createReadyOrg, ingestNote } from "../lib/isolation";
import { API_BASE } from "../stack/config.mjs";

// Home "From your workspaces" (`GET /v1/home/workspace-activity`, flag
// HOME_DASHBOARD_ENABLED, set on in stack/config.mjs). API-level, against a
// real self-serve org: the admin's capture lands in their PRIVATE ocean, which
// must never be a workspace row for the admin or for another member, while the
// route itself answers (200 with a list), validates its parameters and
// requires a token. The headline arithmetic and the shared-ocean rows are
// covered against seeded oceans by backend/tests/workspace_activity_pg.rs.

test("workspace activity never lists a private ocean, validates params and needs a token", async ({ request }) => {
  test.setTimeout(120_000);
  const org = await createReadyOrg(request, "wsa");
  const member = await addMember(request, org, "wsa-member");

  const mine = (await org.adminInOrg.graphs()).find((g) => g.kind === "member");
  expect(mine, "the org admin has no private ocean in the org").toBeTruthy();
  const secret = `wsa-${crypto.randomUUID()}`;
  await ingestNote(org.adminInOrg, mine!.id, `The ${secret} rollout checklist is owned by Priya.`);

  for (const who of [org.adminInOrg, member.inOrg]) {
    const res = (await waitForComputed(who, "/v1/home/workspace-activity?limit=6&since_days=7")).payload;
    expect(Array.isArray(res.workspaces), JSON.stringify(res)).toBe(true);
    const text = JSON.stringify(res);
    expect(text).not.toContain(mine!.id);
    expect(text).not.toContain(secret);
    for (const row of res.workspaces) {
      expect(["org", "team"]).toContain(row.kind);
      expect(row.total_corals_window).toBeGreaterThan(0);
    }
  }

  for (const bad of ["limit=0", "limit=21", "since_days=0", "since_days=31"]) {
    const res = await org.adminInOrg.call("GET", `/v1/home/workspace-activity?${bad}`);
    expect(res.status(), bad).toBe(400);
  }
  expect((await request.get(`${API_BASE}/v1/home/workspace-activity`)).status()).toBe(401);
});
