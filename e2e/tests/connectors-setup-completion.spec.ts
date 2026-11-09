import { expect, test } from "../lib/fixtures";
import { PORTS } from "../stack/config.mjs";
import { freshPayload } from "../lib/readModels";

test("fresh toolkit setup persists completion across scope and Done reloads", async ({ page, signedIn, request }) => {
 const api = signedIn.api;
 const start = await api.json("POST", "/v1/connectors/jira/connect", { return_to: "wizard" });
 const account = start.authorize_url.split("/").pop();
 expect((await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`)).ok()).toBe(true);
 await api.json("POST", "/v1/connectors/jira/connect/complete", { pending_id: start.pending_id });
 const fresh = await freshPayload(api, "/v1/connectors/jira");
 expect(fresh.status).toBe("setting_up");
 expect(fresh.sources[0].setup_completed_at).toBeNull();
 // Enabling alone does not complete setup; the wizard must durably save Everything scope.
 await api.json("PATCH", "/v1/connectors/jira/sources/workspace", { enabled: true });
 expect((await freshPayload(api, "/v1/connectors/jira")).resume_step).toBe("scope");
 await page.goto("/connectors");
 // Reload before opening the wizard: toolkit scope is saved automatically on mount,
 // so its transient scope heading is not an interactive checkpoint.
 await expect(page.getByRole("button", { name: "Continue setup Jira" })).toBeVisible();
 await page.reload();
 await expect(page.getByRole("button", { name: "Continue setup Jira" })).toBeVisible();
 const pending = await freshPayload(api, "/v1/connectors/jira");
 expect(pending.status).toBe("setting_up");
 expect(pending.resume_step).toBe("scope");
 expect(pending.sources[0].setup_completed_at).toBeNull();
 await page.getByRole("button", { name: "Continue setup Jira" }).click();
 await expect(page.getByRole("heading", { name: "Done", exact: true })).toBeVisible();
 await expect(page.getByRole("link", { name: "View Jira" })).toBeVisible();
 const completed = await freshPayload(api, "/v1/connectors/jira");
 expect(completed.status).toBe("active");
 expect(completed.resume_step).toBeNull();
 expect(completed.sources[0].setup_completed_at).not.toBeNull();
 expect(completed.sources[0].history_requested_at).toBeNull();
 expect(completed.sources[0].scope.mode).toBe("everything");
 await page.reload();
 await expect(page.getByRole("button", { name: "Continue setup Jira" })).toHaveCount(0);
 const reloaded = await freshPayload(api, "/v1/connectors/jira");
 expect(reloaded.status).toBe("active");
 expect(reloaded.resume_step).toBeNull();
 expect(reloaded.sources[0].setup_completed_at).toBe(completed.sources[0].setup_completed_at);
 expect(reloaded.sources[0].scope.mode).toBe("everything");
 expect(reloaded.sources[0].history_requested_at).toBeNull();
});

for (const provider of ["jira", "linear", "confluence", "hubspot", "zendesk", "dart"]) {
 test(`${provider}: fresh unsupported-history setup has a durable completion`, async ({ request, makeUser }) => {
  const { api } = await makeUser(`setup-${provider}`);
  await api.graphs();
  const root = `/v1/connectors/${provider}`;
  const start = await api.json("POST", `${root}/connect`, { return_to: "wizard" });
  const account = start.authorize_url.split("/").pop();
  expect((await request.post(`http://127.0.0.1:${PORTS.fakes}/__composio/authorize/${account}`)).ok()).toBe(true);
  await api.json("POST", `${root}/connect/complete`, { pending_id: start.pending_id });
  expect((await freshPayload(api, root)).status).toBe("setting_up");
  await api.json("PATCH", `${root}/sources/workspace`, { enabled: true });
  const before = await freshPayload(api, root);
  expect(before.resume_step).toBe("scope");
  expect(before.sources[0].setup_completed_at).toBeNull();
  const confirmed = await api.json("PUT", `${root}/sources/workspace/scope`, { mode: "everything" });
  expect(confirmed.setup_completed_at).not.toBeNull();
  expect(confirmed.history_requested_at).toBeNull();
  const reloaded = await freshPayload(api, root);
  expect(reloaded.status).toBe("active");
  expect(reloaded.resume_step).toBeNull();
  expect(reloaded.sources[0].setup_completed_at).toBe(confirmed.setup_completed_at);
  const again = await api.json("PUT", `${root}/sources/workspace/scope`, { mode: "everything" });
  expect(again.setup_completed_at).toBe(confirmed.setup_completed_at);
 });
}
