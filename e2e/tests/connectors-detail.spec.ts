import { expect, test } from "../lib/fixtures";
import { computedEnvelope } from "../lib/readModels";
// Mocked UI contract test; captured-data retention requires the integrated stack.
test("static Slack route uses generic management, pause, feed and disconnect", async ({ page, signedIn }) => {
 void signedIn;
 const source = { key: "channels", label: "Channels", enabled: true, paused: false, status: "active", scope_shape: "items", scope: { mode: "pick", count: 1, noun: "channels" }, history: { supported: true, max_days: 90 }, week: [0,1,0,0,0,0,0], last_captured_at: null, history_days: null, history_requested_at: null, setup_completed_at: null, backfill: null };
 const connector = { key: "slack", name: "Slack", beta: false, status: "active", sources_on: 1, sources_total: 1, last_captured_at: null, importing: null, problem: null, resume_step: null, sources: [source], account_label: "Test workspace", connected_at: null, topics: [], captures: { items: [], next_cursor: null }, watched: { threads: [], next_cursor: null }, addin_tokens: { tokens: [] } };
 let disconnected = false;
 await page.route("**/v1/connectors/slack**", async route => {
 const req = route.request(); const url = new URL(req.url());
 if (req.method() === "DELETE") { disconnected = true; connector.status = "not_connected"; await route.fulfill({ status: 204 }); return; }
 if (req.method() === "PATCH") { Object.assign(source, req.postDataJSON()); source.status = source.paused ? "paused" : "active"; await route.fulfill({ json: source }); return; }
 if (url.pathname.endsWith("/captures")) { await route.fulfill({ json: { items: [], next_cursor: null } }); return; }
 await route.fulfill({ json: computedEnvelope("connector_detail", connector) });
 });
 await page.goto("/connectors/slack");
 await page.getByRole("button", { name: "Pause", exact: true }).click();
 await expect(page.getByRole("button", { name: "Resume", exact: true })).toBeVisible();
 const filteredCaptures = page.waitForResponse(response => {
  const url = new URL(response.url());
  return url.pathname === "/v1/connectors/slack/captures" && url.searchParams.get("source") === "channels" && response.ok();
 });
 const channels = page.getByRole("tab", { name: "Channels", exact: true });
 await channels.click();
 await filteredCaptures;
 await expect(channels).toHaveAttribute("aria-selected", "true");
 await expect(page.getByText("No captures yet.")).toBeVisible();
 await page.getByRole("button", { name: "Disconnect", exact: true }).click();
 const dialog = page.getByRole("dialog");
 await expect(dialog.getByText("What’s already captured stays in your ocean.")).toBeVisible();
 await dialog.getByRole("button", { name: "Disconnect", exact: true }).click();
 await expect(dialog).toHaveCount(0);
 expect(disconnected).toBe(true);
});

for (const [oldPath, destination] of [["microsoft-teams", "microsoft"], ["google-chat", "google"], ["composio?toolkit=jira", "jira"]]) {
 test(`legacy ${oldPath} redirects to consolidated detail`, async ({ page, signedIn }) => {
  void signedIn;
  await page.goto(`/connectors/${oldPath}`);
  await expect(page).toHaveURL(new RegExp(`/connectors/${destination}/?$`));
 });
}

test("all hosted legacy queries and GitHub setup preserve their Next destinations", async ({ page, signedIn }) => {
 void signedIn;
 for (const provider of ["jira", "linear", "confluence", "hubspot", "zendesk", "dart"]) {
  await page.goto(`/connectors/composio?toolkit=${provider}`);
  await expect(page).toHaveURL(new RegExp(`/connectors/${provider}/?$`));
 }
 await page.goto("/connectors/github/setup?installation_id=123&setup_action=update");
 await expect(page).toHaveURL(/\/connectors\/github\?installation_id=123$/);
 await page.goto("/connectors/microsoft-teams/meeting-capture");
 await expect(page).toHaveURL(/\/connectors\/microsoft-teams\/meeting-capture$/);
 await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
});
