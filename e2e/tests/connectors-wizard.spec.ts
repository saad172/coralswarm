import { expect, test } from "../lib/fixtures";
import { computedEnvelope, refreshAccepted } from "../lib/readModels";

// UI contract coverage. Real provider/capture validation requires B3+B4 in the
// running stack; these routes deliberately simulate only the catalog boundary.
test("scope close/resume and explicit history choice", async ({ page, signedIn }) => {
  void signedIn;
  let scopeSaved = false;
  let history: unknown;
  const source = { key: "channels", label: "Channels", enabled: true, paused: false, status: "off", scope_shape: "items", scope: null, history: { supported: true, max_days: 90 }, last_captured_at: null, week: [], history_days: null, history_requested_at: null, setup_completed_at: null, backfill: null };
  const connector = { key: "slack", name: "Slack", beta: false, status: "setting_up", sources_on: 1, sources_total: 1, last_captured_at: null, importing: null, problem: null, resume_step: "scope", sources: [source] };
  await page.route("**/v1/connectors**", async route => {
    const path = new URL(route.request().url()).pathname;
    let body: unknown = path === "/v1/connectors/slack" ? computedEnvelope("connector_detail", connector) : computedEnvelope("connectors_list", { connectors: [connector] });
    if (path.endsWith("scope-items")) body = { items: [{ id: "general", label: "General", selected: false, meta: null }], next_cursor: null, total: 1 };
    if (path.endsWith("/scope")) { expect(route.request().postDataJSON()).toEqual({ mode: "pick", item_ids: ["general"] }); scopeSaved = true; body = source; }
    if (path.endsWith("/backfill")) { history = route.request().postDataJSON(); body = source; }
    if (path.endsWith("/refresh")) return route.fulfill(refreshAccepted());
    await route.fulfill({ json: body });
  });
  await page.goto("/connectors");
  await page.getByRole("button", { name: "Continue setup Slack" }).click();
  await expect(page.getByRole("checkbox", { name: "General" })).not.toBeChecked();
  await page.getByRole("button", { name: "Close", exact: true }).first().click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page.getByRole("button", { name: "Continue setup Slack" }).click();
  await page.getByText("General", { exact: true }).click();
  await expect(page.getByRole("checkbox", { name: "General" })).toBeChecked();
  await page.getByRole("button", { name: "Continue", exact: true }).click();
  await page.getByText("Last 7 days", { exact: true }).click();
  await expect(page.getByRole("radio", { name: "Last 7 days" })).toBeChecked();
  await page.getByRole("button", { name: "Finish" }).click();
  await expect(page.getByText("Setup saved. History imports continue in the background.")).toBeVisible();
  await expect(page.getByRole("link", { name: "View Slack" })).toHaveAttribute("href", "/connectors/slack");
  expect(scopeSaved).toBe(true);
  expect(history).toEqual({ days: 7 });
});
