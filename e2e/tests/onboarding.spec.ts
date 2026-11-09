import { expect, test } from "../lib/fixtures";
import { computedEnvelope, refreshAccepted } from "../lib/readModels";

// Personal first-run onboarding is three steps: Secure → Connectors → First
// answer. The retired desktop-app step's old address lands on Connectors.
test("Secure continues straight to Connectors", async ({ page, signedIn }) => {
  void signedIn;
  await page.goto("/onboarding/secure");
  await expect(page.getByText("step 1 of 3")).toBeVisible();
  await page.getByRole("button", { name: "Continue" }).click();
  await expect(page).toHaveURL(/\/onboarding\/connectors\/?$/);
  await expect(page.getByText("step 2 of 3")).toBeVisible();
});

test("the retired /onboarding/setup step redirects to Connectors", async ({ page, signedIn }) => {
  void signedIn;
  await page.goto("/onboarding/setup");
  await expect(page).toHaveURL(/\/onboarding\/connectors\/?$/);
  await expect(page.getByText("step 2 of 3")).toBeVisible();
});

test("the retired /settings/capture page redirects to Privacy", async ({ page, signedIn }) => {
  void signedIn;
  await page.goto("/settings/capture");
  await expect(page).toHaveURL(/\/settings\/privacy\/?$/);
});

test("onboarding shares the connector wizard and can continue after cancellation", async ({ page, signedIn }) => {
  void signedIn;
  await page.goto("/onboarding/connectors");
  await expect(page.locator("[data-connector]")).toHaveCount(10);
  await expect(page.getByText("Start here")).toBeVisible();
  await page.locator('[data-connector="slack"]').getByRole("button").click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.getByRole("dialog").getByRole("button", { name: "Close", exact: true }).last().click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page).toHaveURL(/\/onboarding\/connectors\/?$/);
  await page.getByRole("button", { name: "Ask your first question" }).click();
  await expect(page).toHaveURL(/\/onboarding\/answer\/?$/);
});

test("connections settings points to the shared MCP band", async ({ page, signedIn }) => {
  void signedIn;
  await page.goto("/settings/connections");
  await expect(page.getByText("CoralSwarm address", { exact: true })).toHaveCount(0);
  await page.getByRole("main").getByRole("link", { name: "Connectors", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Use CoralSwarm anywhere" })).toBeVisible();
});

test("connector callback returns to onboarding and resumes setup", async ({ page, signedIn }) => {
  void signedIn;
  const source = {
    key: "channels",
    label: "Channels",
    enabled: false,
    paused: false,
    status: "off",
    scope_shape: "items",
    scope: null,
    history: { supported: true, max_days: 90 },
    last_captured_at: null,
    week: [],
    history_days: null,
    history_requested_at: null,
    setup_completed_at: null,
    backfill: null,
  };
  const enabled = { ...source, enabled: true };
  const connector = {
    key: "slack",
    name: "Slack",
    beta: false,
    status: "setting_up",
    sources_on: 0,
    sources_total: 1,
    last_captured_at: null,
    importing: null,
    problem: null,
    resume_step: "sources",
    sources: [source],
  };
  let enabledDurably = false;
  await page.route("**/v1/connectors**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    let body: unknown = path === "/v1/connectors/slack" ? computedEnvelope("connector_detail", connector) : computedEnvelope("connectors_list", { connectors: [connector] });
    // Slack is one source: the wizard PATCHes it on, then resumes on scope.
    if (path.endsWith("/sources/channels") && route.request().method() === "PATCH") {
      expect(route.request().postDataJSON()).toEqual({ enabled: true });
      enabledDurably = true;
      body = enabled;
    }
    if (path.endsWith("scope-items")) {
      body = { items: [{ id: "general", label: "General", selected: false, meta: null }], next_cursor: null, total: 1 };
    }
    if (path.endsWith("/refresh")) return route.fulfill(refreshAccepted());
    await route.fulfill({ json: body });
  });
  await page.goto("/onboarding/connectors");
  await page.evaluate(() => sessionStorage.setItem("connector-return:slack", "onboarding"));
  await page.goto("/connectors/return?provider=slack&result=ok");
  await expect(page).toHaveURL(/\/onboarding\/connectors\?setup=slack/);
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Choose scope: Channels" })).toBeVisible();
  await expect(page.getByRole("checkbox", { name: "General" })).toBeVisible();
  expect(enabledDurably).toBe(true);
});
