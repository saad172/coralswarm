import { expect, test } from "../lib/fixtures";

// The redesigned Connectors page (connectors redesign, layer W1), behind
// NEXT_PUBLIC_CONNECTORS_V2, which e2e/stack/build-web.mjs turns on. A fresh
// Clerk user has nothing connected, so every card reads "not connected". The
// API side (ten connectors, six Beta, no broker name) is `connectors-api.spec.ts`.

const NAMES = ["Slack", "Microsoft", "Google", "GitHub", "Linear", "Jira", "Confluence", "HubSpot", "Zendesk", "Dart"];
const MCP_URL = "https://api.coralswarm.example/mcp";

test.beforeEach(async ({ page, signedIn }) => {
  void signedIn;
  await page.goto("/connectors");
  await expect(page.getByRole("heading", { name: "Connectors", exact: true })).toBeVisible();
  // The grid has loaded once the first card is there.
  await expect(page.locator("[data-connector]").first()).toBeVisible();
});

test("the grid is ten cards with the right names, none connected", async ({ page }) => {
  const cards = page.locator("[data-connector]");
  await expect(cards).toHaveCount(10);
  for (const [i, name] of NAMES.entries()) {
    await expect(cards.nth(i).getByText(name, { exact: true })).toBeVisible();
    await expect(cards.nth(i).getByText("Not connected")).toBeVisible();
    await expect(cards.nth(i).getByRole("button", { name: `Connect ${name}` })).toBeVisible();
  }
});

test("exactly six cards carry a Beta tag, and nothing names the broker behind them", async ({ page }) => {
  await expect(page.getByText("Beta", { exact: true })).toHaveCount(6);
  for (const name of ["Linear", "Jira", "Confluence", "HubSpot", "Zendesk", "Dart"]) {
    await expect(page.locator("[data-connector]", { hasText: name }).getByText("Beta", { exact: true })).toBeVisible();
  }
  await expect(page.locator("body")).not.toContainText(/composio/i);
});

test("the MCP band sits above the grid and its pill copies the address", async ({ page, context }) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  const title = page.getByRole("heading", { name: "Use CoralSwarm anywhere" });
  await expect(title).toBeVisible();
  const above = await title.evaluate(
    (el) => el.compareDocumentPosition(document.querySelector("[data-connector]")!) & Node.DOCUMENT_POSITION_FOLLOWING,
  );
  expect(above).toBeTruthy();

  const pill = page.getByRole("button", { name: `Copy the MCP address ${MCP_URL}` });
  await expect(pill).toContainText(MCP_URL);
  await expect(pill).toContainText("copy");
  await pill.click();
  await expect(pill).toContainText("copied ✓");
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(MCP_URL);
});

test("the desktop tile, the Zoom tile and 'other ways in' are gone", async ({ page }) => {
  const body = page.locator("body");
  await expect(body).not.toContainText(/other ways in/i);
  await expect(body).not.toContainText(/coming soon/i);
  await expect(page.getByText("Zoom", { exact: true })).toHaveCount(0);
  await expect(page.getByText(/desktop/i)).toHaveCount(0);
  await expect(page.getByText(/paste, upload, or drop a transcript/i)).toHaveCount(0);
});
