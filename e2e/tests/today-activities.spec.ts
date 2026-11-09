import { waitForComputed } from "../lib/readModels";
import crypto from "node:crypto";
import { expect, test } from "../lib/fixtures";
import { personalUser } from "../lib/isolation";

// Today activity cards (GET /v1/journal/activities, .../{key}): the API
// contract, and the Today page built on it (design 3a day, 3b side panel, 3d
// week). Contract: docs/implementation/journal-activities-api.md. The stack
// boots the backend with TODAY_ACTIVITIES_ENABLED=1 and TODAY_GIST_ENABLED=1
// (stack/config.mjs).
//
// One user captures a note through the real ingest route and has its
// extraction written by the one seeding route. The card shows up under Mine and
// Team with its decision-free fallbacks, the detail returns the extracted
// passage, and a second user (their own ocean, their own account) never sees
// the card, nor can open it by key or by naming the first user's ocean.

function windowAround(now: Date) {
  const from = new Date(now.getTime() - 24 * 3600_000).toISOString();
  const to = new Date(now.getTime() + 24 * 3600_000).toISOString();
  return `from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}&tz=UTC`;
}

test.describe("Today activity cards", () => {
  test.describe.configure({ timeout: 180_000 });
  test("a captured note is a card with a passage, and stays private to its owner", async ({ request }) => {
    const [owner, other] = await Promise.all([personalUser(request, "act-owner", "Priya Nair"), personalUser(request, "act-other")]);
    const ocean = await owner.api.personalGraph();
    const otherOcean = await other.api.personalGraph();
    const secret = `halcyon-${crypto.randomUUID().slice(0, 8)}`;
    const { sourceId } = await owner.api.seedSource(
      ocean.id,
      `The ${secret} launch moved to March 14. Priya owns the rollout checklist.`,
    );
    const captured = new Date();
    const range = windowAround(captured);

    // Mine: one note card, owned by the caller, with the ocean attached.
    const mine = (await waitForComputed(owner.api, `/v1/journal/activities?${range}&who=mine`, true, p => p.activities.some((a: any) => a.kind === "note"))).payload;
    const card = mine.activities.find((a: any) => a.kind === "note");
    expect(card, `no note card in ${JSON.stringify(mine).slice(0, 400)}`).toBeTruthy();
    expect(card.graph).toEqual({ id: ocean.id, name: ocean.name });
    expect(card.owner.name).toBe("Priya Nair");
    expect(card.kind_label).toBe("note");
    expect(card.source_count).toBeGreaterThan(0);
    expect(card.outcome).toBeNull(); // no decision was extracted, and nothing calls a model
    expect(mine.day_activity).toHaveLength(14);
    // The strip ends at the day holding the instant just before `to` (here
    // tomorrow, since the window is now +/- 24h), so the capture is in its
    // bucket for today's UTC date, not the last one.
    const today = captured.toISOString().slice(0, 10);
    const bucket = mine.day_activity.find((d: any) => d.day === today);
    expect(bucket, `no ${today} bucket in ${JSON.stringify(mine.day_activity)}`).toBeTruthy();
    expect(bucket.count).toBeGreaterThan(0);
    expect(mine.window).toMatchObject({ who: "mine", tz: "UTC" });

    // Team, as the same user, includes it too.
    const team = (await waitForComputed(owner.api, `/v1/journal/activities?${range}&who=team`)).payload;
    expect(team.activities.map((a: any) => a.key)).toContain(card.key);

    // Detail: the extracted passage, labelled with its kind and time of day.
    const detail = await owner.api.json("GET", `/v1/journal/activities/${encodeURIComponent(card.key)}?graph_id=${ocean.id}&tz=UTC`);
    expect(detail.key).toBe(card.key);
    expect(detail.sources.length).toBeGreaterThan(0);
    expect(detail.sources[0].quote).toContain(secret);
    expect(detail.sources[0].meta_label).toMatch(/^NOTE · \d{2}:\d{2}$/);
    expect(Array.isArray(detail.decisions)).toBe(true);
    expect(Array.isArray(detail.reefs)).toBe(true);
    expect(sourceId).toBeTruthy();

    // The other user: Team and Mine are empty of the card, the key is a 404
    // through their own ocean, and naming the owner's ocean is a 404 too.
    for (const who of ["mine", "team"]) {
      const theirs = (await waitForComputed(other.api, `/v1/journal/activities?${range}&who=${who}`)).payload;
      expect(JSON.stringify(theirs)).not.toContain(secret);
      expect(theirs.activities.map((a: any) => a.key)).not.toContain(card.key);
    }
    const viaOwn = await other.api.call("GET", `/v1/journal/activities/${encodeURIComponent(card.key)}?graph_id=${otherOcean.id}`);
    expect(viaOwn.status()).toBe(404);
    const viaTheirs = await other.api.call("GET", `/v1/journal/activities/${encodeURIComponent(card.key)}?graph_id=${ocean.id}`);
    expect(viaTheirs.status()).toBe(404);
    const narrowed = await other.api.call("GET", `/v1/journal/activities?${range}&who=team&graph_id=${ocean.id}`);
    expect(narrowed.status()).toBe(404);
  });

  test("the window and filters are validated", async ({ request }) => {
    const user = await personalUser(request, "act-valid");
    await user.api.graphs();
    const bad = async (query: string) => (await user.api.call("GET", `/v1/journal/activities?${query}`)).status();
    expect(await bad("from=2026-10-01T00:00:00Z")).toBe(400); // no `to`
    expect(await bad("from=2026-10-02T00:00:00Z&to=2026-10-01T00:00:00Z")).toBe(400);
    expect(await bad("from=2026-01-01T00:00:00Z&to=2026-06-01T00:00:00Z")).toBe(400); // over 62 days
    expect(await bad("from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z&who=everyone")).toBe(400);
    expect(await bad("from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z&tz=Mars/Base")).toBe(400);
    const empty = (await waitForComputed(user.api, "/v1/journal/activities?from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z&tz=UTC")).payload;
    expect(empty.activities).toEqual([]);
    expect(empty.day_activity).toHaveLength(14);
  });
});

// The page itself. One user captures a note through the real ingest route and
// has its extraction written by the one seeding route (`seedSource`), then opens
// Today: the note is a card, the side panel shows the extracted passage, Esc
// closes it, and Week still lists it. No model runs, so the gist stays hidden
// (the page's own fallback) while the stats and chart render.
test("Today shows a captured note as a card, opens its side panel, and switches to Week", async ({ page, signedIn }) => {
  const ocean = await signedIn.api.personalGraph();
  const secret = `halcyon-${crypto.randomUUID().slice(0, 8)}`;
  await signedIn.api.seedSource(ocean.id, `The ${secret} launch moved to March 14. Priya owns the rollout checklist.`);

  await page.goto("/today");
  await expect(page.getByRole("group", { name: "Last 14 days" })).toBeVisible();
  await expect(page.getByRole("tab", { name: "Day", exact: true })).toHaveAttribute("aria-selected", "true");

  // 3a: the note is a card on the timeline; the workstream panel counts it.
  const card = page.getByRole("button", { name: /\bnote\b/ });
  await expect(card).toBeVisible({ timeout: 20_000 });
  await expect(page.getByText("1 event", { exact: true })).toBeVisible();
  const workstream = page.getByRole("complementary", { name: "Workstream" });
  await expect(workstream.getByText("your workstream")).toBeVisible();
  await expect(workstream.getByText("activities", { exact: true })).toBeVisible();
  await expect(workstream.getByRole("img", { name: "time by hour" })).toBeVisible();

  // 3b: the side panel opens over the page with the extracted passage; Esc closes it.
  await card.click();
  const panel = page.getByRole("dialog", { name: /^note:/ });
  await expect(panel).toBeVisible();
  // The passage shows in the title and again under "came from" (one row per
  // extracted quote), so pin each by role/exact text rather than a loose match.
  await expect(panel.getByRole("heading", { level: 2, name: `The ${secret} launch moved to March 14` })).toBeVisible();
  const cameFrom = panel.getByRole("region", { name: "Came from" });
  await expect(cameFrom.getByText(`The ${secret} launch moved to March 14.`, { exact: true })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(panel).toBeHidden();

  // 3d: Week keeps the card, under the week's title and a day heading.
  await page.getByRole("tab", { name: "Week", exact: true }).click();
  await expect(page).toHaveURL(/z=week/);
  await expect(page.getByRole("heading", { level: 1, name: /^Week of / })).toBeVisible();
  await expect(page.getByRole("button", { name: /\bnote\b/ })).toBeVisible();
  await expect(workstream.getByRole("img", { name: "time by day" })).toBeVisible();
});
