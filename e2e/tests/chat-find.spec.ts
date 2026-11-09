import crypto from "node:crypto";
import { expect, test } from "../lib/fixtures";
import { personalUser } from "../lib/isolation";

// Chat find (`source_results` on the scoped answer, GET /v1/find/titles). The
// first scenario is API level, the second drives the chat UI. Contract: docs/implementation/find-results-api.md. The stack boots the
// backend with CHAT_FIND_RESULTS_ENABLED=1 (stack/config.mjs).
//
// One user captures a meeting and a Slack thread through the real ingest route
// and has their extraction written by the one seeding route. A find question
// answers with one tile per SOURCE (the meeting, with its passages as quotes),
// a plain question carries no tiles, the title search finds the Slack thread by
// its channel, and a second user (their own account and ocean) finds neither.

test.describe("Chat find", () => {
  test("a find question answers with a source tile, and the title search finds the thread", async ({ request }) => {
    const [owner, other] = await Promise.all([personalUser(request, "find-owner", "Priya Nair"), personalUser(request, "find-other")]);
    const ocean = await owner.api.personalGraph();
    const theirOcean = await other.api.personalGraph();
    const token = `halcyon${crypto.randomUUID().slice(0, 8)}`;

    await owner.api.seedSource(
      ocean.id,
      `The ${token} pricing review chose three usage tiers. Tom Becker owns the ${token} rollout checklist.`,
      `e2e-meet-${crypto.randomUUID()}`,
      { sourceType: "transcript", participants: ["Tom Becker", "Sara Kim"], durationMs: 42 * 60_000 },
    );
    const channel = `gtm-${token}`;
    await owner.api.seedSource(ocean.id, `Open the ${token} offer at tier B, no lower.`, `e2e-slack-${crypto.randomUUID()}`, {
      sourceType: "slack",
      channelName: channel,
    });

    // A find question: tiles ride on the answer, one per source, best first.
    const ask = (api: typeof owner.api, text: string, graphId: string) =>
      api.json("POST", "/v1/queries", { text, library_scope: { graph_ids: [graphId] } });
    const found = await ask(owner.api, `Find the ${token} pricing meeting`, ocean.id);
    const tiles = found.source_results;
    expect(Array.isArray(tiles), `no source_results in ${JSON.stringify(found).slice(0, 600)}`).toBe(true);
    expect(tiles.length).toBeGreaterThan(0);
    expect(tiles.length).toBeLessThanOrEqual(3);
    expect(new Set(tiles.map((t: any) => t.source_item_id)).size).toBe(tiles.length); // one tile per source
    expect(tiles.map((t: any) => t.best)).toEqual(tiles.map((_: any, i: number) => i === 0));
    const meeting = tiles.find((t: any) => t.kind === "meeting");
    expect(meeting, `no meeting tile in ${JSON.stringify(tiles).slice(0, 600)}`).toBeTruthy();
    expect(meeting.kind_label).toBe("meeting");
    expect(meeting.graph).toEqual({ id: ocean.id, name: ocean.name });
    expect(meeting.duration_ms).toBe(42 * 60_000);
    expect(meeting.people).toEqual(expect.arrayContaining(["Tom Becker", "Sara Kim"]));
    expect(meeting.quotes.length).toBeGreaterThan(0);
    expect(meeting.quotes[0].quote).toContain(token);

    // A question that is not a find question carries no tiles.
    const plain = await ask(owner.api, `Who owns the ${token} rollout checklist?`, ocean.id);
    expect(plain.source_results).toBeUndefined();

    // Title search: the Slack thread by its channel, case-insensitively, in the
    // same tile shape (no best, no quotes).
    const titles = await owner.api.json("GET", `/v1/find/titles?q=${encodeURIComponent(channel.toUpperCase())}`);
    expect(titles.results).toHaveLength(1);
    expect(titles.results[0]).toMatchObject({ kind: "slack", kind_label: "slack thread", title: `#${channel}`, best: false, quotes: [] });
    expect(titles.results[0].graph).toEqual({ id: ocean.id, name: ocean.name });
    expect((await owner.api.call("GET", "/v1/find/titles")).status()).toBe(400);

    // The other user: no tiles for the same wording over their own ocean, and
    // the title search never returns the owner's thread.
    const theirs = await ask(other.api, `Find the ${token} pricing meeting`, theirOcean.id);
    expect(JSON.stringify(theirs.source_results ?? [])).not.toContain(token);
    expect(theirs.source_results ?? []).toEqual([]);
    const theirTitles = await other.api.json("GET", `/v1/find/titles?q=${encodeURIComponent(channel)}`);
    expect(theirTitles.results).toEqual([]);
  });
});

// The chat UI: a find question renders the result tile under the answer, the
// tile opens the shared detail panel from its own data, Esc closes it, and the
// fallback line opens the Cmd-K title search.
test("a find question shows a result tile that opens the detail panel", async ({ page, signedIn }) => {
  const ocean = await signedIn.api.personalGraph();
  const token = `halcyon${crypto.randomUUID().slice(0, 8)}`;
  await signedIn.api.seedSource(
    ocean.id,
    `The ${token} pricing review chose three usage tiers. Tom Becker owns the ${token} rollout checklist.`,
    `e2e-meet-${crypto.randomUUID()}`,
    { sourceType: "transcript", participants: ["Tom Becker", "Sara Kim"], durationMs: 42 * 60_000 },
  );

  await page.goto(`/chats?graph_id=${ocean.id}`);
  const question = page.getByRole("textbox", { name: "Question" });
  await expect(question).toBeEnabled();
  await question.fill(`Find the ${token} pricing meeting`);
  await question.press("Enter");

  const tiles = page.getByRole("region", { name: "Matching sources" });
  await expect(tiles).toBeVisible({ timeout: 20_000 });
  // The tile's name is its own text (kind, title, date line), not the quoted
  // passages: a meeting with no recorded title is titled "Meeting", so match on
  // the recorded length instead of the seeded token.
  const tile = tiles.getByRole("button", { name: /42 min/ }).first();
  await expect(tile).toContainText("best match");
  await expect(tile).toContainText("42 min");
  await expect(tiles.getByText(/Not the one\?/)).toBeVisible();

  // Open it: the panel paints from the tile, and Esc closes it.
  await tile.click();
  const panel = page.getByRole("dialog", { name: /meeting:/ });
  await expect(panel).toBeVisible();
  await expect(panel.getByRole("button", { name: "Ask about this" })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(panel).toBeHidden();

  // The key in the fallback line opens the title search.
  await tiles.getByRole("button", { name: /Search titles/ }).click();
  await expect(page.getByRole("dialog", { name: /search/i })).toBeVisible();
});
