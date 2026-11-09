import crypto from "node:crypto";
import { expect, test } from "../lib/fixtures";
import { Api } from "../lib/api";
import { createUser } from "../lib/clerk";
import { API_BASE, FAKE, REMOTE } from "../stack/config.mjs";

// Slack events reach the ingest door with a routing grant (ADR-0016): a
// message in a channel lands in the ocean that channel routes to. The
// workspace connects through the real OAuth routes against the fake Slack
// (stack/fakes.mjs), and the event arrives signed on the real webhook, so the
// path is the production one from webhook to coral: the durable job, the
// worker, the channel grant, and the capture sealed in the grant's
// transaction. The first scenario takes the account-default route (no
// `slack_watched_channels` row); the second maps the channel through the
// admin route the web app uses, so the row route, with its lock and equality
// check at the seal, is exercised end to end as well.

test.skip(REMOTE, "the fake Slack is local-stack only");

const id = (prefix: string, n: number) => `${prefix}${crypto.randomUUID().replace(/-/g, "").slice(0, n).toUpperCase()}`;

/** Post one signed Slack Events API callback, as Slack does. A `retry` is
 *  Slack's redelivery of the same event: same `event_id`, with the retry
 *  headers Slack sets. */
async function deliver(
  request: import("@playwright/test").APIRequestContext,
  team: string,
  eventId: string,
  event: Record<string, unknown>,
  retry?: number,
) {
  const body = JSON.stringify({ type: "event_callback", team_id: team, event_id: eventId, event });
  const ts = String(Math.floor(Date.now() / 1000));
  const signature = `v0=${crypto.createHmac("sha256", FAKE.slackSigningSecret).update(`v0:${ts}:${body}`).digest("hex")}`;
  const headers: Record<string, string> = { "content-type": "application/json", "x-slack-request-timestamp": ts, "x-slack-signature": signature };
  if (retry !== undefined) {
    headers["x-slack-retry-num"] = String(retry);
    headers["x-slack-retry-reason"] = "http_timeout";
  }
  const res = await request.post(`${API_BASE}/v1/slack/events`, { data: body, headers });
  expect(res.status(), await res.text()).toBe(200);
}

/** Connect a workspace of this scenario's own through the real OAuth routes:
 *  the fake Slack names the workspace after the OAuth code. */
async function connectWorkspace(api: Api, request: import("@playwright/test").APIRequestContext) {
  const team = id("T", 10);
  const start = await api.json("GET", "/v1/integrations/slack/oauth/start");
  const state = new URL(start.url).searchParams.get("state");
  expect(state, start.url).toBeTruthy();
  const callback = await request.get(`${API_BASE}/v1/oauth/slack/callback?code=${team}&state=${encodeURIComponent(String(state))}`, { maxRedirects: 0 });
  expect(callback.status(), await callback.text()).toBeLessThan(400);
  return team;
}

/** The Slack sources in an ocean, by source id. */
async function slackSources(api: Api, graphId: string) {
  const list = await api.json("GET", `/v1/sources/items?graph_id=${graphId}&source_type=slack`);
  return ((list.items ?? []) as { source_id: string }[]).map((i) => i.source_id).sort();
}

/** The chunk corals of one Slack message in an ocean. */
async function chunksOf(api: Api, graphId: string, sourceId: string) {
  const corals = (await api.json("GET", `/v1/graphs/${graphId}/atoms?page_size=200`)) as { source_type: string; source_id: string }[];
  return corals.filter((c) => c.source_type === "slack" && c.source_id.startsWith(`${sourceId}:`));
}

test("a Slack message lands in the ocean its channel routes to, once, across a retry", async ({ request }) => {
  const user = await createUser("slack-ingest");
  const api = new Api(request, user.id);
  const ocean = await api.personalGraph();
  const team = await connectWorkspace(api, request);

  const channel = "C0E2EROUTE";
  const second = Math.floor(Date.now() / 1000);
  const message = { type: "message", channel, user: "U0E2ERILEY", ts: `${second}.000100`, text: `The ${team} launch review moved to Thursday.` };
  const sentinel = { type: "message", channel, user: "U0E2ERILEY", ts: `${second}.000200`, text: `Riley: noted, ${team}.` };
  const sourceId = `${channel}/${message.ts}`;

  // A channel with no destination row routes to a personal account's own
  // ocean.
  const eventId = id("Ev", 9);
  await deliver(request, team, eventId, message);
  await expect.poll(() => slackSources(api, ocean.id), { timeout: 20_000, intervals: [500, 1_000] }).toEqual([sourceId]);

  // Slack redelivers the event once its first job is already done, so the
  // retry is not folded into a pending job: it is a second job that captures
  // the same source again, and must adopt the coral it finds rather than
  // write a second. A deduplicated capture leaves no trace of its own (no
  // new version, `updated_at` untouched) and no supported read exposes
  // pending ingest jobs, so the retry's completion is bounded two ways. A
  // sentinel message follows it: the worker dequeues a tenant's jobs in
  // `run_after` order on one loop, so once the sentinel has landed the
  // earlier-enqueued retry has run. Then the counts are re-read across a
  // settle window longer than the worker's idle poll (500 ms), in case the
  // retry's background enqueue ever finished after the sentinel's.
  await deliver(request, team, eventId, message, 1);
  await deliver(request, team, id("Ev", 9), sentinel);
  await expect.poll(() => slackSources(api, ocean.id), { timeout: 20_000, intervals: [500, 1_000] }).toEqual([sourceId, `${channel}/${sentinel.ts}`]);

  // The retried message is one source with one version and one chunk coral.
  const list = await api.json("GET", `/v1/sources/items?graph_id=${ocean.id}&source_type=slack`);
  const item = (list.items as { source_id: string; source_item_id: string }[]).find((i) => i.source_id === sourceId);
  expect(item, JSON.stringify(list)).toBeTruthy();
  for (let i = 0; i < 4; i++) {
    const evidence = await api.json("GET", `/v1/sources/evidence?graph_id=${ocean.id}&source_item_id=${item!.source_item_id}`);
    expect(evidence.versions, JSON.stringify(evidence)).toHaveLength(1);
    const chunks = await chunksOf(api, ocean.id, sourceId);
    expect(chunks, JSON.stringify(chunks)).toHaveLength(1);
    await new Promise((r) => setTimeout(r, 500));
  }
});

test("a channel mapped through the admin route lands in the mapped ocean, not the account default", async ({ request }) => {
  const user = await createUser("slack-mapped");
  const api = new Api(request, user.id);
  const home = await api.personalGraph();
  const launch = await api.json("POST", "/v1/graphs", { name: "Launch room", kind: "personal" });
  expect(launch.id, JSON.stringify(launch)).toBeTruthy();
  const team = await connectWorkspace(api, request);

  // The web app's channel-destination action: `PUT /v1/connectors/slack/
  // channels/{channel}/destination` writes the `slack_watched_channels` row
  // the grant is built from and locks at its seal.
  const channel = "C0E2EMAPPED";
  const mapped = await api.call("PUT", `/v1/connectors/slack/channels/${channel}/destination`, { kind: "single", graph_id: launch.id });
  const mappedBody = await mapped.text();
  expect(mapped.ok(), mappedBody).toBeTruthy();
  expect(JSON.parse(mappedBody).destination, mappedBody).toMatchObject({ kind: "single", graph_id: launch.id });

  const second = Math.floor(Date.now() / 1000);
  const message = { type: "message", channel, user: "U0E2ERILEY", ts: `${second}.000300`, text: `${team}: the launch room is booked for Friday.` };
  await deliver(request, team, id("Ev", 9), message);
  await expect.poll(() => slackSources(api, launch.id), { timeout: 20_000, intervals: [500, 1_000] }).toEqual([`${channel}/${message.ts}`]);
  expect(await chunksOf(api, launch.id, `${channel}/${message.ts}`)).toHaveLength(1);

  // The account default received nothing: the row named the ocean.
  expect(await slackSources(api, home.id)).toEqual([]);
});
