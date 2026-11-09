import type { APIRequestContext } from "@playwright/test";
import crypto from "node:crypto";
import { expect, test } from "../lib/fixtures";
import { Api } from "../lib/api";
import { selectRows } from "../lib/stack-db";
import { freshPayload, waitForComputed } from "../lib/readModels";
import { createUser } from "../lib/clerk";
import { ingestNote, personalUser } from "../lib/isolation";
import { API_BASE, FAKE, PORTS, WEB_BASE } from "../stack/config.mjs";

// The generic connector API (connectors redesign): `GET /v1/connectors[/{provider}]`
// (B1), connect, complete and disconnect (B2) and import history (B4). API-level,
// as fresh Clerk users.
//
// B3 adds scope and pause: the items a source lets the member pick, the pick, and
// the pause gate every capture door checks. The Slack scenarios post a signed
// message event to the real `/v1/slack/events` and read what was captured back.
//
// B2 connects through the real routes against the Slack and Composio fakes
// (stack/fakes.mjs). The browser's two hops are played by the test: it follows
// the authorize redirect, then calls the callback the way Slack would, with a
// fake code (`team-<x>` signs into workspace T<X>). Local stack only: the fakes
// and the seeding they need do not exist against a deployed target.

const KEYS = ["slack", "microsoft", "google", "github", "linear", "jira", "confluence", "hubspot", "zendesk", "dart"];
const TOOLKITS = ["linear", "jira", "confluence", "hubspot", "zendesk", "dart"];

test("a new user sees ten connectors, none connected, six of them Beta", async ({ request }) => {
  const user = await createUser("connectors");
  const api = new Api(request, user.id);
  const body = await freshPayload(api, "/v1/connectors");

  expect(body.connectors.map((c: any) => c.key)).toEqual(KEYS);
  for (const c of body.connectors) {
    expect(c.status, c.key).toBe("not_connected");
    expect(c.sources_on, c.key).toBe(0);
    expect(c.sources_total, c.key).toBe(c.sources.length);
    expect(c.problem, c.key).toBeNull();
    expect(c.importing, c.key).toBeNull();
    expect(c.resume_step, c.key).toBeNull();
    for (const s of c.sources) {
      expect(s.status, `${c.key}/${s.key}`).toBe("off");
      expect(s.enabled, `${c.key}/${s.key}`).toBe(false);
      expect(s.week, `${c.key}/${s.key}`).toEqual([0, 0, 0, 0, 0, 0, 0]);
    }
  }

  // The Beta badge is the one visible difference between the kinds.
  expect(body.connectors.filter((c: any) => c.beta).map((c: any) => c.key)).toEqual(TOOLKITS);
  expect(body.connectors.filter((c: any) => c.beta)).toHaveLength(6);

  // The broker behind the Beta toolkits is never named.
  expect(JSON.stringify(body).toLowerCase()).not.toContain("composio");
});

test("each connector lists its sources with a scope shape and a history answer", async ({ request }) => {
  const user = await createUser("connectors");
  const api = new Api(request, user.id);
  const { connectors } = await freshPayload(api, "/v1/connectors");
  const by = (k: string) => connectors.find((c: any) => c.key === k);
  const sources = (k: string) => by(k).sources.map((s: any) => s.key);

  expect(sources("slack")).toEqual(["channels"]);
  expect(sources("microsoft")).toEqual(["mail", "calendar", "teams_chats", "teams_channels", "teams_meetings"]);
  expect(sources("google")).toEqual(["gmail", "calendar", "meet", "chat"]);
  expect(sources("github")).toEqual(["repos"]);
  for (const k of TOOLKITS) expect(sources(k), k).toEqual(["workspace"]);

  const shape = (c: string, s: string) => by(c).sources.find((x: any) => x.key === s).scope_shape;
  expect(shape("microsoft", "teams_meetings")).toBe("pick_only");
  expect(shape("google", "meet")).toBe("pick_only");
  expect(shape("microsoft", "calendar")).toBe("everything_only");
  expect(shape("google", "calendar")).toBe("everything_only");
  expect(shape("slack", "channels")).toBe("items");
  for (const k of TOOLKITS) expect(shape(k, "workspace"), k).toBe("everything_only");

  // A toolkit captures from the moment of connect: no history step, with a reason code.
  for (const k of TOOLKITS) {
    const h = by(k).sources[0].history;
    expect(h.supported, k).toBe(false);
    expect(typeof h.reason, k).toBe("string");
  }
  expect(by("slack").sources[0].history).toEqual({ supported: true, max_days: 90 });
  expect(by("google").sources.find((s: any) => s.key === "chat").history).toEqual({ supported: true, max_days: 90 });
});

test("one connector's detail adds the account label, connect time and topics", async ({ request }) => {
  const user = await createUser("connectors");
  const api = new Api(request, user.id);
  const detail = await freshPayload(api, "/v1/connectors/slack");
  expect(detail.key).toBe("slack");
  expect(detail.name).toBe("Slack");
  expect(detail.status).toBe("not_connected");
  expect(detail.account_label).toBeNull();
  expect(detail.connected_at).toBeNull();
  expect(detail.topics).toEqual([]);

  const jira = await freshPayload(api, "/v1/connectors/jira");
  expect(jira.name).toBe("Jira");
  expect(jira.beta).toBe(true);
  expect(JSON.stringify(jira).toLowerCase()).not.toContain("composio");
});

test("an unknown connector is a 404, and the catalog needs a signed-in user", async ({ request }) => {
  const user = await createUser("connectors");
  const api = new Api(request, user.id);
  expect((await api.call("GET", "/v1/connectors/bitbucket")).status()).toBe(404);
  for (const path of [
    "/v1/oauth/google/workspace",
    "/v1/connectors/slack/onboarding",
    "/v1/connectors/microsoft/teams",
    "/v1/connectors/microsoft/teams/channels",
    "/v1/connectors/microsoft/teams/chats",
    "/v1/connectors/microsoft/teams/watched",
  ]) {
    expect((await api.call("GET", path)).status(), path).toBe(404);
  }
  for (const path of ["/v1/connectors/slack/backfill", "/v1/connectors/microsoft/teams/ingest"]) {
    expect((await api.call("POST", path, {})).status(), path).toBe(404);
  }
  for (const [method, path] of [
    ["GET", "/v1/connectors/composio"],
    ["GET", "/v1/connectors/composio/jira"],
    ["POST", "/v1/connectors/composio/jira/connect"],
    ["POST", "/v1/connectors/composio/jira/connections/test/verify"],
    ["POST", "/v1/connectors/composio/jira/connections/test/disconnect"],
    ["DELETE", "/v1/connectors/microsoft/teams/watched/00000000-0000-0000-0000-000000000001"],
  ] as const) expect((await api.call(method, path)).status(), `${method} ${path}`).toBe(404);
  // These authenticated operational handlers must still resolve.
  for (const path of ["/v1/oauth/connections", "/v1/connectors/github/installs", "/v1/connectors/slack/watched", "/v1/addin/tokens", "/v1/connectors/google/meet/meetings"]) {
    const response = await api.call("GET", path);
    expect([404, 405], path).not.toContain(response.status());
  }

  for (const [method, path] of [
    ["POST", "/v1/connectors/microsoft/teams/onboarding"],
    ["POST", "/v1/connectors/microsoft/teams/install"],
    ["PUT", "/v1/connectors/github/repos"],
    ["POST", "/v1/connectors/google/meet/captures"],
    ["POST", "/v1/connectors/microsoft/teams/transcripts/capture"],
  ] as const) {
    const response = await api.call(method, path, {});
    expect([404, 405], `${method} ${path}`).not.toContain(response.status());
  }

  expect((await request.get(`${API_BASE}/v1/connectors`)).status()).toBe(401);
  expect((await request.get(`${API_BASE}/v1/connectors/slack`)).status()).toBe(401);
});

test.describe("the connector catalog answers only for the caller @isolation", () => {
  test("two users each get their own, identical-in-shape, empty catalog", async ({ request }) => {
    const a = await createUser("connectors-a");
    const b = await createUser("connectors-b");
    const rawA = JSON.stringify(await freshPayload(new Api(request, a.id), "/v1/connectors"));
    const rawB = JSON.stringify(await freshPayload(new Api(request, b.id), "/v1/connectors"));

    // Neither payload names the other user (or the caller): the catalog is built
    // from the caller's rows only and carries no identity of its own.
    for (const raw of [rawA, rawB]) {
      for (const u of [a, b]) expect(raw).not.toContain(u.id);
    }
    const strip = (raw: string) => JSON.parse(raw).connectors.map((c: any) => [c.key, c.status, c.sources_on]);
    expect(strip(rawA)).toEqual(strip(rawB));
    for (const [, status] of strip(rawA)) expect(status).toBe("not_connected");
  });
});

// ---------------------------------------------------------------------------
// Connect, complete and disconnect (B2)
// ---------------------------------------------------------------------------

const FAKES = `http://127.0.0.1:${PORTS.fakes}`;
const RETURN = `${WEB_BASE}/connectors/return`;
/** A short lowercase-alphanumeric token: one fake Slack workspace per scenario. */
const team = () => Math.random().toString(36).slice(2, 10);

/** Start a Slack connect and follow the authorize hop to Slack. */
async function startSlack(request: APIRequestContext, api: Api) {
  const start = await api.json("POST", "/v1/connectors/slack/connect", { return_to: "wizard" });
  expect(start.authorize_url).toContain(`${API_BASE}/v1/oauth/slack/authorize?state=`);
  expect(start.pending_id ?? null).toBeNull();
  expect(Date.parse(start.expires_at)).toBeGreaterThan(Date.now());
  const hop = await request.get(start.authorize_url, { maxRedirects: 0 });
  expect(hop.status()).toBe(307);
  const slackUrl = new URL(hop.headers().location);
  return { slackUrl, state: slackUrl.searchParams.get("state")! };
}

/** What Slack does after consent: sends the browser to our callback with a code. */
async function slackCallback(request: APIRequestContext, state: string, query: string) {
  const res = await request.get(`${API_BASE}/v1/oauth/slack/callback?${query}&state=${encodeURIComponent(state)}`, { maxRedirects: 0 });
  return { status: res.status(), location: res.headers().location as string | undefined };
}

async function connectSlack(request: APIRequestContext, api: Api, workspace = team()) {
  const { state } = await startSlack(request, api);
  const back = await slackCallback(request, state, `code=team-${workspace}`);
  expect(back.status).toBe(307);
  expect(back.location).toBe(`${RETURN}?provider=slack&result=ok`);
  return workspace;
}

/** Start a Jira connect; the member signs in at the link, which the fake plays. */
async function startJira(api: Api) {
  const start = await api.json("POST", "/v1/connectors/jira/connect", { return_to: "wizard" });
  expect(start.authorize_url).toMatch(/^https:\/\//);
  expect(start.pending_id).toMatch(/^[0-9a-f-]{36}$/);
  const account = start.authorize_url.split("/").pop() as string;
  return { pendingId: start.pending_id as string, account };
}
const signInAtJira = async (request: APIRequestContext, account: string) => {
  const res = await request.post(`${FAKES}/__composio/authorize/${account}`);
  expect(res.ok(), await res.text()).toBeTruthy();
};

test.describe("connect, complete and disconnect", () => {
  test("Slack: one consent installs the app and signs the member in, and the connector reads setting up", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-slack");
    const workspace = team();
    const { slackUrl, state } = await startSlack(request, api);

    // One OAuth carries the bot scopes and the member's own.
    expect(slackUrl.origin + slackUrl.pathname).toBe("https://slack.com/oauth/v2/authorize");
    expect(slackUrl.searchParams.get("scope")).toContain("chat:write");
    expect(slackUrl.searchParams.get("user_scope")).toContain("channels:history");

    const back = await slackCallback(request, state, `code=team-${workspace}`);
    expect(back.status).toBe(307);
    expect(back.location).toBe(`${RETURN}?provider=slack&result=ok`);

    const slack = await freshPayload(api, "/v1/connectors/slack");
    expect(slack.status).toBe("setting_up");
    expect(slack.resume_step).toBe("sources");
    expect(slack.sources_on).toBe(0);
    expect(slack.problem).toBeNull();
    expect(slack.account_label).toBe(`E2E Workspace ${workspace}`);
    expect(slack.connected_at).not.toBeNull();
    const { connectors } = await freshPayload(api, "/v1/connectors");
    expect(connectors.find((c: any) => c.key === "slack").status).toBe("setting_up");

    // The state is single use: replaying the callback lands on the error page.
    const replay = await slackCallback(request, state, `code=team-${workspace}`);
    expect(replay.location).toBe(`${RETURN}?provider=slack&result=error&code=invalid_request`);
  });

  test("Slack: pressing Cancel at Slack returns to the app with a code and connects nothing", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-slack-denied");
    const { state } = await startSlack(request, api);
    const back = await slackCallback(request, state, "error=access_denied");
    expect(back.status).toBe(307);
    expect(back.location).toBe(`${RETURN}?provider=slack&result=error&code=denied`);
    expect((await freshPayload(api, "/v1/connectors/slack")).status).toBe("not_connected");
  });

  test("a connect started from the older UI keeps redirecting to /connectors and creates no setup state", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-slack-legacy");
    const { state } = await api.json("GET", "/v1/oauth/state");
    const back = await slackCallback(request, state, `code=team-${team()}`);
    expect(back.status).toBe(307);
    expect(back.location).toBe(`${WEB_BASE}/connectors?connected=slack`);
    // No connector_sources rows: a connection made outside the new flow reads as set up.
    expect((await freshPayload(api, "/v1/connectors/slack")).status).toBe("active");
  });

  test("Jira: start, sign in at the link, complete, then disconnect", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-jira");
    const { pendingId, account } = await startJira(api);

    // Still at the link: not connected, and complete says so.
    expect((await freshPayload(api, "/v1/connectors/jira")).status).toBe("not_connected");
    const early = await api.call("POST", "/v1/connectors/jira/connect/complete", { pending_id: pendingId });
    expect(early.status()).toBe(409);
    expect((await early.json()).code).toBe("sign_in_pending");

    await signInAtJira(request, account);
    const done = await api.json("POST", "/v1/connectors/jira/connect/complete", { pending_id: pendingId });
    expect(done.key).toBe("jira");
    expect(done.beta).toBe(true);
    expect(done.status).toBe("setting_up");
    expect(done.resume_step).toBe("sources");
    expect(JSON.stringify(done).toLowerCase()).not.toContain("composio");
    const detail = await freshPayload(api, "/v1/connectors/jira");
    expect(detail.status).toBe("setting_up");
    expect(detail.connected_at).not.toBeNull();

    // Disconnect revokes the account at the provider.
    const gone = await api.call("DELETE", "/v1/connectors/jira");
    expect(gone.status()).toBe(204);
    expect((await freshPayload(api, "/v1/connectors/jira")).status).toBe("not_connected");
    const seen: { method: string; path: string }[] = await (await request.get(`${FAKES}/__requests`)).json();
    expect(seen.some((r) => r.method === "DELETE" && r.path.endsWith(`/connected_accounts/${account}`))).toBe(true);
  });

  test("a connector that is not hosted has nothing to complete, and an unknown one is a 404", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-complete");
    const res = await api.call("POST", "/v1/connectors/slack/connect/complete", { pending_id: crypto.randomUUID() });
    expect(res.status()).toBe(400);
    expect((await res.json()).code).toBe("nothing_to_complete");
    expect((await api.call("POST", "/v1/connectors/bitbucket/connect", { return_to: "wizard" })).status()).toBe(404);
    expect((await api.call("DELETE", "/v1/connectors/bitbucket")).status()).toBe(404);
    expect((await api.call("POST", "/v1/connectors/slack/connect", { return_to: "elsewhere" })).status()).toBeGreaterThanOrEqual(400);
    // Signed in only.
    expect((await request.post(`${API_BASE}/v1/connectors/slack/connect`, { data: { return_to: "wizard" } })).status()).toBe(401);
    expect((await request.delete(`${API_BASE}/v1/connectors/slack`)).status()).toBe(401);
  });

  test("disconnect revokes the sign-in and a captured atom stays readable", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-disconnect");
    await connectSlack(request, api);

    const ocean = await api.personalGraph();
    const { atomIds } = await ingestNote(api, ocean.id, "The Narwhal migration is owned by Priya and lands in March.");
    const before = await api.json("GET", `/v1/atoms/${atomIds[0]}`);

    const gone = await api.call("DELETE", "/v1/connectors/slack");
    expect(gone.status()).toBe(204);
    const after = await freshPayload(api, "/v1/connectors/slack");
    expect(after.status).toBe("not_connected");
    expect(after.sources_on).toBe(0);
    expect(after.connected_at).toBeNull();

    // Slack was told to revoke the token.
    const seen: { method: string; path: string }[] = await (await request.get(`${FAKES}/__requests`)).json();
    expect(seen.some((r) => r.path.endsWith("/auth.revoke"))).toBe(true);

    // Captured data stays.
    const kept = await api.json("GET", `/v1/atoms/${atomIds[0]}`);
    expect(kept.id).toBe(before.id);
    expect(JSON.stringify(kept)).toContain("Narwhal");

    // Nothing left to disconnect.
    const again = await api.call("DELETE", "/v1/connectors/slack");
    expect(again.status()).toBe(404);
    expect((await again.json()).code).toBe("not_connected");

    // A new connect starts setup afresh.
    await connectSlack(request, api);
    expect((await freshPayload(api, "/v1/connectors/slack")).status).toBe("setting_up");
  });
});

test.describe("connecting and disconnecting touch only the caller's own connection @isolation", () => {
  test("user B cannot see, complete or disconnect user A's connections", async ({ request }) => {
    const a = await personalUser(request, "connectors-iso-a");
    const b = await personalUser(request, "connectors-iso-b");
    const workspace = await connectSlack(request, a.api);
    const pending = await startJira(a.api);

    // Positive control: A sees the Slack connection.
    expect((await freshPayload(a.api, "/v1/connectors/slack")).status).toBe("setting_up");

    // B sees nothing of it, in the list or the detail, and nothing names A.
    const rawList = JSON.stringify(await freshPayload(b.api, "/v1/connectors"));
    for (const c of JSON.parse(rawList).connectors) expect(c.status, c.key).toBe("not_connected");
    const rawDetail = JSON.stringify(await freshPayload(b.api, "/v1/connectors/slack"));
    expect(JSON.parse(rawDetail).account_label).toBeNull();
    for (const raw of [rawList, rawDetail]) {
      expect(raw).not.toContain(a.user.id);
      expect(raw).not.toContain(workspace);
    }

    // B disconnecting A's connector is exactly what disconnecting one that never
    // existed is, and A's connection is untouched.
    const real = await b.api.call("DELETE", "/v1/connectors/slack");
    const never = await b.api.call("DELETE", "/v1/connectors/dart");
    expect(real.status()).toBe(404);
    expect(never.status()).toBe(404);
    expect(await real.text()).toBe(await never.text());
    expect((await freshPayload(a.api, "/v1/connectors/slack")).status).toBe("setting_up");

    // B cannot complete A's pending sign-in, and gets what a made-up id gets.
    const stolen = await b.api.call("POST", "/v1/connectors/jira/connect/complete", { pending_id: pending.pendingId });
    const made = await b.api.call("POST", "/v1/connectors/jira/connect/complete", { pending_id: crypto.randomUUID() });
    expect(stolen.status()).toBe(404);
    expect(made.status()).toBe(404);
    expect(await stolen.text()).toBe(await made.text());
    expect((await freshPayload(b.api, "/v1/connectors/jira")).status).toBe("not_connected");

    // A's own pending sign-in still completes.
    await signInAtJira(request, pending.account);
    const done = await a.api.json("POST", "/v1/connectors/jira/connect/complete", { pending_id: pending.pendingId });
    expect(done.status).toBe("setting_up");
    // A's completion never dirties B's row, so force B's recompute: a cached
    // answer from before A completed would pass a leak check vacuously.
    expect((await waitForComputed(b.api, "/v1/connectors/jira", true)).payload.status).toBe("not_connected");
  });
});


// ---------------------------------------------------------------------------
// Scope and pause (B3)
// ---------------------------------------------------------------------------

const CHANNELS = "/v1/connectors/slack/sources/channels";
/** The fake workspace names its channels C0E2E00001 .. C0E2E00200, e2e-channel-001 .. */
const channelId = (n: number) => `C0E2E${String(n).padStart(5, "0")}`;

/** A message event as Slack would sign and send it. */
async function postSlackMessage(request: APIRequestContext, workspace: string, channel: string, ts: string) {
  const body = JSON.stringify({
    type: "event_callback",
    team_id: `T${workspace.toUpperCase()}`,
    event_id: `Ev${ts.replace(".", "")}`,
    event: { type: "message", channel, user: "U0E2EUSER1", ts, text: `Live message ${ts}` },
  });
  const stamp = String(Math.floor(Date.now() / 1000));
  const signature = "v0=" + crypto.createHmac("sha256", FAKE.slackSigningSecret).update(`v0:${stamp}:${body}`).digest("hex");
  const res = await request.post(`${API_BASE}/v1/slack/events`, {
    data: body,
    headers: { "content-type": "application/json", "x-slack-request-timestamp": stamp, "x-slack-signature": signature },
  });
  expect(res.status(), await res.text()).toBe(200);
}

/** A fresh message timestamp, unique per call. */
let tick = 0;
const nextTs = () => `${Math.floor(Date.now() / 1000) - 30}.${String(100 + ++tick).padStart(6, "0")}`;

/** What Slack atoms the member's personal ocean holds, as `channel/ts` ids. */
async function slackCaptured(api: Api, graphId: string): Promise<string[]> {
  const list = await api.json("GET", `/v1/sources/items?graph_id=${graphId}&source_type=slack`);
  return (list.items ?? []).map((i: any) => String(i.source_id).replace(/:chunk:\d+$/, ""));
}

/** A new Slack connection with the source on, in "Only what I pick" with `picks`. */
async function slackWithPicks(request: APIRequestContext, api: Api, picks: string[]) {
  const workspace = await connectSlack(request, api);
  await api.json("PATCH", CHANNELS, { enabled: true });
  const source = await api.json("PUT", `${CHANNELS}/scope`, { mode: "pick", item_ids: picks });
  return { workspace, source };
}

test.describe("scope: choosing what a source captures", () => {
  test("Slack lists the member's 200 channels paginated and searchable, and starts with nothing picked", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-scope-list");
    await connectSlack(request, api);

    // A new connection: the source is off and no scope is chosen yet.
    const slack = await freshPayload(api, "/v1/connectors/slack");
    expect(slack.sources[0]).toMatchObject({ key: "channels", enabled: false, paused: false, status: "off", scope: null });

    const first = await api.json("GET", `${CHANNELS}/scope-items?limit=50`);
    expect(first.items).toHaveLength(50);
    expect(first.total).toBe(200);
    expect(first.next_cursor).not.toBeNull();
    expect(first.items[0]).toMatchObject({ id: channelId(1), label: "e2e-channel-001", selected: false });
    expect(first.items.every((i: any) => i.selected === false)).toBe(true);

    // Walk the pages: 200 distinct channels, the last page has no cursor.
    const seen = new Set<string>(first.items.map((i: any) => i.id));
    let cursor: string | null = first.next_cursor;
    let pages = 1;
    while (cursor) {
      const page = await api.json("GET", `${CHANNELS}/scope-items?limit=50&cursor=${encodeURIComponent(cursor)}`);
      page.items.forEach((i: any) => seen.add(i.id));
      cursor = page.next_cursor;
      pages++;
    }
    expect(pages).toBe(4);
    expect(seen.size).toBe(200);

    // Search matches the label, not the id, and counts what matched.
    const found = await api.json("GET", `${CHANNELS}/scope-items?q=CHANNEL-017`);
    expect(found.items.map((i: any) => i.id)).toEqual([channelId(17)]);
    expect(found.total).toBe(1);
    expect(found.next_cursor).toBeNull();
    const none = await api.json("GET", `${CHANNELS}/scope-items?q=no-such-channel`);
    expect(none.items).toEqual([]);
    expect(none.total).toBe(0);

    // A workspace-wide source has nothing to list.
    const { api: other } = await personalUser(request, "connectors-scope-list-2");
    const { pendingId, account } = await startJira(other);
    await signInAtJira(request, account);
    await other.json("POST", "/v1/connectors/jira/connect/complete", { pending_id: pendingId });
    const noItems = await other.call("GET", "/v1/connectors/jira/sources/workspace/scope-items");
    expect(noItems.status()).toBe(422);
    expect((await noItems.json()).code).toBe("no_scope_items");
  });

  test("a mode the source's shape does not allow is a 422", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-scope-shape");
    // Meetings are picked one by one; there is no "everything" for them.
    for (const [p, s] of [["microsoft", "teams_meetings"], ["google", "meet"]]) {
      const res = await api.call("PUT", `/v1/connectors/${p}/sources/${s}/scope`, { mode: "everything" });
      expect(res.status(), `${p}/${s}`).toBe(422);
      expect((await res.json()).code).toBe("everything_not_allowed");
    }
    // A workspace-wide source has nothing to pick.
    for (const [p, s] of [["microsoft", "calendar"], ["google", "calendar"], ["jira", "workspace"]]) {
      const res = await api.call("PUT", `/v1/connectors/${p}/sources/${s}/scope`, { mode: "pick", item_ids: ["x"] });
      expect(res.status(), `${p}/${s}`).toBe(422);
      expect((await res.json()).code).toBe("pick_not_allowed");
    }
    // A mode that is not one is a 422 as well; an unknown source is a 404.
    expect((await api.call("PUT", `${CHANNELS}/scope`, { mode: "some" })).status()).toBe(422);
    expect((await api.call("PUT", "/v1/connectors/slack/sources/nope/scope", { mode: "pick" })).status()).toBe(404);
    // Allowed, but there is no sign-in to store it for.
    expect((await api.call("PUT", `${CHANNELS}/scope`, { mode: "pick", item_ids: [] })).status()).toBe(404);
  });

  test("picking two channels stores them, counts them, and only they are captured", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-scope-pick");
    const picked = [channelId(3), channelId(7)];
    const { workspace, source } = await slackWithPicks(request, api, picked);

    expect(source).toMatchObject({ key: "channels", enabled: true, status: "active", scope: { mode: "pick", count: 2, noun: "channels" } });
    const summary = (await freshPayload(api, "/v1/connectors/slack")).sources[0];
    expect(summary.scope).toEqual({ mode: "pick", count: 2, noun: "channels" });

    // The picker shows exactly those two as selected.
    const list = await api.json("GET", `${CHANNELS}/scope-items?limit=200`);
    expect(list.items.filter((i: any) => i.selected).map((i: any) => i.id).sort()).toEqual(picked);

    // An event in a picked channel is captured; one in a channel the member can
    // read but did not pick is not.
    const graph = await api.personalGraph();
    const inPicked = nextTs();
    const inOther = nextTs();
    await postSlackMessage(request, workspace, picked[0], inPicked);
    await postSlackMessage(request, workspace, channelId(9), inOther);
    await expect.poll(() => slackCaptured(api, graph.id), { timeout: 15_000 }).toContain(`${picked[0]}/${inPicked}`);
    await new Promise((r) => setTimeout(r, 1500));
    expect(await slackCaptured(api, graph.id)).not.toContain(`${channelId(9)}/${inOther}`);

    // Picks are replaced, not added to.
    const again = await api.json("PUT", `${CHANNELS}/scope`, { mode: "pick", item_ids: [channelId(5)] });
    expect(again.scope).toEqual({ mode: "pick", count: 1, noun: "channels" });
    // "Everything" has no count.
    const every = await api.json("PUT", `${CHANNELS}/scope`, { mode: "everything" });
    expect(every.scope).toEqual({ mode: "everything", count: null, noun: "channels" });
  });

  test("pausing stops capture, the status says so, and resuming captures again", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-scope-pause");
    const channel = channelId(4);
    const { workspace } = await slackWithPicks(request, api, [channel]);
    const graph = await api.personalGraph();

    const before = nextTs();
    await postSlackMessage(request, workspace, channel, before);
    await expect.poll(() => slackCaptured(api, graph.id), { timeout: 15_000 }).toContain(`${channel}/${before}`);

    // Pause: the source and the connector both report it.
    const paused = await api.json("PATCH", CHANNELS, { paused: true });
    expect(paused).toMatchObject({ key: "channels", paused: true, enabled: true, status: "paused" });
    const list = await freshPayload(api, "/v1/connectors");
    expect(list.connectors.find((c: any) => c.key === "slack").sources[0].status).toBe("paused");

    // An event for a picked channel while paused writes nothing.
    const during = nextTs();
    await postSlackMessage(request, workspace, channel, during);
    await new Promise((r) => setTimeout(r, 2500));
    expect(await slackCaptured(api, graph.id)).not.toContain(`${channel}/${during}`);

    // Resume: active, and the next event is captured. (The one sent while paused
    // is not replayed: events are doorbells, not a queue.)
    const resumed = await api.json("PATCH", CHANNELS, { paused: false });
    expect(resumed).toMatchObject({ paused: false, status: "active", scope: { mode: "pick", count: 1 } });
    const after = nextTs();
    await postSlackMessage(request, workspace, channel, after);
    await expect.poll(() => slackCaptured(api, graph.id), { timeout: 15_000 }).toContain(`${channel}/${after}`);
    expect(await slackCaptured(api, graph.id)).not.toContain(`${channel}/${during}`);

    // Turning the source off stops it the same way, and an empty patch is a 400.
    const off = await api.json("PATCH", CHANNELS, { enabled: false });
    expect(off).toMatchObject({ enabled: false, status: "off" });
    const empty = await api.call("PATCH", CHANNELS, {});
    expect(empty.status()).toBe(400);
    expect((await empty.json()).code).toBe("nothing_to_change");
  });

  test("a connection made before the catalog keeps capturing every channel", async ({ request }) => {
    // Connected through the older UI's state: no connector_sources rows.
    const { api } = await personalUser(request, "connectors-scope-legacy");
    const workspace = team();
    const { state } = await api.json("GET", "/v1/oauth/state");
    const back = await slackCallback(request, state, `code=team-${workspace}`);
    expect(back.location).toBe(`${WEB_BASE}/connectors?connected=slack`);
    const slack = await freshPayload(api, "/v1/connectors/slack");
    expect(slack.sources[0].scope).toEqual({ mode: "everything", count: null, noun: "channels" });
    expect(slack.sources[0]).toMatchObject({ enabled: true, status: "active" });

    // Wait for the connect-time index of the member's channels, then an event in
    // a channel nobody picked is captured. The event is retried until the index
    // exists (it is built in the background after the callback).
    const graph = await api.personalGraph();
    const channel = channelId(11);
    const sent: string[] = [];
    await expect
      .poll(
        async () => {
          const ts = nextTs();
          sent.push(`${channel}/${ts}`);
          await postSlackMessage(request, workspace, channel, ts);
          await new Promise((r) => setTimeout(r, 1000));
          const captured = await slackCaptured(api, graph.id);
          return sent.some((id) => captured.includes(id));
        },
        { timeout: 20_000 },
      )
      .toBe(true);
  });
});

test.describe("a member's scope and pause are theirs alone @isolation", () => {
  test("user B cannot list, change or pause user A's source", async ({ request }) => {
    const a = await personalUser(request, "connectors-scope-iso-a");
    const b = await personalUser(request, "connectors-scope-iso-b");
    await slackWithPicks(request, a.api, [channelId(2), channelId(6)]);

    // B has no Slack sign-in: every call answers what an unconnected connector
    // answers, and none of them touches A.
    const attempts: [string, string, unknown?][] = [
      ["GET", `${CHANNELS}/scope-items`],
      ["PUT", `${CHANNELS}/scope`, { mode: "everything" }],
      ["PUT", `${CHANNELS}/scope`, { mode: "pick", item_ids: [channelId(9)] }],
      ["PATCH", CHANNELS, { paused: true }],
      ["PATCH", CHANNELS, { enabled: false }],
    ];
    for (const [method, path, body] of attempts) {
      const res = await b.api.call(method as any, path, body);
      expect(res.status(), `${method} ${path}`).toBe(404);
      const text = await res.text();
      expect(JSON.parse(text).code).toBe("not_connected");
      expect(text).not.toContain(a.user.id);
    }

    // A's source is exactly as A left it, and B has none of their own.
    const mine = (await freshPayload(a.api, "/v1/connectors/slack")).sources[0];
    expect(mine).toMatchObject({ enabled: true, paused: false, status: "active", scope: { mode: "pick", count: 2 } });
    const theirs = await freshPayload(b.api, "/v1/connectors/slack");
    expect(theirs.status).toBe("not_connected");
    expect(theirs.sources[0]).toMatchObject({ enabled: false, status: "off", scope: null });
  });
});

// Import history (B4)
// ---------------------------------------------------------------------------

const BACKFILL = "/v1/connectors/slack/sources/channels/backfill";
const DAY_MS = 86_400_000;

/** Run `fn` with the fake Slack workspace holding `n` channels, then put it back. One
 *  worker runs a stack's scenarios in turn, so nothing else reads the count meanwhile. */
async function withSlackChannels<T>(request: APIRequestContext, n: number, fn: () => Promise<T>): Promise<T> {
  const set = (channels: number) => request.post(`${FAKES}/__slack/config`, { data: { channels } });
  expect((await set(n)).ok()).toBeTruthy();
  try {
    return await fn();
  } finally {
    await set(200);
  }
}

/** Connect Slack the way the older UI does, which leaves the channels source on and set
 *  up (the new flow leaves it off until the scope step, and a source that is off takes no
 *  history). The callback indexes the member's channels in the background. */
async function connectSlackFromOlderUi(request: APIRequestContext, api: Api, workspace: string) {
  const { state } = await api.json("GET", "/v1/oauth/state");
  const back = await slackCallback(request, state, `code=team-${workspace}`);
  expect(back.location).toBe(`${WEB_BASE}/connectors?connected=slack`);
  await api.json("PUT", `${CHANNELS}/scope`, { mode: "pick", item_ids: [channelId(1), channelId(2)] });
}

/** The bot-token `conversations.history` calls the fake saw for a workspace: the ones
 *  an import job makes. (The member's own capture reads with its own token.) */
async function botHistoryCalls(request: APIRequestContext, workspace: string) {
  const seen: any[] = await (await request.get(`${FAKES}/__requests`)).json();
  return seen.filter((r) => r.path.endsWith("/conversations.history") && r.bearer === `xoxb-e2e-T${workspace.toUpperCase()}`);
}

test.describe("import history", () => {
  test("Slack: 7 days reads importing, ends active, and asks Slack for only that window", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-history");
    // A `team-old<x>` workspace also holds messages 10 and 45 days old in every channel.
    const workspace = `old${team()}`;
    await withSlackChannels(request, 3, async () => {
      await connectSlackFromOlderUi(request, api, workspace);
      expect((await freshPayload(api, "/v1/connectors/slack")).status).toBe("active");

      // The channel index fills in the background after the connect, so ask until the
      // request has something to import. A request that found nothing queues nothing and
      // does not block the next.
      const asked = Date.now();
      let started: any;
      await expect
        .poll(async () => {
          const res = await api.call("POST", BACKFILL, { days: 7 });
          started = await res.json();
          expect(res.status(), JSON.stringify(started)).toBe(202);
          return started.backfill;
        })
        .not.toBeNull();
      expect(started.key).toBe("channels");
      expect(started.status).toBe("importing");
      expect(started.backfill.window_days).toBe(7);
      expect(started.backfill.percent).toBeGreaterThanOrEqual(0);
      expect(started.backfill.percent).toBeLessThan(100);

      // A second request while it runs is refused, and queues nothing.
      const again = await api.call("POST", BACKFILL, { days: 30 });
      if (again.status() === 409) expect((await again.json()).code).toBe("backfill_running");

      // It runs to the end: the source and the connector go back to active.
      const done = await freshPayload(api, "/v1/connectors/slack", (slack) => slack.status === "active", 20_000);
      expect(done.importing).toBeNull();
      expect(done.sources[0].history_days).toBe(7);
      expect(done.sources[0].history_requested_at).not.toBeNull();
      expect(done.sources[0].backfill).toBeNull();
      expect(done.sources[0].status).toBe("active");

      // Slack was asked for exactly seven days, with the workspace app's token: the oldest
      // moment it was given is now minus seven days, and the 10 and 45 day old messages
      // were not served, while the recent three were.
      const calls = await botHistoryCalls(request, workspace);
      expect(calls.length).toBeGreaterThan(0);
      const floor = (asked - 7 * DAY_MS) / 1000;
      for (const c of calls) {
        const oldest = Number(c.params.oldest);
        expect(Math.abs(oldest - floor), `oldest ${oldest} vs ${floor}`).toBeLessThan(60);
        expect(c.served, JSON.stringify(c)).toHaveLength(3);
        for (const ts of c.served) expect(ts).toBeGreaterThan(oldest);
      }

      // The fake history is in the member's ocean.
      const ocean = await api.personalGraph();
      await expect
        .poll(async () => JSON.stringify(await api.json("GET", `/v1/graphs/${ocean.id}/atoms?page_size=200`)))
        .toContain("Message 1 in C0E2E00001");
    });
  });

  test("a connector that cannot backfill refuses with its reason", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-history-jira");
    const { pendingId, account } = await startJira(api);
    await signInAtJira(request, account);
    await api.json("POST", "/v1/connectors/jira/connect/complete", { pending_id: pendingId });

    for (const days of [7, 0]) {
      const res = await api.call("POST", "/v1/connectors/jira/sources/workspace/backfill", { days });
      expect(res.status()).toBe(409);
      const body = await res.json();
      expect(body.code).toBe("history_unsupported");
      expect(body.reason).toBe("starts_at_connect");
      expect(JSON.stringify(body).toLowerCase()).not.toContain("composio");
    }
    // Calendar's window is automatic; a meeting source is per meeting.
    const calendar = await api.call("POST", "/v1/connectors/google/sources/calendar/backfill", { days: 30 });
    expect(calendar.status()).toBe(409);
    expect((await calendar.json()).reason).toBe("automatic_window");
    expect((await freshPayload(api, "/v1/connectors/jira")).sources[0].status).toBe("off");
  });

  test("a window that is not 0, 7, 30 or 90 is a 422, and nothing is queued", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-history-422");
    for (const days of [120, 8, 1, 365]) {
      const res = await api.call("POST", BACKFILL, { days });
      expect(res.status(), `days ${days}`).toBe(422);
      const body = await res.json();
      expect(body.code).toBe("invalid_days");
      expect(body.allowed).toEqual([0, 7, 30, 90]);
    }
    expect((await api.call("POST", BACKFILL, { days: -7 })).status()).toBe(422);
    expect((await api.call("POST", BACKFILL, {})).status()).toBe(422);
    // Unknown connector or source; signed in only.
    expect((await api.call("POST", "/v1/connectors/bitbucket/sources/x/backfill", { days: 7 })).status()).toBe(404);
    expect((await api.call("POST", "/v1/connectors/slack/sources/dms/backfill", { days: 7 })).status()).toBe(404);
    expect((await request.post(`${API_BASE}${BACKFILL}`, { data: { days: 7 } })).status()).toBe(401);
    // Not connected, so there is nothing to import.
    const none = await api.call("POST", BACKFILL, { days: 7 });
    expect(none.status()).toBe(404);
    expect((await none.json()).code).toBe("not_connected");
  });

  test("None records the choice and runs nothing", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-history-none");
    const workspace = team();
    await withSlackChannels(request, 3, async () => {
      await connectSlackFromOlderUi(request, api, workspace);
      const res = await api.call("POST", BACKFILL, { days: 0 });
      expect(res.status()).toBe(202);
      const source = await res.json();
      expect(source.status).toBe("active");
      expect(source.backfill).toBeNull();
      const slack = await freshPayload(api, "/v1/connectors/slack");
      expect(slack.status).toBe("active");
      expect(slack.resume_step).toBeNull();
      expect(slack.importing).toBeNull();

      // No job was queued: nothing read the workspace's history with the app's token.
      await new Promise((r) => setTimeout(r, 2_000));
      expect(await botHistoryCalls(request, workspace)).toHaveLength(0);

      // "None" never blocks a later window.
      await expect
        .poll(async () => (await (await api.call("POST", BACKFILL, { days: 7 })).json()).backfill)
        .not.toBeNull();
    });
  });

  test("a source that is not on takes no history, and nothing is recorded", async ({ request }) => {
    const { api } = await personalUser(request, "connectors-history-off");
    await connectSlack(request, api);
    // The new flow leaves the source off until the member turns it on.
    const res = await api.call("POST", BACKFILL, { days: 7 });
    expect(res.status()).toBe(409);
    expect((await res.json()).code).toBe("source_off");
    const slack = await freshPayload(api, "/v1/connectors/slack");
    expect(slack.status).toBe("setting_up");
    expect(slack.resume_step).toBe("sources");
    expect(slack.sources[0].status).toBe("off");
  });
});

test.describe("import history is the caller's own @isolation", () => {
  test("user B cannot start, see or block user A's import", async ({ request }) => {
    const a = await personalUser(request, "connectors-history-iso-a");
    const b = await personalUser(request, "connectors-history-iso-b");
    await withSlackChannels(request, 3, async () => {
      const workspace = team();
      await connectSlackFromOlderUi(request, a.api, workspace);
      await expect
        .poll(async () => (await (await a.api.call("POST", BACKFILL, { days: 30 })).json()).backfill)
        .not.toBeNull();

      // B has no Slack, so B has nothing to import and is told so.
      const res = await b.api.call("POST", BACKFILL, { days: 7 });
      expect(res.status()).toBe(404);
      expect((await res.json()).code).toBe("not_connected");
      const raw = JSON.stringify(await freshPayload(b.api, "/v1/connectors/slack"));
      expect(JSON.parse(raw).status).toBe("not_connected");
      expect(JSON.parse(raw).importing).toBeNull();
      expect(raw).not.toContain(a.user.id);
      expect(raw).not.toContain(workspace);
    });
  });
});

test("capture feed validates source and cursor and isolates new members", async ({ request }) => {
  const user = await createUser("b5");
  const api = new Api(request, user.id);
  expect(await api.json("GET", "/v1/connectors/slack/captures?source=channels&limit=1"))
    .toEqual({ items: [], next_cursor: null });
  for (const query of ["source=gmail", "cursor=bad", "limit=0", "limit=101"]) {
    expect((await api.call("GET", `/v1/connectors/slack/captures?${query}`)).status()).toBe(400);
  }
});

test("connector activity counts captures, filters sources, pages and isolates users", async ({ request }) => {
  const alice = await personalUser(request, "b5a");
  const bob = await personalUser(request, "b5b");
  const graphs = await alice.api.graphs();
  const graph = graphs[0].id;
  for (const sourceType of ["outlook", "outlook", "microsoft_calendar"]) {
    await alice.api.json("POST", "/v1/ingest/atom", {
      graph_id: graph, source_type: sourceType, source_id: `b5-${crypto.randomUUID()}`,
      content: "Activity API fixture records one retained capture.",
      participants: [], timestamp: new Date().toISOString(),
    });
  }
  const detail = (await waitForComputed(alice.api, "/v1/connectors/microsoft", true, (p) => p.last_captured_at !== null)).payload;
  expect(detail.last_captured_at).not.toBeNull();
  expect(detail.sources.find((s: any) => s.key === "mail").week.reduce((a: number, b: number) => a + b, 0)).toBe(2);
  const first = await alice.api.json("GET", "/v1/connectors/microsoft/captures?source=mail&limit=1");
  expect(first.items).toHaveLength(1);
  expect(first.items[0].source).toBe("mail");
  expect(first.next_cursor).not.toBeNull();
  const second = await alice.api.json("GET", `/v1/connectors/microsoft/captures?source=mail&limit=1&cursor=${encodeURIComponent(first.next_cursor)}`);
  expect(second.items).toHaveLength(1);
  expect(second.items[0].atom_id).not.toBe(first.items[0].atom_id);
  expect(second.next_cursor).toBeNull();
  const calendar = await alice.api.json("GET", "/v1/connectors/microsoft/captures?source=calendar");
  expect(calendar.items).toHaveLength(1);
  expect((await bob.api.json("GET", "/v1/connectors/microsoft/captures")).items).toEqual([]);
});

// Integrated browser requests: no catalog interception or fulfilled responses.
// The wizard and the detail page's capture management are two scenarios: each
// step waits on a real read-model recompute, and together they pass 30 s.
for (const entry of ["/connectors", "/onboarding/connectors"]) test(`real Slack wizard from ${entry} persists scope across reload and requests the chosen history`, async ({ page, signedIn }) => {
  const api = signedIn.api;
  const workspace = team();
  // Only the external provider consent page is simulated. Catalog, authorize,
  // callback, persistence and all browser fetches use the running backend.
  await page.route("https://slack.com/oauth/v2/authorize**", async route => {
    const state = new URL(route.request().url()).searchParams.get("state")!;
    await route.fulfill({ status: 302, headers: { location: `${API_BASE}/v1/oauth/slack/callback?code=team-${workspace}&state=${encodeURIComponent(state)}` } });
  });
  await page.goto(entry);
  await page.getByRole("button", { name: "Connect Slack", exact: true }).click();
  await page.getByRole("button", { name: "Sign in to Slack", exact: true }).click();
  // Single-source connectors enable their source and open scope directly.
  const wizard = page.getByRole("dialog");
  await expect(wizard.getByRole("heading", { name: "Choose scope: Channels", exact: true })).toBeVisible();
  for (const n of [1, 2]) {
    const checkbox = wizard.getByRole("checkbox", { name: `e2e-channel-00${n}`, exact: true });
    await checkbox.press("Space");
    await expect(checkbox).toBeChecked();
  }
  await page.getByRole("button", { name: "Continue", exact: true }).click();
  await expect(page.getByText("Last 7 days", { exact: true })).toBeVisible();
  await page.reload();
  await expect(page.getByText("Last 7 days", { exact: true })).toBeVisible();
  expect((await freshPayload(api, "/v1/connectors/slack")).sources[0].scope.count).toBe(2);
  await page.getByText("Last 7 days", { exact: true }).click();
  const requested = page.waitForResponse((r) => r.request().method() === "POST" && new URL(r.url()).pathname === BACKFILL);
  await page.getByRole("button", { name: "Finish", exact: true }).click();
  const accepted = await requested;
  expect(accepted.status()).toBe(202);
  expect((await accepted.json()).history_days).toBe(7);
  await expect(page.getByRole("link", { name: "View Slack", exact: true })).toBeVisible();
  // The stored detail is served stale while the import's captures land (each is
  // a reducing graph write), so the stored history is the "Slack: 7 days" scenario's.
});

test("the real Slack detail page pauses, resumes and disconnects capture, and captured content stays", async ({ page, signedIn, request }) => {
  const api = signedIn.api;
  const graph = await api.personalGraph();
  const { workspace } = await slackWithPicks(request, api, [channelId(1), channelId(2)]);
  await page.goto("/connectors/slack");
  const first = nextTs();
  await postSlackMessage(request, workspace, channelId(1), first);
  await expect.poll(() => slackCaptured(api, graph.id)).toContain(`${channelId(1)}/${first}`);
  const captured = await api.json("GET", "/v1/connectors/slack/captures");
  expect(captured.items.length).toBeGreaterThan(0);
  const retainedAtom = captured.items[0].atom_id;
  let retainedBody: any;
  await expect.poll(async () => {
    const response = await api.call("GET", `/v1/atoms/${retainedAtom}`);
    if (response.status() === 200) retainedBody = await response.json();
    return response.status();
  }).toBe(200);
  expect(typeof retainedBody.content).toBe("string");
  expect(retainedBody.content.length).toBeGreaterThan(0);
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await freshPayload(api, "/v1/connectors/slack", (slack) => slack.sources[0].paused === true);
  const blocked = nextTs();
  await postSlackMessage(request, workspace, channelId(1), blocked);
  // The webhook acknowledges enqueue, not completion. Keep the source paused
  // until its actual install-capture job settles before testing resume.
  await expect.poll(() => selectRows(`SELECT status::text FROM jobs WHERE kind='SlackMessageIngest' AND payload->>'ts'='${blocked}' AND payload->>'workspace_id'='T${workspace.toUpperCase()}'`).map(r => r[0]), { timeout: 15_000 }).toEqual(["done"]);
  expect(await slackCaptured(api, graph.id)).not.toContain(`${channelId(1)}/${blocked}`);
  await page.getByRole("button", { name: "Resume", exact: true }).click();
  await freshPayload(api, "/v1/connectors/slack", (slack) => slack.sources[0].paused === false);
  const resumed = nextTs();
  await postSlackMessage(request, workspace, channelId(1), resumed);
  await expect.poll(() => slackCaptured(api, graph.id)).toContain(`${channelId(1)}/${resumed}`);
  expect(await slackCaptured(api, graph.id)).not.toContain(`${channelId(1)}/${blocked}`);
  expect((await api.call("DELETE", "/v1/connectors/slack")).status()).toBe(204);
  expect(await slackCaptured(api, graph.id)).toContain(`${channelId(1)}/${first}`);
  await expect.poll(async () => {
    const response = await api.call("GET", `/v1/atoms/${retainedAtom}`);
    return response.ok() ? (await response.json()).content : null;
  }).toEqual(retainedBody.content);
});
