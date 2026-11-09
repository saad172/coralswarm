import { expect, test } from "../lib/fixtures";
import { googlePushToken } from "../lib/api";
import { selectRows } from "../lib/stack-db";
import { API_BASE, FAKE, GOOGLE_PUSH_AUDIENCE } from "../stack/config.mjs";

// ADR-0016's receive stage: the Pub/Sub push and Graph notification webhooks
// verify, write the delivery as one durable `IngestReceive` job and answer at
// once. No fake serves Gmail history or Graph messages, so the worker's fetch
// and `submit` are covered by the backend's Postgres suites. This pins the
// public contract against the real stack, including that the serving role can
// write the receive job: a refused write answers 503.

const pushBody = (subscription: string, data: unknown, messageId: string) => ({
  subscription: `projects/coralswarm-e2e/subscriptions/${subscription}`,
  message: { data: Buffer.from(JSON.stringify(data)).toString("base64"), messageId, publishTime: new Date().toISOString() },
});

test("a Pub/Sub push is refused unverified and held when verified", async ({ request }) => {
  const url = `${API_BASE}/v1/webhooks/google`;
  const now = Math.floor(Date.now() / 1000);
  const claims = {
    iss: "https://accounts.google.com",
    aud: GOOGLE_PUSH_AUDIENCE,
    email: FAKE.googlePushServiceAccount,
    email_verified: true,
    iat: now,
    exp: now + 600,
  };
  const gmail = pushBody("gmail-push-sub", { emailAddress: `nobody-${now}@example.com`, historyId: "1" }, `gmail-${now}`);

  expect((await request.post(url, { data: gmail })).status(), "no token").toBe(401);
  const foreign = await googlePushToken(request, { ...claims, aud: "another-app" });
  expect((await request.post(url, { data: gmail, headers: { authorization: `Bearer ${foreign}` } })).status(), "another audience").toBe(403);

  const auth = { authorization: `Bearer ${await googlePushToken(request, claims)}` };
  expect((await request.post(url, { data: gmail, headers: auth })).status(), "gmail push").toBe(200);
  const meet = pushBody("meet-events-sub", { transcript: { name: "conferenceRecords/r1/transcripts/t1" } }, `meet-${now}`);
  expect((await request.post(url, { data: meet, headers: auth })).status(), "workspace event").toBe(200);
  const garbage = await request.post(url, { data: "not json", headers: { ...auth, "content-type": "application/json" } });
  expect(garbage.status(), "a body that never parses is acked").toBe(200);
});

// The verified-enqueue, worker and settle path end to end, on the Gmail push:
// the stack has a fake Google signing key, so a push verifies and is held as
// one `IngestReceive` job, the worker claims it, and it settles with the
// mailbox address scrubbed from the row. Nobody watches the mailbox, so the
// pass is a permanent refusal and the job completes without a retry; a
// watched mailbox's fetch and `submit` are the backend's Postgres suites.
// Graph cannot be covered this way: a Graph subscription only exists after a
// Microsoft OAuth connection and a subscription created against
// graph.microsoft.com (`connectors::microsoft::GRAPH` is not configurable and
// the stack has no Microsoft fake), and no supported route seeds one.
test("a verified Gmail push is held as a receive job and settled with its mailbox scrubbed", async ({ request }) => {
  const url = `${API_BASE}/v1/webhooks/google`;
  const now = Math.floor(Date.now() / 1000);
  const claims = {
    iss: "https://accounts.google.com",
    aud: GOOGLE_PUSH_AUDIENCE,
    email: FAKE.googlePushServiceAccount,
    email_verified: true,
    iat: now,
    exp: now + 600,
  };
  const auth = { authorization: `Bearer ${await googlePushToken(request, claims)}` };
  const mailbox = `held-${now}-${process.pid}@example.com`;
  const [[mark]] = selectRows("select clock_timestamp()");
  const held = () =>
    selectRows(
      `select status::text, coalesce(account_id::text, ''), payload::text, coalesce(error_message, '') from jobs
        where kind = 'IngestReceive' and payload->>'provider' = 'gmail_push' and created_at > '${mark}'
        order by created_at`,
    );

  const push = pushBody("gmail-push-sub", { emailAddress: mailbox, historyId: "7" }, `gmail-held-${now}`);
  expect((await request.post(url, { data: push, headers: auth })).status(), "verified push").toBe(200);
  expect(held().length, "one receive job is written for the push").toBe(1);

  await expect.poll(() => held()[0]?.[0], { timeout: 15_000, intervals: [250, 500, 1_000] }).toBe("done");
  const [[status, accountId, payload, error]] = held();
  expect(status).toBe("done");
  expect(accountId, "a receive job belongs to no account until a worker resolves it").toBe("");
  expect(error, "a mailbox nobody watches is a permanent refusal, not a failure").toBe("");
  const settled = JSON.parse(payload);
  expect(settled.provider).toBe("gmail_push");
  expect(settled.email_address, "the mailbox address does not outlive the job").toBe("[redacted on completion]");
  expect(payload).not.toContain(mailbox);
});

test("a Graph subscription is validated and its notifications are held", async ({ request }) => {
  const url = `${API_BASE}/v1/webhooks/graph`;
  const echo = await request.post(`${url}?validationToken=${encodeURIComponent("e2e token &1")}`);
  expect(echo.status()).toBe(200);
  expect(echo.headers()["content-type"]).toContain("text/plain");
  expect(await echo.text()).toBe("e2e token &1");

  const batch = {
    value: [{
      subscriptionId: `e2e-${Date.now()}`,
      clientState: "e2e-client-state",
      changeType: "created",
      resource: "me/mailFolders('inbox')/messages/m-1",
      resourceData: { id: "m-1" },
    }],
  };
  expect((await request.post(url, { data: batch })).status(), "notification batch").toBe(202);
  // A batch is verified per unique subscription, not per entry: many
  // entries across a few subscriptions, including one that names no
  // resource, are answered within Graph's budget.
  const many = Array.from({ length: 40 }, (_, i) => ({
    subscriptionId: `e2e-batch-${Date.now()}-${i % 3}`,
    clientState: "e2e-client-state",
    changeType: "created",
    resource: i % 7 === 0 ? undefined : `me/mailFolders('inbox')/messages/m-${i}`,
    resourceData: i % 7 === 0 ? undefined : { id: `m-${i}` },
  }));
  const started = Date.now();
  expect((await request.post(url, { data: { value: many } })).status(), "a 40-entry batch").toBe(202);
  expect(Date.now() - started, "answered well inside Graph's 3 s budget").toBeLessThan(3000);
  const garbage = await request.post(url, { data: "not json", headers: { "content-type": "application/json" } });
  expect(garbage.status(), "a body that never parses is acked").toBe(202);
});

test("Slack events have one Request URL; the legacy webhook is gone", async ({ request }) => {
  const challenge = { type: "url_verification", challenge: "e2e-challenge" };
  expect((await request.post(`${API_BASE}/v1/webhooks/slack`, { data: challenge })).status()).toBe(404);
  const events = await request.post(`${API_BASE}/v1/slack/events`, { data: challenge });
  expect(events.status()).toBe(200);
  expect((await events.json()).challenge).toBe("e2e-challenge");
});
