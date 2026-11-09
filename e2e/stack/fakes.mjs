// Deterministic stand-ins for the paid providers the backend calls, in one
// dependency-free HTTP server. Started by `stack/up.mjs`.
//
//   /openrouter/v1/embeddings        OpenAI embeddings shape. A 384-wide
//                                    hashed bag of words, L2-normalised, so
//                                    texts sharing words are near each other.
//   /openrouter/v1/chat/completions  OpenAI chat-completions shape, blocking
//                                    or SSE. Ask synthesis gets an answer that
//                                    quotes and cites [n] the first numbered
//                                    passage it was given; source extraction
//                                    gets one quoted fact per sentence;
//                                    anything else gets a fixed short reply.
//   /openrouter/v1/generation        Generation cost lookup.
//   /stripe/v1/*                     The handful of Stripe objects the trial
//                                    Checkout path creates and re-reads.
//   /slack/api/{method}              The Slack Web API calls a connected
//                                    workspace makes: the OAuth code exchange
//                                    (the code names the workspace), and the
//                                    user, channel and workspace lookups
//                                    message ingest reads. Anything else is
//                                    `{ok: true}`.
//   /google/oauth2/v3/certs          Google's OIDC signing keys, as the
//                                    Pub/Sub push route fetches them.
//   /__google/push-token             Mints the bearer token Pub/Sub would
//                                    send with a push, for the claims posted.
//   /slack/api/*                     Slack Web API (SLACK_API_BASE): oauth.v2.access,
//                                    auth.test, auth.revoke, users.conversations
//                                    (paginated), conversations.history (honours
//                                    `oldest`; a `team-old<x>` workspace also holds
//                                    10 and 45 day old messages, and the request log
//                                    records the bearer, `oldest` and the ts served),
//                                    plus conversations.info/members
//                                    and users.info. See the Slack section below.
//   /composio/api/v3.1/*             Composio REST (COMPOSIO_API_BASE): auth config
//                                    lookup, connected_accounts/link, connected
//                                    account status, tools/execute for Jira.
//   /__slack/config, /__composio/*   Test controls for the two fakes above.
//   /__requests                      Every request seen, for debugging.
//   /__s3/outage/{ocean}             POST starts an R2 outage for one ocean,
//                                    DELETE ends it. The backend reaches S3Mock
//                                    through a proxy on E2E_S3_PROXY_PORT; while
//                                    an ocean is out, every object PUT under it
//                                    answers 503 SlowDown, as R2 does when it
//                                    is down. Everything else passes through.
//
// No randomness: the same request always gets the same response, except
// that the Google signing key is generated per boot.

import http from "node:http";
import fs from "node:fs";
import { githubFake } from "./github-fake.mjs";
import { microsoftRoute } from "./microsoft-fake.mjs";
import crypto from "node:crypto";
import { googleNative } from "./google-native.mjs";
const handleGoogleNative = googleNative();

const PORT = Number(process.env.E2E_FAKES_PORT ?? 18090);
const S3_PORT = Number(process.env.E2E_S3_PORT ?? 19090);
const S3_PROXY_PORT = Number(process.env.E2E_S3_PROXY_PORT ?? 19091);
// Must match `FAKE.slackClientId/Secret` in stack/config.mjs.
const FAKE_SLACK = { clientId: "e2e-slack-client-id", clientSecret: "e2e-slack-client-secret" };
const DIM = 384;
const seen = [];

function json(res, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { "content-type": "application/json", "content-length": Buffer.byteLength(payload) });
  res.end(payload);
}

function hash32(s) {
  return crypto.createHash("sha256").update(s).digest().readUInt32BE(0);
}

export function embed(text) {
  const v = new Array(DIM).fill(0);
  v[0] = 0.05; // never the zero vector (the boot probe refuses one)
  for (const word of String(text).toLowerCase().match(/[\p{L}\p{N}]+/gu) ?? []) {
    const h = hash32(word);
    v[1 + (h % (DIM - 1))] += h & 0x100 ? 1 : -1;
  }
  const norm = Math.sqrt(v.reduce((a, x) => a + x * x, 0));
  return v.map((x) => x / norm);
}

function messageText(content) {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) return content.map((p) => p.text ?? "").join("\n");
  return "";
}

// Ask synthesis sends one user message holding a JSON prompt whose passages
// carry a number `n` (see backend/src/scoped_ask.rs).
export function synthesize(body) {
  const user = [...(body.messages ?? [])].reverse().find((m) => m.role === "user");
  let prompt;
  try {
    prompt = JSON.parse(messageText(user?.content));
  } catch {
    return null;
  }
  if (!prompt || typeof prompt !== "object" || !("question" in prompt)) return null;
  const cited = [];
  for (const outcome of prompt.outcomes ?? []) {
    for (const c of outcome.citations ?? []) {
      if (typeof c.n === "number") cited.push({ n: c.n, text: outcome.statement ?? c.quote ?? "" });
    }
  }
  for (const chunk of prompt.legacy_evidence ?? []) {
    if (typeof chunk.n === "number") cited.push({ n: chunk.n, text: chunk.content ?? chunk.text ?? "" });
  }
  if (cited.length === 0) return "The selected sources do not contain an answer to that question.";
  const first = cited[0];
  const quote = String(first.text).trim().replace(/\s+/g, " ").slice(0, 300);
  return `From the retained evidence: ${quote} [${first.n}]`;
}

// Source extraction (`emit_source_atoms_v2`/`_v3`, a forced tool call): one
// `fact` per sentence of the window, each quoting that sentence verbatim, so
// the backend's own validator anchors every quote.
export function extract(body) {
  const tool = body.tools?.[0]?.function?.name ?? "";
  if (!tool.startsWith("emit_source_atoms")) return null;
  const user = [...(body.messages ?? [])].reverse().find((m) => m.role === "user");
  const text = messageText(user?.content);
  const marker = "Source text:\n";
  const at = text.indexOf(marker);
  const window = at >= 0 ? text.slice(at + marker.length) : text;
  const sentences = window.match(/[^.!?\n]+[.!?]?/g)?.map((x) => x.trim()).filter((x) => x.length > 3) ?? [];
  const v3 = tool.endsWith("_v3");
  const atoms = sentences.slice(0, 20).map((sentence) => ({
    kind: "fact",
    text: sentence,
    evidence_quote: sentence,
    entities: [],
    relations: [],
    valid_from: null,
    valid_until: null,
    validity_quote: null,
    ...(v3 ? { changes: [] } : {}),
  }));
  return { tool, args: JSON.stringify({ atoms }) };
}

function completion(body) {
  const content = synthesize(body) ?? "OK.";
  const id = "gen-e2e-" + crypto.createHash("sha256").update(JSON.stringify(body.messages ?? [])).digest("hex").slice(0, 16);
  const usage = { prompt_tokens: 100, completion_tokens: 20, total_tokens: 120, cost: 0 };
  return { id, content, usage, model: body.model ?? "e2e-fake" };
}

function sendCompletion(res, body) {
  const x = extract(body);
  if (x) {
    const id = "gen-e2e-x-" + crypto.createHash("sha256").update(x.args).digest("hex").slice(0, 16);
    const call = { id: "call_" + id.slice(-12), type: "function", function: { name: x.tool, arguments: x.args } };
    const usage = { prompt_tokens: 200, completion_tokens: 50, total_tokens: 250, cost: 0 };
    if (body.stream) {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.write(`data: ${JSON.stringify({ id, object: "chat.completion.chunk", model: body.model, choices: [{ index: 0, delta: { role: "assistant", tool_calls: [{ index: 0, ...call }] }, finish_reason: null }] })}\n\n`);
      res.write(`data: ${JSON.stringify({ id, object: "chat.completion.chunk", model: body.model, choices: [{ index: 0, delta: {}, finish_reason: "tool_calls" }], usage })}\n\n`);
      return res.end("data: [DONE]\n\n");
    }
    return json(res, 200, {
      id, object: "chat.completion", created: 1767225600, model: body.model,
      choices: [{ index: 0, message: { role: "assistant", content: null, tool_calls: [call] }, finish_reason: "tool_calls" }],
      usage,
    });
  }
  const c = completion(body);
  if (body.stream) {
    res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    const chunk = (delta, finish) =>
      `data: ${JSON.stringify({ id: c.id, object: "chat.completion.chunk", model: c.model, choices: [{ index: 0, delta, finish_reason: finish }] })}\n\n`;
    res.write(chunk({ role: "assistant", content: c.content }, null));
    res.write(`data: ${JSON.stringify({ id: c.id, object: "chat.completion.chunk", model: c.model, choices: [{ index: 0, delta: {}, finish_reason: "stop" }], usage: c.usage })}\n\n`);
    res.end("data: [DONE]\n\n");
    return;
  }
  json(res, 200, {
    id: c.id,
    object: "chat.completion",
    created: 1767225600,
    model: c.model,
    choices: [{ index: 0, message: { role: "assistant", content: c.content }, finish_reason: "stop" }],
    usage: c.usage,
  });
}

// ── Stripe ──────────────────────────────────────────────────────────────────
// State lives in memory for the life of the stack: ids are sequential, so a
// fresh stack always hands out the same ids in the same order.
const stripe = { seq: 0, customers: new Map(), sessions: new Map(), subscriptions: new Map(), paymentMethods: new Map() };
const nextId = (prefix) => `${prefix}_e2e${String(++stripe.seq).padStart(6, "0")}`;
const now = () => Math.floor(Date.now() / 1000);

function form(raw) {
  const out = {};
  for (const [k, v] of new URLSearchParams(raw)) out[k] = v;
  return out;
}

function metadataOf(params, prefix = "metadata") {
  const md = {};
  for (const [k, v] of Object.entries(params)) {
    const m = k.match(new RegExp(`^${prefix}\\[(.+)\\]$`));
    if (m) md[m[1]] = v;
  }
  return md;
}

// `line_items[i][price]` (a catalog price) or `line_items[i][price_data][...]`
// (an inline price; Stripe mints a one-time price for it).
function lineItems(params) {
  const items = [];
  for (let i = 0; `line_items[${i}][quantity]` in params || `line_items[${i}][price]` in params; i++) {
    const at = (k) => params[`line_items[${i}][${k}]`];
    const quantity = Number(at("quantity") ?? 1);
    if (at("price")) {
      items.push({ id: nextId("li"), object: "item", quantity, amount_total: null, price: { id: at("price"), object: "price", type: "recurring" } });
    } else {
      const unit = Number(at("price_data][unit_amount") ?? 0);
      items.push({
        id: nextId("li"),
        object: "item",
        quantity,
        amount_total: unit * quantity,
        description: at("price_data][product_data][name") ?? null,
        price: { id: nextId("price"), object: "price", type: "one_time", unit_amount: unit, currency: at("price_data][currency") ?? "usd" },
      });
    }
  }
  return items;
}

function sessionView(s, expand) {
  const view = { ...s };
  if (expand.includes("line_items")) {
    view.line_items = { object: "list", has_more: false, data: s._line_items };
  }
  delete view._line_items;
  return view;
}

function stripeRoute(req, res, path, params, query) {
  const parts = path.split("/").filter(Boolean); // ["v1", ...]
  const [, kind, id, action] = parts;
  const expand = [...query.entries(), ...Object.entries(params)].filter(([k]) => k === "expand" || k.startsWith("expand[")).map(([, v]) => v);

  if (kind === "customers" && req.method === "POST" && !id) {
    const c = { id: nextId("cus"), object: "customer", email: params.email ?? null, metadata: metadataOf(params) };
    stripe.customers.set(c.id, c);
    return json(res, 200, c);
  }
  if (kind === "customers" && id) {
    const c = stripe.customers.get(id);
    if (!c) return json(res, 404, { error: { type: "invalid_request_error", message: "No such customer" } });
    return json(res, 200, c);
  }
  if (kind === "checkout" && id === "sessions") {
    const sid = action;
    const sub = parts[4];
    if (req.method === "POST" && !sid) {
      const s = {
        id: nextId("cs_test"),
        object: "checkout.session",
        mode: params.mode ?? "subscription",
        status: "open",
        customer: params.customer ?? null,
        client_reference_id: params.client_reference_id ?? null,
        metadata: metadataOf(params),
        subscription: null,
        payment_status: "unpaid",
        url: `${params.success_url ?? "http://localhost/"}`,
        success_url: params.success_url ?? null,
        cancel_url: params.cancel_url ?? null,
        _trial_days: params["subscription_data[trial_period_days]"] ?? null,
        _line_items: lineItems(params),
      };
      // `subscription_data[metadata][k]` → `_subscription_metadata`
      s._subscription_metadata = {};
      for (const [k, v] of Object.entries(params)) {
        const m = k.match(/^subscription_data\[metadata\]\[(.+)\]$/);
        if (m) s._subscription_metadata[m[1]] = v;
      }
      stripe.sessions.set(s.id, s);
      return json(res, 200, sessionView(s, []));
    }
    if (req.method === "GET" && !sid) {
      const data = [...stripe.sessions.values()]
        .filter((s) => (!query.get("customer") || s.customer === query.get("customer")) && (!query.get("status") || s.status === query.get("status")))
        .map((s) => sessionView(s, []));
      return json(res, 200, { object: "list", has_more: false, data });
    }
    const s = stripe.sessions.get(sid);
    if (!s) return json(res, 404, { error: { type: "invalid_request_error", message: "No such checkout session" } });
    if (req.method === "POST" && sub === "expire") {
      s.status = "expired";
      return json(res, 200, sessionView(s, []));
    }
    return json(res, 200, sessionView(s, expand));
  }
  if (kind === "payment_methods" && id) {
    const pm = stripe.paymentMethods.get(id);
    if (!pm) return json(res, 404, { error: { type: "invalid_request_error", message: "No such payment method" } });
    return json(res, 200, pm);
  }
  if (kind === "subscriptions" && id) {
    const s = stripe.subscriptions.get(id);
    if (!s) return json(res, 404, { error: { type: "invalid_request_error", message: "No such subscription" } });
    return json(res, 200, s);
  }
  if (kind === "subscriptions" && req.method === "GET") {
    const data = [...stripe.subscriptions.values()].filter((s) => !query.get("customer") || s.customer === query.get("customer"));
    return json(res, 200, { object: "list", has_more: false, data });
  }
  return json(res, 404, { error: { type: "invalid_request_error", message: `e2e fake Stripe has no ${req.method} ${path}` } });
}

// Slack's Web API. The OAuth code is the workspace id, so each scenario
// connects its own workspace, and the bot token carries it back on every
// later call.
function legacySlackRoute(req, res, method, params) {
  const auth = req.headers.authorization ?? "";
  const team = auth.startsWith("Bearer xoxb-e2e-") ? auth.slice("Bearer xoxb-e2e-".length) : params.get("team") ?? "";
  switch (method) {
    case "oauth.v2.access": {
      const id = params.get("code") ?? "";
      return json(res, 200, {
        ok: true,
        access_token: `xoxb-e2e-${id}`,
        token_type: "bot",
        scope: "channels:history,channels:read,users:read",
        bot_user_id: "UE2EBOT",
        team: { id, name: `E2E ${id}` },
        authed_user: { id: "UE2EADMIN" },
      });
    }
    case "auth.test":
      return json(res, 200, { ok: true, team_id: team, user_id: "UE2EBOT", bot_id: "BE2EBOT", url: `https://${team.toLowerCase()}.slack.com/` });
    case "users.info": {
      const id = params.get("user") ?? "";
      return json(res, 200, { ok: true, user: { id, name: "riley", real_name: "Riley E2E", is_bot: false, profile: { real_name: "Riley E2E", display_name: "riley" } } });
    }
    case "conversations.info": {
      const id = params.get("channel") ?? "";
      return json(res, 200, { ok: true, channel: { id, name: "launch-room", is_private: false } });
    }
    default:
      return json(res, 200, { ok: true });
  }
}

// `POST /__stripe/complete/{session}` — what Stripe does when the customer
// finishes Checkout: the session completes and a (trialing) subscription
// exists. Returns the `checkout.session.completed` event to deliver; the
// harness signs it and posts it to the backend's real webhook route.
function completeSession(res, sid) {
  const s = stripe.sessions.get(sid);
  if (!s) return json(res, 404, { error: "no such session" });
  const start = now();
  const trialDays = Number(s._trial_days ?? 0);
  const end = start + (trialDays > 0 ? trialDays : 30) * 86400;
  const item = s._line_items.find((li) => li.price.type === "recurring") ?? s._line_items[0];
  // The card the customer entered. A fresh fingerprint per Checkout: the
  // backend allows one trial per card, and every scenario is a new customer.
  const pm = { id: nextId("pm"), object: "payment_method", type: "card", customer: s.customer, card: { brand: "visa", last4: "4242", fingerprint: `fp_${s.id}` } };
  stripe.paymentMethods.set(pm.id, pm);
  const sub = {
    id: nextId("sub"),
    default_payment_method: pm.id,
    object: "subscription",
    customer: s.customer,
    status: trialDays > 0 ? "trialing" : "active",
    metadata: s._subscription_metadata ?? {},
    current_period_start: start,
    current_period_end: end,
    trial_start: trialDays > 0 ? start : null,
    trial_end: trialDays > 0 ? end : null,
    cancel_at_period_end: false,
    latest_invoice: null,
    items: {
      object: "list",
      data: [{ id: nextId("si"), object: "subscription_item", quantity: item.quantity, price: item.price, current_period_start: start, current_period_end: end }],
    },
  };
  stripe.subscriptions.set(sub.id, sub);
  s.status = "complete";
  s.payment_status = trialDays > 0 ? "no_payment_required" : "paid";
  s.subscription = sub.id;
  const event = {
    id: nextId("evt"),
    object: "event",
    type: "checkout.session.completed",
    created: start,
    data: { object: sessionView(s, []) },
  };
  return json(res, 200, event);
}

// Google's push signing key, standing in for the key Google signs Pub/Sub
// push tokens with. The backend fetches it from `/google/oauth2/v3/certs`
// (`GOOGLE_OIDC_CERTS_URL`).
const googleKey = crypto.generateKeyPairSync("rsa", { modulusLength: 2048 });
const GOOGLE_KID = "e2e-google-push";
const b64url = (v) => Buffer.from(typeof v === "string" ? v : JSON.stringify(v)).toString("base64url");

function googlePushToken(claims) {
  const signed = `${b64url({ alg: "RS256", typ: "JWT", kid: GOOGLE_KID })}.${b64url(claims)}`;
  return `${signed}.${crypto.sign("RSA-SHA256", Buffer.from(signed), googleKey.privateKey).toString("base64url")}`;
}

// ── Slack ───────────────────────────────────────────────────────────────────
// The Web API the backend reaches through SLACK_API_BASE. Deterministic: the
// workspace has `channels` public channels C0E2E00001.. (default 200, set with
// E2E_SLACK_CHANNELS or `POST /__slack/config {"channels":N}`), each holding
// `SLACK_MESSAGES_PER_CHANNEL` messages posted 1h, 2h, 3h ... before the
// request, so a default backfill window always contains them and `oldest`
// trims them. Tokens are `xoxb-e2e-<team>` / `xoxp-e2e-<team>`; `auth.revoke`
// kills a token, after which every call with it answers `token_revoked`.
const legacySlackTeams = new Set();
const slack = {
  channels: Number(process.env.E2E_SLACK_CHANNELS ?? 200),
  revoked: new Set(),
};
const SLACK_MESSAGES_PER_CHANNEL = 3;
const SLACK_BOT_SCOPES =
  "channels:history channels:read users:read users:read.email reactions:read files:read canvases:read chat:write im:write channels:leave";
const SLACK_MEMBER_SCOPES =
  "channels:history channels:read groups:history groups:read im:history im:read mpim:history mpim:read users:read users:read.email";

const slackChannelId = (i) => `C0E2E${String(i).padStart(5, "0")}`;
const slackChannelIndex = (id) => {
  const m = /^C0E2E(\d{5})$/.exec(id ?? "");
  return m ? Number(m[1]) : 0;
};
const slackErr = (res, error) => json(res, 200, { ok: false, error });
const slackBearer = (req) => (req.headers.authorization ?? "").replace(/^Bearer\s+/i, "");

// `oauth.v2.access`: the code `team-<id>` signs into workspace T<id>, any other
// code into the default workspace. Both a bot and a user token come back, so
// the bot connect and the member (user-scope-only) connect share the fake.
function slackOAuth(res, params) {
  if (!params.code) return slackErr(res, "invalid_code");
  if (params.client_id !== FAKE_SLACK.clientId || params.client_secret !== FAKE_SLACK.clientSecret) return slackErr(res, "bad_client_secret");
  const named = /^team-([A-Za-z0-9]+)$/.exec(params.code);
  const team = named ? `T${named[1].toUpperCase()}` : "T0E2E00001";
  return json(res, 200, {
    ok: true,
    access_token: `xoxb-e2e-${team}`,
    token_type: "bot",
    scope: SLACK_BOT_SCOPES,
    bot_user_id: "U0E2EBOT01",
    app_id: "A0E2E00001",
    team: { id: team, name: named ? `E2E Workspace ${named[1]}` : "E2E Workspace" },
    enterprise: null,
    authed_user: {
      id: `U0E2E${team.slice(1, 6).padEnd(5, "0")}`,
      scope: SLACK_MEMBER_SCOPES,
      access_token: `xoxp-e2e-${team}`,
      token_type: "user",
    },
  });
}

function slackMessage(channel, i, nowSec, ageSec = (i + 1) * 3600) {
  const ts = nowSec - ageSec;
  return {
    type: "message",
    user: `U0E2EUSER${(i % 2) + 1}`,
    ts: `${ts}.${String(slackChannelIndex(channel) * 10 + i).padStart(6, "0")}`,
    text: `Message ${i + 1} in ${channel}: the e2e fake workspace shipped milestone ${slackChannelIndex(channel)}.${i + 1}.`,
  };
}

function slackRoute(req, res, method, params) {
  // Main's ingest scenarios use raw workspace IDs; retain their fixtures.
  if (method === "oauth.v2.access" && /^T[A-Z0-9]+$/.test(params.code ?? "")) {
    legacySlackTeams.add(params.code);
    return legacySlackRoute(req, res, method, new URLSearchParams(params));
  }
  if (legacySlackTeams.has(slackBearer(req).replace(/^xoxb-e2e-/, "")))
    return legacySlackRoute(req, res, method, new URLSearchParams(params));
  if (method === "oauth.v2.access") return slackOAuth(res, params);
  const token = slackBearer(req);
  if (!token) return slackErr(res, "not_authed");
  if (slack.revoked.has(token)) return slackErr(res, "token_revoked");
  const team = token.replace(/^xox[bp]-e2e-/, "") || "T0E2E00001";
  switch (method) {
    case "auth.test":
      return json(res, 200, {
        ok: true,
        url: `https://e2e-${team.toLowerCase()}.slack.com/`,
        team: "E2E Workspace",
        team_id: team,
        user: "coralswarm",
        user_id: "U0E2EBOT01",
        bot_id: "B0E2EBOT01",
      });
    case "auth.revoke":
      slack.revoked.add(token);
      return json(res, 200, { ok: true, revoked: true });
    case "users.conversations": {
      const limit = Math.min(Math.max(Number(params.limit ?? 100), 1), 1000);
      const start = params.cursor ? Number(Buffer.from(params.cursor, "base64").toString().replace(/^o:/, "")) || 0 : 0;
      const end = Math.min(start + limit, slack.channels);
      const channels = [];
      for (let i = start + 1; i <= end; i++) {
        channels.push({
          id: slackChannelId(i),
          name: `e2e-channel-${String(i).padStart(3, "0")}`,
          is_channel: true,
          is_private: false,
          is_archived: false,
          is_member: true,
          num_members: 2 + (i % 5),
          created: 1767225600,
        });
      }
      const next = end < slack.channels ? Buffer.from(`o:${end}`).toString("base64") : "";
      return json(res, 200, { ok: true, channels, response_metadata: { next_cursor: next } });
    }
    case "conversations.info": {
      const i = slackChannelIndex(params.channel);
      if (i < 1 || i > slack.channels) return slackErr(res, "channel_not_found");
      return json(res, 200, { ok: true, channel: { id: params.channel, name: `e2e-channel-${String(i).padStart(3, "0")}`, is_channel: true, is_private: false, is_member: true } });
    }
    case "conversations.members":
      return json(res, 200, { ok: true, members: ["U0E2EBOT01", "U0E2EUSER1", "U0E2EUSER2"], response_metadata: { next_cursor: "" } });
    case "conversations.history": {
      const i = slackChannelIndex(params.channel);
      if (i < 1 || i > slack.channels) return slackErr(res, "channel_not_found");
      // The single-message read (`oldest` = `latest`, inclusive) the doorbell
      // path makes for an event's `ts`: any ts a scenario posts exists, with a
      // text that names it, so a scenario can drive an event for a message of
      // its own choosing.
      if (params.inclusive === "true" && params.oldest && params.oldest === params.latest) {
        return json(res, 200, {
          ok: true,
          messages: [{ type: "message", user: "U0E2EUSER1", ts: params.oldest, text: `Live message ${params.oldest} in ${params.channel}: the e2e fake workspace posted it.` }],
          has_more: false,
          response_metadata: { next_cursor: "" },
        });
      }
      const nowSec = Math.floor(Date.now() / 1000);
      const oldest = Number(params.oldest ?? 0);
      const latest = params.latest ? Number(params.latest) : Infinity;
      const limit = Math.max(Number(params.limit ?? 100), 1);
      // Newest first, like Slack. Exclusive bounds unless `inclusive`.
      // A workspace whose code starts `old` (team-old<x>) also holds a message 10
      // days old and one 45 days old per channel, so a 7-day window has something
      // to exclude. Every other workspace is unchanged.
      const ages = Array.from({ length: SLACK_MESSAGES_PER_CHANNEL }, (_, k) => (k + 1) * 3600);
      if (team.startsWith("TOLD")) ages.push(10 * 86400, 45 * 86400);
      const all = ages.map((age, k) => slackMessage(params.channel, k, nowSec, age)).filter((m) => {
        const t = Number(m.ts);
        return t > oldest && t < latest;
      });
      const messages = all.slice(0, limit);
      // What was asked and what was served, for scenarios that check a window.
      Object.assign(seen[seen.length - 1], { bearer: token, params: { channel: params.channel, oldest: params.oldest ?? null }, served: messages.map((m) => Number(m.ts)) });
      return json(res, 200, { ok: true, messages, has_more: all.length > messages.length, response_metadata: { next_cursor: "" } });
    }
    case "users.info":
      return json(res, 200, {
        ok: true,
        user: {
          id: params.user,
          team_id: team,
          name: String(params.user ?? "").toLowerCase(),
          real_name: `E2E ${params.user}`,
          is_bot: params.user === "U0E2EBOT01",
          profile: { email: `${String(params.user ?? "u").toLowerCase()}@example.com`, display_name: `E2E ${params.user}` },
        },
      });
    default:
      return slackErr(res, "unknown_method");
  }
}

// ── Composio ────────────────────────────────────────────────────────────────
// REST v3.1 behind COMPOSIO_API_BASE, x-api-key authenticated. Responses
// cover the six pinned toolkit recipes under backend/src/connectors/composio/recipes:
//   GET  /auth_configs/{id}              toolkit + the read-only scope list
//   POST /connected_accounts/link        mints an account and an https redirect
//   GET  /connected_accounts/{id}        INITIATED until `POST /__composio/authorize/{id}`,
//                                        then ACTIVE with the pinned grant
//   DELETE /connected_accounts/{id}
//   POST /tools/execute/{TOOL}           server info, who-am-i, issue search, comments
// Auth config ids are `ac_e2e_<toolkit>` (the stack sets COMPOSIO_<TOOLKIT>_AUTH_CONFIG_ID).
const composio = { seq: 0, accounts: new Map() };
const COMPOSIO_SITE = { id: "e2e-jira-site-0001", url: "https://e2e-fake.atlassian.net" };
const composioErr = (res, status, message) => json(res, status, { error: { message, status } });

function composioToolkit(authConfig) {
  const m = /^ac_e2e_([a-z0-9]+)$/.exec(authConfig ?? "");
  return m ? m[1] : null;
}

const toolkitScopes = {
 confluence: ["read:content-details:confluence"],
 hubspot: ["oauth", "crm.objects.contacts.read", "crm.objects.companies.read", "crm.objects.deals.read"],
};
const JIRA_SCOPES = ["read:jira-work", "offline_access"];

function composioExecute(res, tool, body) {
  const ok = (data) => json(res, 200, { successful: true, data, error: null });
  const acct = composio.accounts.get(body.connected_account_id);
  if (!acct || acct.status !== "ACTIVE") return json(res, 200, { successful: false, data: null, error: { status_code: 401, message: "account not active" } });
  const nowIso = new Date().toISOString();
  switch (tool) {
    case "LINEAR_RUN_QUERY_OR_MUTATION": {
      const query = body.arguments?.query_or_mutation ?? "";
      if (query.includes("CoralSwarmWorkspace")) return ok({ data: { organization: { id: "e2e-linear" } } });
      if (acct.recipeProbe === "failed") return ok({ data: {} });
      if (acct.recipeProbe === "inconclusive") return ok({ data: Object.fromEntries(["issues", "comments", "projects"].map(k => [k, { nodes: [], pageInfo: { hasNextPage: false } }])) });
      const key = query.includes("CommentsChanged") ? "comments" : query.includes("ProjectsChanged") ? "projects" : "issues";
      return ok({ data: { [key]: { nodes: [{ id: key + "1", issue: { id: "issues1", title: "E2E Linear" }, title: "E2E Linear", name: "E2E project", description: "Acceptance capture", body: "Acceptance comment", updatedAt: nowIso }], pageInfo: { hasNextPage: false } } } });
    }
    case "CONFLUENCE_WHO_AM_I": return ok({ sites: [COMPOSIO_SITE] });
    case "CONFLUENCE_CQL_SEARCH": return ok({ _links: { base: COMPOSIO_SITE.url }, results: [{ content: { id: "page1", container: { id: "parent1" }, title: "E2E page", version: { when: nowIso }, body: { storage: { value: "Acceptance content" } } } }], totalSize: 1 });
    case "HUBSPOT_GET_ACCOUNT_INFO": return ok({ portalId: 12345 });
    case "HUBSPOT_SEARCH_CRM_OBJECTS_BY_CRITERIA": return ok({ results: [{ id: body.arguments?.objectType + "1", updatedAt: nowIso, properties: { name: "E2E company", description: "Acceptance capture", email: "fake@example.test", dealname: "E2E deal", hs_note_body: "Acceptance note", hs_call_title: "Acceptance call", hs_meeting_title: "Acceptance meeting" } }] });
    case "ZENDESK_GET_SEARCH_EXPORT": return ok({ results: [{ id: 1, subject: "E2E ticket", description: "Acceptance capture", updated_at: nowIso }], meta: { has_more: false } });
    case "ZENDESK_GET_TICKETS_CONVERSATION_LOG": return ok({ events: [], meta: { has_more: false } });
    case "DART_GET_CONFIG": return ok({ user: { email: "fake@example.test" } });
    case "DART_LIST_TASKS": return ok({ results: [{ id: "task1", title: "E2E task", description: "Acceptance capture", updatedAt: nowIso }], count: 1 });
    case "JIRA_GET_SERVER_INFO":
      return ok({ baseUrl: COMPOSIO_SITE.url, serverTitle: "E2E Jira" });
    case "JIRA_WHO_AM_I":
      return ok({ accountId: "e2e-jira-user-1", displayName: "E2E Jira User", sites: [COMPOSIO_SITE] });
    case "JIRA_SEARCH_FOR_ISSUES_USING_JQL_GET": {
      const issue = (n) => ({
        id: `1000${n}`,
        key: `E2E-${n}`,
        fields: {
          summary: `E2E issue ${n}`,
          description: `The e2e fake Jira issue ${n} tracks the billing export rollout.`,
          status: { name: "In Progress" },
          project: { id: "10000", name: "E2E Project" },
          creator: { accountId: "e2e-jira-user-1", displayName: "E2E Jira User" },
          created: nowIso,
          updated: nowIso,
        },
      });
      return ok({ issues: [issue(1), issue(2)], isLast: true });
    }
    case "JIRA_LIST_ISSUE_COMMENTS":
      return ok({
        comments: [
          { id: `c-${body.arguments?.issue_id_or_key ?? "x"}-1`, body: "Looks good, shipping it.", created: nowIso, updated: nowIso, author: { accountId: "e2e-jira-user-1", displayName: "E2E Jira User" } },
        ],
        total: 1,
      });
    default:
      return json(res, 200, { successful: false, data: null, error: { status_code: 404, message: `e2e fake Composio has no tool ${tool}` } });
  }
}

function composioRoute(req, res, path, body) {
  if (!req.headers["x-api-key"]) return composioErr(res, 401, "missing x-api-key");
  const parts = path.split("/").filter(Boolean);
  const [kind, id] = parts;
  if (kind === "auth_configs" && id && req.method === "GET") {
    const toolkit = composioToolkit(id);
    if (!toolkit) return composioErr(res, 404, "no such auth config");
    return json(res, 200, { id, toolkit: { slug: toolkit }, credentials: { scopes: toolkit === "jira" ? JIRA_SCOPES : (toolkitScopes[toolkit] ?? ["read"]) } });
  }
  if (kind === "connected_accounts" && id === "link" && req.method === "POST") {
    const toolkit = composioToolkit(body.auth_config_id);
    if (!toolkit) return composioErr(res, 404, "no such auth config");
    const accountId = `ca_e2e${String(++composio.seq).padStart(6, "0")}`;
    composio.accounts.set(accountId, { id: accountId, status: "INITIATED", user_id: body.user_id, toolkit, auth_config: body.auth_config_id });
    return json(res, 200, { connected_account_id: accountId, redirect_url: `https://connect.composio-e2e.example/link/${accountId}`, status: "INITIATED" });
  }
  if (kind === "connected_accounts" && id) {
    const a = composio.accounts.get(id);
    if (!a) return composioErr(res, 404, "no such connected account");
    if (req.method === "DELETE") {
      composio.accounts.delete(id);
      return json(res, 200, { success: true });
    }
    return json(res, 200, {
      id: a.id,
      params: { base_url: "https://e2e.zendesk.com" },
      status: a.status,
      user_id: a.user_id,
      toolkit: { slug: a.toolkit },
      auth_config: { id: a.auth_config },
      requested_scopes: a.toolkit === "jira" ? JIRA_SCOPES : (toolkitScopes[a.toolkit] ?? ["read"]),
    });
  }
  if (kind === "tools" && id === "execute" && parts[2] && req.method === "POST") return composioExecute(res, parts[2], body);
  return composioErr(res, 404, `e2e fake Composio has no ${req.method} ${path}`);
}

const githubRoute = process.env.E2E_GITHUB_PUBLIC_KEY_FILE ? githubFake({
  publicKey: fs.readFileSync(process.env.E2E_GITHUB_PUBLIC_KEY_FILE),
  callback: `http://localhost:${process.env.E2E_API_PORT ?? 18080}/v1/oauth/github/callback`,
}) : () => false;
const server = http.createServer((req, res) => {
  let raw = "";
  req.on("data", (d) => (raw += d));
  req.on("end", () => {
    const url = new URL(req.url, "http://fake");
    seen.push({ method: req.method, path: url.pathname, at: Date.now() });
    if (seen.length > 500) seen.shift();
    try {
      if (githubRoute(req, res, url, raw)) return;
      if (handleGoogleNative(req, res, url, raw)) return;
      if (microsoftRoute(req, res, url, raw)) return;
      if (url.pathname === "/__health") return json(res, 200, { ok: true });
      if (url.pathname === "/__requests") return json(res, 200, seen);
      const outage = url.pathname.match(/^\/__s3\/outage\/([0-9a-f-]{36})$/);
      if (outage && req.method === "POST") return json(res, 200, { out: [...oceansOut.add(outage[1])] });
      if (outage && req.method === "DELETE") return json(res, 200, { ended: oceansOut.delete(outage[1]) });
      if (url.pathname === "/__slack/config" && req.method === "POST") {
        const cfg = raw ? JSON.parse(raw) : {};
        if (Number.isInteger(cfg.channels) && cfg.channels >= 0) slack.channels = cfg.channels;
        if (cfg.reset) slack.revoked.clear();
        return json(res, 200, { channels: slack.channels, revoked: slack.revoked.size });
      }
      // What Composio does when the member finishes signing in at the redirect.
      const authorize = url.pathname.match(/^\/__composio\/authorize\/(.+)$/);
      if (authorize && req.method === "POST") {
        const a = composio.accounts.get(authorize[1]);
        if (!a) return json(res, 404, { error: "no such connected account" });
        const control = raw ? JSON.parse(raw) : {};
        a.status = control.status ?? "ACTIVE";
        a.recipeProbe = control.recipeProbe ?? "passed";
        return json(res, 200, { id: a.id, status: a.status });
      }
      if (url.pathname.startsWith("/slack/api/")) {
        const method = url.pathname.slice("/slack/api/".length);
        const params = req.method === "GET" ? Object.fromEntries(url.searchParams) : { ...Object.fromEntries(url.searchParams), ...form(raw) };
        return slackRoute(req, res, method, params);
      }
      if (url.pathname.startsWith("/composio/api/v3.1/")) {
        const path = url.pathname.slice("/composio/api/v3.1".length);
        return composioRoute(req, res, path, raw ? JSON.parse(raw) : {});
      }
      const m = url.pathname.match(/^\/__stripe\/complete\/(.+)$/);
      if (m && req.method === "POST") return completeSession(res, m[1]);
      if (url.pathname === "/google/oauth2/v3/certs") {
        const jwk = googleKey.publicKey.export({ format: "jwk" });
        return json(res, 200, { keys: [{ ...jwk, kid: GOOGLE_KID, alg: "RS256", use: "sig" }] });
      }
      if (url.pathname === "/__google/push-token" && req.method === "POST") {
        return json(res, 200, { token: googlePushToken(JSON.parse(raw)) });
      }
      if (url.pathname.startsWith("/openrouter/")) {
        const path = url.pathname.slice("/openrouter".length).replace(/^\/api/, "");
        const body = raw ? JSON.parse(raw) : {};
        if (path === "/v1/embeddings") {
          const inputs = Array.isArray(body.input) ? body.input : [body.input];
          return json(res, 200, {
            object: "list",
            model: body.model,
            data: inputs.map((t, index) => ({ object: "embedding", index, embedding: embed(t) })),
            usage: { prompt_tokens: inputs.length, total_tokens: inputs.length, cost: 0 },
          });
        }
        if (path === "/v1/chat/completions") return sendCompletion(res, body);
        if (path === "/v1/generation") {
          return json(res, 200, { data: { id: url.searchParams.get("id"), total_cost: 0, native_tokens_prompt: 100, native_tokens_completion: 20, provider_name: "e2e-fake" } });
        }
      }
      if (url.pathname.startsWith("/slack/api/")) {
        const params = new URLSearchParams(url.search);
        for (const [k, v] of Object.entries(form(raw))) params.set(k, v);
        return slackRoute(req, res, url.pathname.slice("/slack/api/".length), params);
      }
      if (url.pathname.startsWith("/stripe/")) {
        const path = url.pathname.slice("/stripe".length);
        return stripeRoute(req, res, path, form(raw), url.searchParams);
      }
      json(res, 404, { error: `e2e fakes: no route for ${req.method} ${url.pathname}` });
    } catch (e) {
      json(res, 500, { error: String(e?.stack ?? e) });
    }
  });
});

// Oceans whose object writes fail (`/__s3/outage/{ocean}`). Scoped to one
// ocean so a scenario's outage never reaches another scenario's captures.
const oceansOut = new Set();

// Path-style S3 (`/{bucket}/{key}`); every blob key holds its ocean id as one
// segment (`{graph}/{id}` or `{org}/{graph}/{id}`).
const s3Proxy = http.createServer((req, res) => {
  const segments = new URL(req.url, "http://s3").pathname.split("/");
  if (req.method === "PUT" && segments.some((s) => oceansOut.has(s))) {
    req.resume();
    res.writeHead(503, { "content-type": "application/xml" });
    return res.end("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>SlowDown</Code><Message>e2e: R2 outage</Message></Error>");
  }
  const headers = { ...req.headers };
  // This proxy already accepted the body; S3Mock need not be asked again.
  delete headers.expect;
  const upstream = http.request({ host: "127.0.0.1", port: S3_PORT, method: req.method, path: req.url, headers }, (up) => {
    res.writeHead(up.statusCode ?? 502, up.headers);
    up.pipe(res);
  });
  upstream.on("error", (e) => {
    res.writeHead(502, { "content-type": "text/plain" });
    res.end(`e2e s3 proxy: ${e.message}`);
  });
  req.pipe(upstream);
});

if (import.meta.url === `file://${process.argv[1]}` || process.argv[1]?.endsWith("fakes.mjs")) {
  server.listen(PORT, "127.0.0.1", () => console.log(`e2e fakes listening on http://127.0.0.1:${PORT}`));
  s3Proxy.listen(S3_PROXY_PORT, "127.0.0.1", () => console.log(`e2e s3 proxy on http://127.0.0.1:${S3_PROXY_PORT} -> ${S3_PORT}`));
}
