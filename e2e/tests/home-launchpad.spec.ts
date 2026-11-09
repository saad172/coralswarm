import { randomUUID } from "node:crypto";
import { expect, test } from "@playwright/test";
import { fixtureSql } from "../lib/pageBudgetFixture";
import { ingestNote, personalUser } from "../lib/isolation";
import { API_BASE, CONTROL_DB, REMOTE } from "../stack/config.mjs";

// Home launchpad reads (`GET /v1/home/continue`, `/v1/home/whats-new`,
// `/v1/home/attention`; contract docs/implementation/home-launchpad-api.md),
// API-level against the real stack, real Clerk session and serving role, on
// the personal plane. A brand-new user gets the setup state; a real capture
// flips `has_content`; then seeded rows (a chat, two sessions of one repo and
// branch, an agent-captured meeting session, a decision reef named by its
// coral, a dead Gmail credential, an MCP tool call, a question) come back
// from all three reads, also behind 200 newer checkpoints of the user's in an
// ocean they archived. Plain page reads: no worker, no `computed_at`,
// the first answer is already current. The caps, kind order, window and
// access rules are covered against seeded oceans by
// backend/tests/home_launchpad_pg.rs; the UI scenario lives with the web PR.

test("Home launchpad: setup state, then continue, what's new and attention from real rows", async ({ request }) => {
  test.skip(REMOTE, "seeds rows with SQL on the isolated local stack");
  test.setTimeout(120_000);
  const { api } = await personalUser(request, "launchpad");
  const graph = (await api.graphs()).find((g) => g.kind === "member" || g.kind === "personal")!;
  const me = await api.json("GET", "/v1/me");
  const sql = (query: string) => fixtureSql(CONTROL_DB, query);

  // A brand-new user: the setup checklist state, nothing to continue, nothing new.
  const empty = await api.json("GET", "/v1/home/attention");
  expect(empty).toEqual({ has_content: false, has_source: false, has_mcp: false, has_asked: false, attention: null, generated_at: expect.any(String) });
  expect((await api.json("GET", "/v1/home/continue")).items).toEqual([]);
  const quiet = await api.json("GET", "/v1/home/whats-new");
  expect(quiet).toMatchObject({ items: [], window_days: 7 });

  // A real capture is content the user can see.
  await ingestNote(api, graph.id, `Launchpad e2e ${randomUUID()}: the Orchid rollout is owned by Priya.`);
  expect((await api.json("GET", "/v1/home/attention")).has_content).toBe(true);

  const tag = randomUUID().slice(0, 8);
  const chat = sql(`INSERT INTO conversations(account_id,graph_id,user_id,title,updated_at) VALUES('${me.account_id}','${graph.id}','${me.user_id}','Launch questions ${tag}',now()-interval '5 minutes') RETURNING id`);
  for (const [n, minutes] of [[1, 30], [2, 2]] as const) {
    const id = `lp-${tag}-${n}`;
    sql(`INSERT INTO coding_sessions(graph_id,session_id,session_kind,project_key,branch,title,started_at,last_activity_at,platform) VALUES('${graph.id}','${id}','coding','github.com/acme/orchid','cs-home-${tag}','Session ${n}',now()-interval '${minutes} minutes',now()-interval '${minutes} minutes','claude-code')`);
    sql(`INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,ingested_by,"timestamp") VALUES('${graph.id}','coding_session','cc:${id}:ck1','e2e-${id}','checkpoint','${me.user_id}',now()-interval '${minutes} minutes')`);
  }
  // An agent-captured meeting (a session of kind meeting) has no transcript
  // for /meeting to open, so it is an agent row.
  const meetingSession = `lp-${tag}-mtg`;
  sql(`INSERT INTO coding_sessions(graph_id,session_id,session_kind,title,started_at,last_activity_at) VALUES('${graph.id}','${meetingSession}','meeting','Standup notes ${tag}',now()-interval '10 minutes',now()-interval '10 minutes')`);
  sql(`INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,ingested_by,"timestamp") VALUES('${graph.id}','meeting_session','cc:${meetingSession}:ck1','e2e-${meetingSession}','notes','${me.user_id}',now()-interval '10 minutes')`);
  // 200 of the user's checkpoints, newer than all of the above, in an ocean
  // they archived: skipped before Continue's per-type bound, never counted
  // against it, so the sessions above are still found.
  const archived = sql(`INSERT INTO graphs(account_id,name,kind,owner_user_id,archived_at) VALUES('${me.account_id}','Archived ${tag}','member','${me.user_id}',now()) RETURNING id`);
  sql(`INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,ingested_by,"timestamp") SELECT '${archived}','coding_session','cc:gone-${tag}-'||i||':ck1','e2e-gone-${tag}-'||i,'checkpoint','${me.user_id}',now() FROM generate_series(1,200) i`);
  const title = `Orchid launch moves ${tag}`;
  const atom = sql(`INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,ingested_by,"timestamp") VALUES('${graph.id}','note','lp-${tag}','e2e-lp-${tag}','Decision: ${title} to March 14.','${me.user_id}',now()) RETURNING atom_id`);
  const reef = sql(`INSERT INTO nodes(graph_id,type,title,body) VALUES('${graph.id}','decision','${title}',jsonb_build_object('decided_at',now()::text)) RETURNING node_id`);
  sql(`INSERT INTO node_atoms(node_id,atom_id) VALUES('${reef}','${atom}')`);
  sql(`INSERT INTO oauth_tokens(account_id,provider,scope,owner_user_id,access_token,credential_invalid_at,credential_invalid_reason) VALUES('${me.account_id}','google_gmail','gmail.readonly','${me.user_id}','\\x00'::bytea,now(),'google_invalid_grant')`);

  // Continue: newest first; the two sessions of one repo and branch are one
  // row whose context is the newest session's harness and title (the count
  // is its own field); the meeting session is an agent row.
  const cont = await api.json("GET", "/v1/home/continue");
  expect(cont.items.slice(0, 3), JSON.stringify(cont)).toEqual([
    { kind: "coding", id: `lp-${tag}-2`, title: `orchid · cs-home-${tag}`, context: "claude code · last: Session 2", count: 2, at: expect.any(String), graph_id: null },
    { kind: "chat", id: chat, title: expect.any(String), context: graph.name, count: null, at: expect.any(String), graph_id: graph.id },
    { kind: "agent", id: meetingSession, title: `Standup notes ${tag}`, context: graph.name, count: null, at: expect.any(String), graph_id: null },
  ]);
  expect(cont.items.length).toBeLessThanOrEqual(6);
  expect(cont.items.some((item: { kind: string }) => item.kind === "meeting"), JSON.stringify(cont)).toBe(false);

  // What's new: the decision reef, named, in the user's ocean. (The capture
  // above may form reefs of its own; decisions and threads still lead.)
  const fresh = await api.json("GET", "/v1/home/whats-new");
  expect(fresh.items, JSON.stringify(fresh)).toContainEqual({
    reef_id: reef, kind: "decision", title,
    ocean: { graph_id: graph.id, name: graph.name, kind: graph.kind },
    formed_at: expect.any(String),
  });
  expect(fresh.items.length).toBeLessThanOrEqual(5);
  const rank = (kind: string) => (kind === "decision" || kind === "thread" ? 0 : kind === "entity" ? 1 : 2);
  const ranks = fresh.items.map((item: { kind: string }) => rank(item.kind));
  expect(ranks, JSON.stringify(fresh)).toEqual([...ranks].sort((a, b) => a - b));

  // Attention: the dead credential is the one row that needs the user, and
  // a connected (even broken) source completes checklist step 1. The chat
  // has no message yet, so nothing has been asked.
  const attention = await api.json("GET", "/v1/home/attention");
  expect(attention).toEqual({
    has_content: true,
    has_source: true,
    has_mcp: false,
    has_asked: false,
    attention: { connector_id: "google_gmail:-:member", provider: "google_gmail", label: "Gmail needs you to sign in again", reason: "reauth" },
    generated_at: expect.any(String),
  });
  // An MCP client that has made a tool call completes step 2.
  sql(`INSERT INTO mcp_tool_calls(account_id,user_id,tool,outcome) VALUES('${me.account_id}','${me.user_id}','ask','ok')`);
  expect(await api.json("GET", "/v1/home/attention")).toMatchObject({ has_source: true, has_mcp: true, has_asked: false, attention: { provider: "google_gmail" } });
  // An assistant message alone is not a question; the user's first one
  // completes step 3.
  sql(`INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES('${chat}',1,'assistant','Hi')`);
  expect((await api.json("GET", "/v1/home/attention")).has_asked).toBe(false);
  sql(`INSERT INTO conversation_messages(conversation_id,seq,role,text) VALUES('${chat}',2,'user','Who owns the Orchid rollout?')`);
  expect(await api.json("GET", "/v1/home/attention")).toMatchObject({ has_content: true, has_source: true, has_mcp: true, has_asked: true });

  // No parameters, a token required, and live (never cached) private reads.
  for (const path of ["/v1/home/continue", "/v1/home/whats-new", "/v1/home/attention"]) {
    expect((await api.call("GET", `${path}?since=2026-01-01`)).status(), path).toBe(400);
    expect((await request.get(`${API_BASE}${path}`)).status(), path).toBe(401);
    expect((await api.call("GET", path)).headers()["cache-control"], path).toBe("no-store");
  }
});
