import { expect, it } from "vitest";
import { StoredReadLedger, storedReadEvidence, type LoadAttempt } from "./pageLoadEvidence";
const pending = { model: "home_metrics", version: 1, payload: {}, computed_at: null, stale: true, refresh_eta: "2026-10-31T00:00:02Z" };
const attempt = (identity = "u|/v1/home/metrics?days=30", started = 0, path = "/v1/home/metrics"): LoadAttempt => ({ identity, path, method: "GET", started, finished: null, status: null, ledger: "initial", model: null, computedAt: null, refreshEta: null });
function trace(body: unknown, nextIdentity?: string, path?: string) {
  const ledger = new StoredReadLedger(); const rows: LoadAttempt[] = [];
  for (let n = 0; n < 6; n++) {
    const row = attempt(n ? `other-${n}` : undefined, 0, path);
    ledger.start(row); rows.push(row); row.status = 200; row.finished = 1;
    ledger.response(row, n ? {} : body);
  }
  const seventh = attempt(nextIdentity, 2001, path); ledger.start(seventh); rows.push(seventh);
  return rows;
}
it.each([
  ["wrong version", { ...pending, version: 999 }],
  ["wrong model", { ...pending, model: "conversation_list" }],
  ["missing payload", { model: "home_metrics", version: 1, computed_at: null, stale: true, refresh_eta: pending.refresh_eta }],
  ["parseable non-timestamp", { ...pending, computed_at: "0" }],
  ["invalid calendar timestamp", { ...pending, computed_at: "2026-02-30T00:00:00Z" }],
  ["bad computed timestamp", { ...pending, computed_at: "not a date" }],
  ["bad ETA", { ...pending, refresh_eta: "invalid" }],
  ["missing stale", { model: "home_metrics", version: 1, payload: {}, computed_at: null, refresh_eta: pending.refresh_eta }],
  ["null authoritative payload", { ...pending, computed_at: "2026-10-31T00:00:00Z", payload: null }],
  ["scalar authoritative payload", { ...pending, computed_at: "2026-10-31T00:00:00Z", payload: 0 }],
  ["scalar pending payload", { ...pending, payload: 0 }],
  ["null pending payload", { ...pending, payload: null }],
  ["wrong stale type", { ...pending, stale: 1 }],
  ["uncomputed but fresh", { ...pending, stale: false }],
  ["uncomputed without refresh identity", { ...pending, refresh_eta: null }],
  ["owed but fresh", { ...pending, computed_at: "2026-10-31T00:00:00Z", stale: false }],
])("does not exempt a seventh critical start using %s", (_kind, body) => {
  const rows = trace(body);
  expect(rows).toHaveLength(7); expect(rows.filter((r) => r.ledger === "initial")).toHaveLength(7);
});
it("valid evidence exempts only a same-address owed poll, retaining all seven starts", () => {
  const rows = trace(pending); expect(rows).toHaveLength(7); expect(rows.at(-1)?.ledger).toBe("owed-refresh");
  for (const other of ["other-user|/v1/home/metrics?days=30", "u|/v1/home/metrics?days=60"]) expect(trace(pending, other).at(-1)?.ledger).toBe("initial");
});
it("does not accept an envelope on an unrelated endpoint", () => {
  expect(storedReadEvidence("/v1/activity", pending)).toBeNull();
  expect(trace(pending, undefined, "/v1/activity").filter((r) => r.ledger === "initial")).toHaveLength(7);
});
it("never exempts retries, overlapping attempts, or polls after computed resolution", () => {
  for (const state of ["failure", "overlap", "resolved"] as const) {
    const ledger = new StoredReadLedger(); const prior = attempt(); ledger.start(prior);
    prior.status = state === "failure" ? 503 : 200; prior.finished = state === "overlap" ? null : 1;
    ledger.response(prior, state === "resolved" ? { ...pending, computed_at: "2026-10-31T00:00:01Z", refresh_eta: null } : pending);
    const next = attempt(undefined, 2001); ledger.start(next); expect(next.ledger).not.toBe("owed-refresh");
  }
});
it("invalid responses revoke older owed evidence instead of leaving an exemption behind", () => {
  const ledger = new StoredReadLedger(); const first = attempt(); ledger.start(first); first.status = 200; first.finished = 1; ledger.response(first, pending);
  const second = attempt(undefined, 2001); ledger.start(second); second.status = 200; second.finished = 2002; ledger.response(second, { ...pending, version: 999 });
  const third = attempt(undefined, 4002); ledger.start(third); expect(third.ledger).toBe("initial");
});

it("uses the same model payload checks as the application before accepting owed evidence", () => {
  expect(storedReadEvidence("/v1/conversations", { ...pending, model: "conversation_list", computed_at: "2026-10-31T00:00:00Z", payload: { conversations: [{ id: "bad", title: "Private" }], shared: [] } })).toBeNull();
});
it.each([null, {}, { account_id: "bad", anchor_graph_id: null, refresh_path: "/v1/load/oceans/component/refresh", stored: pending }])("rejects malformed fan-in constituents without accepting an outer certificate: %s", (component) => {
  expect(storedReadEvidence("/v1/load/oceans", { components: [component] })).toBeNull();
});
it("selects literal registered paths before overlapping model parameter paths", () => {
  const evidence = storedReadEvidence("/v1/connectors/status", { ...pending, model: "connector_status" });
  expect(evidence?.[0].model).toBe("connector_status");
});
it("rejects a detail payload for another conversation even when its own UUID is valid", () => {
  const id = "11111111-1111-4111-8111-111111111111";
  expect(storedReadEvidence(`/v1/load/conversations/${id}`, { ...pending, model: "conversation_detail", computed_at: "2026-10-31T00:00:00Z",
    payload: { conversation: { id: "22222222-2222-4222-8222-222222222222", title: "Other private chat", library_scope: null, scope_status: "unscoped" },
      resolved_scope: null, messages: [], messages_truncated: false, older_before_seq: null } })).toBeNull();
});

it("does not exempt a different actual address even if a trace reuses its credential identity", () => {
  const ledger = new StoredReadLedger();
  const first = { ...attempt(), address: "/v1/home/metrics?days=30" };
  ledger.start(first); first.status = 200; first.finished = 1; ledger.response(first, pending);
  const changed = { ...attempt(undefined, 2001), address: "/v1/home/metrics?days=60" };
  ledger.start(changed); expect(changed.ledger).toBe("initial");
});
it("retains per-account fan-in certificates and bounds unchanged pending responses despite anchor changes", () => {
  const ledger = new StoredReadLedger();
  const account = "11111111-1111-4111-8111-111111111111";
  const graph = "22222222-2222-4222-8222-222222222222";
  const body = (anchor: string | null) => ({ components: [{ account_id: account, anchor_graph_id: anchor,
    refresh_path: `/v1/load/oceans/component/refresh${anchor ? `?graph=${anchor}` : ""}`,
    stored: { ...pending, model: "oceans_index" } }] });
  const first = attempt("u|oceans", 0, "/v1/load/oceans"); ledger.start(first); first.status = 200; first.finished = 1; ledger.response(first, body(null));
  expect(first.certificates?.[0]).toMatchObject({ identity: account, model: "oceans_index", computedAt: null, stale: true });
  const second = attempt("u|oceans", 2001, "/v1/load/oceans"); ledger.start(second);
  expect(second.owedFrom?.[0].identity).toBe(account);
  const deadline = second.owedFrom![0].deadline;
  second.status = 200; second.finished = 2002; ledger.response(second, body(graph));
  const third = attempt("u|oceans", 4002, "/v1/load/oceans"); ledger.start(third);
  expect(third.owedFrom?.[0].deadline).toBe(deadline);
  third.status = 200; third.finished = 4003; ledger.response(third, body(graph));
  const expired = attempt("u|oceans", deadline + 1, "/v1/load/oceans"); ledger.start(expired);
  expect(expired.ledger).toBe("initial");
});

it("qualifies a callback phase only after its real same-provider and same-credential refresh acknowledgement", async () => {
  const { ConnectorCallbackPhase } = await import('./pageLoadEvidence');
  const phase = new ConnectorCallbackPhase('/connectors/return?provider=slack&result=ok', true);
  const read = attempt('u|/v1/connectors/slack', 21, '/v1/connectors/slack');
  expect(phase.classify(read)).toBeNull();
  const post = { ...attempt('u|/v1/connectors/slack/refresh', 10, '/v1/connectors/slack/refresh'), method: 'POST', status: 202, finished: 20 };
  phase.acknowledge(post, { refresh_eta: 'not a timestamp' });
  expect(phase.classify(read)).toBeNull();
  phase.acknowledge(post, { refresh_eta: '2026-10-31T00:00:00Z' });
  expect(phase.classify(read)).toMatchObject({ acknowledgedBy: post.identity });
  expect(phase.classify({ ...read, identity: 'other|/v1/connectors/slack' })).toBeNull();
  expect(phase.classify({ ...read, path: '/v1/connectors/google' })).toBeNull();
  expect(phase.classify({ ...read, started: 19 })).toBeNull();
  for (const [url, declared] of [['/connectors/return?provider=slack', true], ['/home?provider=slack&result=ok', true], ['/connectors/return?provider=slack&result=ok', false]] as const) {
    const invalid = new ConnectorCallbackPhase(url, declared); invalid.acknowledge(post, { refresh_eta: null }); expect(invalid.classify(read)).toBeNull();
  }
});
