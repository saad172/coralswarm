import { expect, test } from "../lib/fixtures";
import { Api } from "../lib/api";
import { createUser } from "../lib/clerk";

// The desktop app is retired (PR 3/7): its backend routes are gone, and the
// desktop's screen-capture `work` session kind is an unknown kind.

test("the retired desktop and capture-policy routes are gone", async ({ request }) => {
  const user = await createUser("desktop");
  const api = new Api(request, user.id);
  await api.personalGraph();
  const routes: ["GET" | "POST" | "PUT" | "DELETE", string, unknown?][] = [
    ["POST", "/v1/desktop/tokens", { label: "e2e" }],
    ["GET", "/v1/desktop/tokens"],
    ["DELETE", "/v1/desktop/tokens/00000000-0000-0000-0000-000000000000"],
    ["POST", "/v1/desktop/auth/ticket", {}],
    ["GET", "/v1/client-config"],
    ["PUT", "/v1/capture-policy", { captured: {}, deny_list: [] }],
    ["PUT", "/v1/org/capture-policy", { captured: {}, deny_list: [] }],
    ["POST", "/v1/org/capture-attestation", { notice_version: 1 }],
  ];
  for (const [method, path, body] of routes) {
    const res = await api.call(method, path, body);
    expect(res.status(), `${method} ${path}`).toBe(404);
  }
});

test("a work session is rejected as an unknown session kind", async ({ request }) => {
  const user = await createUser("workkind");
  const api = new Api(request, user.id);
  const graph = await api.personalGraph();
  const res = await api.call("POST", "/v1/ingest/atom", {
    graph_id: graph.id,
    source_type: "note",
    source_id: "e2e-work-session-checkpoint",
    content: "A screen-observed work checkpoint.",
    participants: [],
    timestamp: new Date().toISOString(),
    session_id: "e2e-work-session",
    session_kind: "work",
  });
  expect(res.status()).toBe(422);
  expect((await res.json()).error).toBe("session_kind must be one of: coding, meeting, agent");

  const list = await api.call("GET", `/v1/sessions?graph_id=${graph.id}&kind=work`);
  expect(list.status()).toBe(400);
});
