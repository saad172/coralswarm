import { waitForComputed } from "../lib/readModels";
import { expect, test } from "../lib/fixtures";
import { Api } from "../lib/api";
import { createUser } from "../lib/clerk";
import { API_BASE } from "../stack/config.mjs";

// `GET /v1/connectors/status` (docs/implementation/connector-status-api.md):
// the Home dashboard's connector health. API-level: the populated states
// (failing / stale / healthy per provider) are covered by the backend unit
// tests and the Postgres test, because no public route can plant a revoked
// credential or a sync error. What only the real stack can prove is the
// contract at the door: auth, the flag, the empty answer for a user with no
// connectors, and the response headers and shape.

test("connector status requires a signed-in caller", async ({ request }) => {
  const res = await request.get(`${API_BASE}/v1/connectors/status`);
  expect(res.status()).toBe(401);
});

test("a user with no connectors gets an empty, uncached, well-formed answer", async ({ request }) => {
  const user = await createUser("connector-status");
  const api = new Api(request, user.id);
  // The first authenticated call provisions the account.
  await api.personalGraph();

  const computed = await waitForComputed(api, "/v1/connectors/status");
  const res = await api.call("GET", "/v1/connectors/status");
  expect(res.status(), await res.text()).toBe(200);
  expect(res.headers()["cache-control"]).toBe("no-store");
  expect((await res.json()).payload).toEqual(computed.payload);
  expect(computed.payload).toEqual({
    connectors: [],
    summary: { total: 0, healthy: 0, stale: 0, failing: 0 },
  });
});
