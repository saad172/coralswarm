import { expect, test } from "@playwright/test";
import { Api } from "../lib/api";
import { sessionToken } from "../lib/clerk";
import { cached, expectIndistinguishable, ingestNote, orgApi } from "../lib/isolation";
import { API_BASE } from "../stack/config.mjs";
import { readManifest, type N1Manifest } from "./manifest";

// N-1 phase 2, on the PRODUCTION release's backend, booted on the schema and
// the data THIS tree's backend wrote (seed.spec.ts). The rollback question:
// can the binary we would roll back to serve what the new one left behind?
//
// Only routes a release build serves without the e2e fakes: no seeding, no
// LLM answer, no blob storage, no Stripe. Reads, plus one light write.

let m: N1Manifest;
test.beforeAll(() => {
  m = readManifest();
});
const as = (request: Parameters<typeof Api.withToken>[0], userId: string) => new Api(request, userId, cached(() => sessionToken(userId)));

test("the production binary is up on this schema @n1", async ({ request }) => {
  for (const path of ["/healthz", "/readyz"]) expect((await request.get(`${API_BASE}${path}`)).status(), path).toBe(200);
  expect((await request.get(`${API_BASE}/v1/graphs`)).status()).toBe(401);
});

test("it serves a user's personal ocean, sources and atoms as written @n1", async ({ request }) => {
  const a = as(request, m.personal.userId);
  expect((await a.graphs()).map((g) => g.id)).toContain(m.personal.oceanId);
  expect(JSON.stringify(await a.json("GET", "/v1/bootstrap"))).toContain(m.personal.oceanId);
  expect(JSON.stringify(await a.json("GET", `/v1/graphs/${m.personal.oceanId}/sources`))).toContain(m.personal.sourceId);
  expect(JSON.stringify(await a.json("GET", `/v1/graphs/${m.personal.oceanId}/atoms`))).toContain(m.personal.atomId);
  expect(JSON.stringify(await a.json("GET", `/v1/atoms/${m.personal.atomId}`))).toContain(m.personal.secret);
});

test("it still isolates that data from another user @n1", async ({ request }) => {
  const stranger = as(request, m.stranger.userId);
  await expectIndistinguishable(stranger, "GET", (x) => `/v1/atoms/${x}`, m.personal.atomId, { secret: m.personal.secret });
  await expectIndistinguishable(stranger, "GET", (g) => `/v1/graphs/${g}/atoms`, m.personal.oceanId, { secret: m.personal.secret });
});

test("it serves the org's tenant database: oceans, members, the admin's content @n1", async ({ request }) => {
  const admin = await orgApi(request, m.org.admin.userId, m.org.orgId);
  const listed = (await admin.graphs()).map((g) => g.id);
  expect(listed).toContain(m.org.oceanId);
  expect(listed).toContain(m.org.privateOceanId);
  expect(JSON.stringify(await admin.json("GET", "/v1/orgs/members"))).toContain(m.org.member.email);
  expect(JSON.stringify(await admin.json("GET", `/v1/atoms/${m.org.atomId}`))).toContain(m.org.secret);

  const member = await orgApi(request, m.org.member.userId, m.org.orgId);
  expect((await member.graphs()).map((g) => g.id)).toContain(m.org.oceanId);
  await expectIndistinguishable(member, "GET", (x) => `/v1/atoms/${x}`, m.org.atomId, { secret: m.org.secret });
});

test("it accepts a new capture on top of the new data @n1", async ({ request }) => {
  const a = as(request, m.personal.userId);
  const { sourceId, atomIds } = await ingestNote(a, m.personal.oceanId, "Captured by the previous release after a rollback.");
  expect(JSON.stringify(await a.json("GET", `/v1/graphs/${m.personal.oceanId}/sources`))).toContain(sourceId);
  expect(JSON.stringify(await a.json("GET", `/v1/atoms/${atomIds[0]}`))).toContain("previous release");
});
