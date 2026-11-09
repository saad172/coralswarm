import { test as base, expect } from "@playwright/test";
import crypto from "node:crypto";
import { addMember, createReadyOrg, expectIndistinguishable, ingestNote, orgApi, personalUser, probe, type Org } from "../lib/isolation";
import { API_BASE, WEB_BASE } from "../stack/config.mjs";

// Ported from the org-tenancy suites (ci.yml `cross-tenant-probe`,
// `binding-gate`): two real self-serve orgs, each with its own tenant
// database built by the backend's provisioning job, BOTH live in the same
// backend at once, so a forged id has a real other tenant to cross into.
//
// The two orgs are a worker fixture: provisioning one is ~7 s of the stack's
// own work, and every scenario below only reads them (or asserts that a
// write did nothing), so each shard builds them once.

type Tenant = Org & {
  /** The admin's own private ocean in the org ("My Ocean", kind `member`):
   *  ingest lands there, the org ocean is fed by promotion. */
  privateOceanId: string;
  secret: string;
  atomId: string;
  member: Awaited<ReturnType<typeof addMember>>;
};

const test = base.extend<{}, { tenants: { a: Tenant; b: Tenant } }>({
  tenants: [
    async ({ playwright }, use) => {
      const request = await playwright.request.newContext();
      const build = async (label: string): Promise<Tenant> => {
        const org = await createReadyOrg(request, label);
        const mine = (await org.adminInOrg.graphs()).find((g) => g.kind === "member");
        expect(mine, "the org admin has no private ocean in the org").toBeTruthy();
        const secret = `${label}-roadmap-${crypto.randomUUID()}`;
        const { atomIds } = await ingestNote(org.adminInOrg, mine!.id, `The ${secret} is due in Q3.`);
        const member = await addMember(request, org, `${label}-member`);
        return { ...org, privateOceanId: mine!.id, secret, atomId: atomIds[0], member };
      };
      const [a, b] = await Promise.all([build("iso-orga"), build("iso-orgb")]);
      await use({ a, b });
      await request.dispose();
    },
    { scope: "worker", timeout: 90_000 },
  ],
});

test.describe("org tenants are isolated from each other @isolation", () => {
  test("an org's oceans and atoms are unreachable from another org, by real id", async ({ tenants: { a, b } }) => {
    // Positive controls: each org reads its own content through its own
    // pool, and its member sees the org's shared ocean. Without these, a gate
    // that refuses everything would pass every arm below.
    for (const t of [a, b]) {
      expect(JSON.stringify(await t.adminInOrg.json("GET", `/v1/atoms/${t.atomId}`))).toContain(t.secret);
      expect((await t.member.inOrg.graphs()).map((g) => g.id)).toContain(t.oceanId);
      expect((await t.member.inOrg.json("GET", `/v1/graphs/${t.oceanId}`)).id).toBe(t.oceanId);
    }
    for (const [from, to] of [
      [a, b],
      [b, a],
    ] as const) {
      for (const who of [from.adminInOrg, from.member.inOrg]) {
        const listed = (await who.graphs()).map((g) => g.id);
        expect(listed).not.toContain(to.oceanId);
        expect(listed).not.toContain(to.privateOceanId);
        await expectIndistinguishable(who, "GET", (x) => `/v1/atoms/${x}`, to.atomId, { secret: to.secret });
        for (const ocean of [to.oceanId, to.privateOceanId]) {
          await expectIndistinguishable(who, "GET", (g) => `/v1/graphs/${g}`, ocean, { secret: to.secret });
          await expectIndistinguishable(who, "GET", (g) => `/v1/graphs/${g}/stats`, ocean);
          await expectIndistinguishable(who, "GET", (g) => `/v1/graphs/${g}/atoms`, ocean, { secret: to.secret });
          await expectIndistinguishable(who, "GET", (g) => `/v1/graphs/${g}/sources`, ocean, { secret: to.secret });
        }
        // The other org's atom id, asked for under this org's OWN ocean.
        await expectIndistinguishable(who, "GET", (x) => `/v1/graphs/${from.oceanId}/atoms/${x}`, to.atomId, { secret: to.secret });
      }
    }
  });

  test("an org refuses another org's writes and questions, and nothing lands in it", async ({ tenants: { a, b } }) => {
    // Both of B's oceans: the shared org ocean and the admin's private one.
    const snapshot = async () =>
      Promise.all([b.oceanId, b.privateOceanId].map((g) => b.adminInOrg.json("GET", `/v1/graphs/${g}/sources`)));
    const before = await snapshot();
    for (const ocean of [b.oceanId, b.privateOceanId]) {
      await expectIndistinguishable(a.adminInOrg, "POST", () => "/v1/ingest/atom", ocean, {
        label: "POST /v1/ingest/atom into another org's ocean",
        data: (g) => ({
          graph_id: g,
          source_type: "note",
          source_id: "e2e-iso-forged",
          content: "planted by another org",
          participants: [],
          timestamp: new Date().toISOString(),
        }),
      });
    }
    await expectIndistinguishable(a.member.inOrg, "POST", () => "/v1/queries", b.privateOceanId, {
      label: "POST /v1/queries against another org's ocean",
      data: (g) => ({ text: `When is the ${b.secret} due?`, graph_id: g }),
      secret: b.secret,
    });
    expect(await snapshot(), "B's oceans changed after A's refused writes").toEqual(before);
  });

  test("org admin routes never act on another org", async ({ tenants: { a, b } }) => {
    const rosterB = async () => JSON.stringify(await b.adminInOrg.json("GET", "/v1/orgs/members"));
    const before = await rosterB();
    expect(before).toContain(b.member.email);
    // A's roster never lists B's people.
    const rosterA = JSON.stringify(await a.adminInOrg.json("GET", "/v1/orgs/members"));
    expect(rosterA).toContain(a.member.email);
    expect(rosterA).not.toContain(b.member.email);
    expect(rosterA).not.toContain(b.admin.email);

    // Org-id routes: B's real org id answers A's admin exactly like an org
    // id that does not exist.
    for (const path of [(o: string) => `/v1/orgs/${o}/provisioning`, (o: string) => `/v1/orgs/${o}/deletion`]) {
      const real = await probe(a.adminInOrg, "GET", path(b.orgId));
      const ghost = await probe(a.adminInOrg, "GET", path(`org_${crypto.randomUUID().replace(/-/g, "").slice(0, 27)}`));
      expect(real.status, `${path("{org}")}: ${real.body}`).toBeGreaterThanOrEqual(400);
      expect(real.status).toBeLessThan(500);
      expect(real, `${path("{org}")}: another org's id must look nonexistent`).toEqual(ghost);
    }

    // Member and invite routes, with B's REAL ids. Both are keyed by the
    // member's user id (`Path<Uuid>`); B's member was invited and has never
    // signed in, so their id is also B's pending invitation. Each answers A's
    // admin exactly like an id that never existed.
    const entries: any[] = JSON.parse(before).members ?? JSON.parse(before);
    const bEntry = entries.find((m) => JSON.stringify(m).includes(b.member.email));
    const bMemberId: string | undefined = bEntry?.user_id ?? bEntry?.id;
    expect(bMemberId, `no user id for ${b.member.email} in B's roster: ${before.slice(0, 400)}`).toMatch(/^[0-9a-f-]{36}$/);
    await expectIndistinguishable(a.adminInOrg, "DELETE", (u) => `/v1/orgs/members/${u}`, bMemberId!);
    await expectIndistinguishable(a.adminInOrg, "DELETE", (u) => `/v1/orgs/invites/${u}`, bMemberId!);
    await expectIndistinguishable(a.adminInOrg, "POST", (u) => `/v1/orgs/invites/${u}/resend`, bMemberId!);
    expect(await rosterB(), "B's roster changed after A's admin acted on B's member").toEqual(before);
    // And B's member is still in B: their org session still reaches B's ocean.
    expect((await b.member.inOrg.json("GET", `/v1/graphs/${b.oceanId}`)).id).toBe(b.oceanId);
  });

  test("personal-ocean data is invisible to org members, and an org session never serves the consumer plane", async ({ tenants: { a } }) => {
    const personal = await a.admin.api.personalGraph();
    const secret = `diary-${crypto.randomUUID()}`;
    const { atomIds } = await ingestNote(a.admin.api, personal.id, `My ${secret} is private.`);
    // The owner, on their personal session, reads it.
    expect(JSON.stringify(await a.admin.api.json("GET", `/v1/atoms/${atomIds[0]}`))).toContain(secret);

    // Inside the org, too: the admin's private ocean ("My Ocean") is theirs
    // alone; a fellow member gets what a nonexistent ocean gets.
    expect((await a.member.inOrg.graphs()).map((g) => g.id)).not.toContain(a.privateOceanId);
    await expectIndistinguishable(a.member.inOrg, "GET", (x) => `/v1/atoms/${x}`, a.atomId, { secret: a.secret });
    await expectIndistinguishable(a.member.inOrg, "GET", (g) => `/v1/graphs/${g}/atoms`, a.privateOceanId, { secret: a.secret });

    // A member of the same org: neither their org session nor their own
    // personal session reaches the admin's personal ocean.
    for (const who of [a.member.inOrg, a.member.personal]) {
      expect((await who.graphs()).map((g) => g.id)).not.toContain(personal.id);
      await expectIndistinguishable(who, "GET", (x) => `/v1/atoms/${x}`, atomIds[0], { secret });
      await expectIndistinguishable(who, "GET", (g) => `/v1/graphs/${g}/atoms`, personal.id, { secret });
    }
    // The admin's OWN org session: an org-active token is served the
    // tenant plane only, so their own personal ocean is not there...
    expect((await a.adminInOrg.graphs()).map((g) => g.id)).not.toContain(personal.id);
    await expectIndistinguishable(a.adminInOrg, "GET", (x) => `/v1/atoms/${x}`, atomIds[0], { secret });
    // ...and the personal session is not served the org's tenant plane.
    expect((await a.admin.api.graphs()).map((g) => g.id)).not.toContain(a.oceanId);
    await expectIndistinguishable(a.admin.api, "GET", (x) => `/v1/atoms/${x}`, a.atomId, { secret: a.secret });
  });
});

test.describe("the binding gate picks the plane from the claim, never the Host @isolation", () => {
  const port = new URL(API_BASE).port;

  test("retired org addresses and foreign hosts are refused before auth", async ({ request }) => {
    const { api } = await personalUser(request, "iso-gate");
    await api.graphs();
    const get = async (host: string, auth: "valid" | "garbage" | "none") => {
      const headers: Record<string, string> = { host };
      const res =
        auth === "valid"
          ? await api.call("GET", "/v1/graphs", undefined, headers)
          : await request.get(`${API_BASE}/v1/graphs`, { headers: auth === "garbage" ? { ...headers, authorization: "Bearer not-a-jwt" } : headers });
      return res.status();
    };
    // Sanity: the apex serves a valid token, refuses a bad one with 401.
    expect(await get(`localhost:${port}`, "valid")).toBe(200);
    expect(await get(`localhost:${port}`, "garbage")).toBe(401);
    // `<slug>.<apex>` is the retired org API address: 410 for everyone,
    // before auth even runs (a garbage token is 410, not 401).
    for (const auth of ["valid", "garbage", "none"] as const) {
      expect(await get(`acme.localhost:${port}`, auth), `retired org host, ${auth} token`).toBe(410);
    }
    // A host that is not ours: 404 before auth, for a valid token too.
    for (const host of ["evil.example", `127.0.0.1:${port}`, `a.b.localhost:${port}`]) {
      for (const auth of ["valid", "garbage", "none"] as const) {
        expect(await get(host, auth), `foreign host ${host}, ${auth} token`).toBe(404);
      }
    }
  });

  test("forwarded-host headers never select a tenant, with another tenant live", async ({ tenants: { a, b } }) => {
    const forged: Record<string, string>[] = [
      { "x-forwarded-host": "b.localhost" },
      { forwarded: `host=${b.orgId}.localhost;proto=https` },
      { "x-original-host": "b.localhost" },
      { "x-forwarded-host": `localhost:${port}`, "x-forwarded-for": "10.0.0.1" },
    ];
    for (const who of [a.adminInOrg, a.member.inOrg, a.admin.api]) {
      const plain = await probe(who, "GET", "/v1/graphs");
      expect(plain.status).toBe(200);
      for (const h of forged) {
        expect(await probe(who, "GET", "/v1/graphs", undefined, h), `forged ${JSON.stringify(h)} changed the verdict`).toEqual(plain);
      }
      const atom = await probe(who, "GET", `/v1/atoms/${b.atomId}`, undefined, { "x-forwarded-host": "b.localhost" });
      expect(atom).toEqual(await probe(who, "GET", `/v1/atoms/${b.atomId}`));
      expect(atom.status).toBe(404);
      expect(atom.body).not.toContain(b.secret);
    }
  });

  test("an org whose tenant is not ready refuses its org session instead of falling back to the personal plane", async ({ request }) => {
    const { user, api: personalApi } = await personalUser(request, "iso-unready", "E2E Unready");
    const personal = await personalApi.personalGraph();
    const created = await personalApi.json(
      "POST",
      "/v1/orgs",
      { name: `E2E iso-unready ${crypto.randomUUID().slice(0, 6)}`, seats: 2, success_url: `${WEB_BASE}/x`, cancel_url: `${WEB_BASE}/y` },
      { "Idempotency-Key": crypto.randomUUID() },
    );
    expect(created.status).toBe("awaiting_payment");
    const inOrg = await orgApi(request, user.id, created.org_id);
    for (const path of ["/v1/graphs", `/v1/graphs/${personal.id}/atoms`, "/v1/bootstrap"]) {
      const res = await inOrg.call("GET", path);
      const body = await res.text();
      expect(res.status(), `${path} on an unprovisioned org's session: ${body.slice(0, 200)}`).toBeGreaterThanOrEqual(400);
      expect(body).not.toContain(personal.id);
    }
  });
});
