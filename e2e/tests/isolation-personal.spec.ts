import crypto from "node:crypto";
import { expect, test } from "../lib/fixtures";
import { expectIndistinguishable, ingestNote, personalUser } from "../lib/isolation";

// Ported from the consumer-plane RLS suites (ci.yml `consumer-rls`,
// `rls-probe-harness`): two real users on the shared consumer plane, the
// backend serving as `coralswarm_app` with RLS enforced. User B guesses user
// A's real ids on every read and write route that takes one. What the pg
// suites asserted on SQL (`app.account_id` scoped to B returns none of A's
// rows), these assert on the product: B gets exactly what an id that never
// existed gets, and never A's content.

test.describe("personal oceans are private to their owner @isolation", () => {
  test("another user's ocean, atoms and sources are indistinguishable from nonexistent @smoke", async ({ request }) => {
    const [a, b] = await Promise.all([personalUser(request, "iso-a"), personalUser(request, "iso-b")]);
    const [aOcean, bOcean] = await Promise.all([a.api.personalGraph(), b.api.personalGraph()]);
    const secret = `halcyon-${crypto.randomUUID()}`;
    const { atomIds } = await ingestNote(a.api, aOcean.id, `The ${secret} rollout is owned by Priya.`);
    const atomId = atomIds[0];

    // Positive control: the owner reads it, so a gate that refuses everything
    // cannot pass the arms below.
    const own = await a.api.json("GET", `/v1/atoms/${atomId}`);
    expect(JSON.stringify(own)).toContain(secret);

    // B's own listing never names A's ocean.
    const bGraphs = await b.api.graphs();
    expect(bGraphs.map((g) => g.id)).not.toContain(aOcean.id);

    for (const [path, id] of [
      [(g: string) => `/v1/graphs/${g}`, aOcean.id],
      [(g: string) => `/v1/graphs/${g}/stats`, aOcean.id],
      [(g: string) => `/v1/graphs/${g}/atoms`, aOcean.id],
      [(g: string) => `/v1/graphs/${g}/sources`, aOcean.id],
      [(g: string) => `/v1/graphs/${g}/source-groups`, aOcean.id],
      [(g: string) => `/v1/nodes?graph_id=${g}`, aOcean.id],
      [(x: string) => `/v1/atoms/${x}`, atomId],
      [(x: string) => `/v1/graphs/${bOcean.id}/atoms/${x}`, atomId],
    ] as const) {
      await expectIndistinguishable(b.api, "GET", path, id, { secret });
    }
  });

  test("another user's ocean refuses writes and questions, and nothing lands in it", async ({ request }) => {
    const [a, b] = await Promise.all([personalUser(request, "iso-a"), personalUser(request, "iso-b")]);
    const aOcean = await a.api.personalGraph();
    const secret = `orchid-${crypto.randomUUID()}`;
    const bOcean = await b.api.personalGraph();
    const { sourceId } = await ingestNote(a.api, aOcean.id, `Project ${secret} ships in May.`);
    const before = await a.api.json("GET", `/v1/graphs/${aOcean.id}/sources`);

    // Write into A's ocean by id.
    await expectIndistinguishable(b.api, "POST", () => "/v1/ingest/atom", aOcean.id, {
      label: "POST /v1/ingest/atom into another user's ocean",
      data: (g) => ({
        graph_id: g,
        source_type: "note",
        source_id: "e2e-iso-forged",
        content: "planted by another account",
        participants: [],
        timestamp: new Date().toISOString(),
      }),
    });
    // Ask in A's ocean by id.
    await expectIndistinguishable(b.api, "POST", () => "/v1/queries", aOcean.id, {
      label: "POST /v1/queries against another user's ocean",
      data: (g) => ({ text: `When does project ${secret} ship?`, graph_id: g }),
      secret,
    });
    // Delete A's real source, anchored on A's ocean...
    await expectIndistinguishable(b.api, "DELETE", (g) => `/v1/graphs/${g}/sources?source_type=note&source_id=${sourceId}`, aOcean.id);
    // ...and anchored on B's OWN ocean, where the route deletes "every
    // matching atom across the caller's account": A's account is not B's.
    const own = await b.api.call("DELETE", `/v1/graphs/${bOcean.id}/sources?source_type=note&source_id=${sourceId}`);
    expect(own.status(), await own.text()).toBeLessThan(500);

    const after = await a.api.json("GET", `/v1/graphs/${aOcean.id}/sources`);
    expect(after, "A's ocean changed after B's refused writes").toEqual(before);
    expect(JSON.stringify(after)).not.toContain("planted by another account");
  });
});
