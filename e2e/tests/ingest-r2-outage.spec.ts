import crypto from "node:crypto";
import { expect, test } from "../lib/fixtures";
import { r2Outage } from "../lib/api";

// ADR-0016 §4: a failed R2 upload aborts the capture. The caller gets a
// retryable 503 and nothing is stored: no source, no coral. It used to answer
// 200 and drop the coral. Once R2 is back, the identical request lands.
test("an R2 outage stores nothing and answers a retryable error", async ({ makeUser, request }) => {
  const { api } = await makeUser("r2-outage");
  const ocean = await api.personalGraph();
  const sourceId = `e2e-r2-outage-${crypto.randomUUID()}`;
  const note = {
    graph_id: ocean.id,
    source_type: "note",
    source_id: sourceId,
    content: "The Halden rollout waits until the object store is back.",
    participants: [],
    timestamp: new Date().toISOString(),
  };
  const stored = async () => {
    const sources = await api.json("GET", `/v1/sources/items?graph_id=${ocean.id}&source_type=note`);
    const corals = (await api.json("GET", `/v1/graphs/${ocean.id}/atoms?page_size=200`)) as { source_id: string }[];
    return {
      sources: (sources.items ?? []).filter((i: { source_id: string }) => i.source_id === sourceId).length,
      corals: corals.filter((a) => a.source_id.startsWith(`${sourceId}:chunk:`)).length,
    };
  };

  await r2Outage(request, ocean.id, true);
  try {
    const res = await api.call("POST", "/v1/ingest/atom", note);
    const body = await res.json();
    expect(res.status(), JSON.stringify(body)).toBe(503);
    expect(body).toMatchObject({
      error: "object storage is unavailable; nothing was stored, retry ingest",
      code: "storage_unavailable",
    });
    expect(typeof body.cleanup_status).toBe("string");
    expect(await stored()).toEqual({ sources: 0, corals: 0 });
  } finally {
    await r2Outage(request, ocean.id, false);
  }

  const landed = await api.json("POST", "/v1/ingest/atom", note);
  expect(landed.atom_ids).toHaveLength(1);
  expect(landed.jobs_enqueued).toBe(1);
  expect(await stored()).toEqual({ sources: 1, corals: 1 });
});
