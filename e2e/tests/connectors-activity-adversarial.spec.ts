import { waitForComputed } from "../lib/readModels";
import crypto from "node:crypto";
import { expect, test } from "../lib/fixtures";
import { addMember, createReadyOrg } from "../lib/isolation";

test("B5 same-account capture feed, counts and freshness isolate A and B", async ({ request }) => {
  test.setTimeout(180_000);
  const org = await createReadyOrg(request, "b5");
  const member = await addMember(request, org, "b5b");
  const clients = [org.adminInOrg, member.inOrg];
  const ids: string[][] = [];
  for (const [i, api] of clients.entries()) {
    const graph = (await api.graphs()).find((g) => g.kind === "member");
    expect(graph).toBeTruthy();
    const captured: string[] = [];
    for (let n = 0; n <= i; n++) {
      const result = await api.json("POST", "/v1/ingest/atom", {
        graph_id: graph!.id, source_type: "outlook", source_id: crypto.randomUUID(),
        content: `Private capture ${i}/${n}.`, participants: [], timestamp: new Date().toISOString(),
      });
      expect(result.atom_ids.length).toBe(1);
      captured.push(...result.atom_ids);
    }
    ids.push(captured);
  }
  for (const [i, api] of clients.entries()) {
    const feed = await api.json("GET", "/v1/connectors/microsoft/captures");
    expect(feed.items.map((x: any) => x.atom_id).sort()).toEqual(ids[i].sort());
    const detail = (await waitForComputed(api, "/v1/connectors/microsoft", true, p => p.sources.find((x: any) => x.key === "mail").week[6] === i+1)).payload;
    const list = (await waitForComputed(api, "/v1/connectors")).payload;
    const summary = list.connectors.find((x: any) => x.key === "microsoft");
    for (const value of [detail, summary]) {
      expect(value.last_captured_at).toBe(feed.items[0].captured_at);
      expect(value.sources.find((x: any) => x.key === "mail").week).toEqual([0,0,0,0,0,0,i+1]);
    }
    expect(detail.topics).toEqual([]);
    expect(feed.items.every((x: any) => x.topics.length === 0)).toBe(true);
  }
});
