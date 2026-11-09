import { expect, test } from "../lib/fixtures";

test("the answer stream reports each stage, then one cited final", async ({ signedIn }) => {
  const graph = await signedIn.api.personalGraph();
  await signedIn.api.seedSource(
    graph.id,
    "The Halcyon launch moved to March 14 after the security review. Priya Raman owns the rollout checklist.",
  );

  const frames = await signedIn.api.askStream(graph.id, "When is the Halcyon launch?");
  expect(frames.map((f) => f.event)).toEqual(["progress", "progress", "progress", "progress", "meta", "final"]);
  expect(frames.slice(0, 4).map((f) => f.data.stage)).toEqual(["searching", "retrieved", "writing", "checking"]);
  expect(frames.slice(0, 4).every((f) => f.data.attempt === 1)).toBe(true);
  expect(frames[1].data.sources).toBeGreaterThan(0);

  const final = frames[5].data;
  expect(final.mode).toBe("synthesized");
  expect(final.answer).toMatch(/\[\d+\]/);
  expect(final.retained_citations.some((c: { quote: string }) => c.quote.includes("Halcyon launch moved to March 14"))).toBe(true);
  expect(frames[4].data.conversation_id).toBe(final.conversation_id);
  for (const f of frames.slice(0, 5)) expect(JSON.stringify(f.data)).not.toContain(final.answer.slice(0, 24));

  const refused = await signedIn.api.call("POST", "/v1/queries/stream", { text: "When?", library_scope: { graph_ids: [graph.id] }, effort: "high" });
  expect(refused.status()).toBe(400);
  expect(refused.headers()["content-type"]).toContain("application/json");
});
