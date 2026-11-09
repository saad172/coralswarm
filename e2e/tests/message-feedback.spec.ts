import crypto from "node:crypto";
import { expect, test } from "../lib/fixtures";

test("a down vote with reasons survives a reload, and nobody else can vote on the answer", async ({ makeUser }) => {
  const owner = await makeUser("feedback-owner", "Feedback Owner");
  const stranger = await makeUser("feedback-stranger", "Feedback Stranger");
  const graph = await owner.api.personalGraph();
  await owner.api.seedSource(
    graph.id,
    "The Halcyon launch moved to March 14 after the security review. Priya Raman owns the rollout checklist.",
  );

  const answer = await owner.api.json("POST", "/v1/queries", {
    text: "When is the Halcyon launch?",
    library_scope: { graph_ids: [graph.id] },
  });
  const conversation: string = answer.conversation_id;
  const message: string = answer.message_id;
  expect(conversation && message, JSON.stringify(answer).slice(0, 400)).toBeTruthy();
  const path = (c: string, m: string) => `/v1/conversations/${c}/messages/${m}/feedback`;

  const vote = { vote: "down", reasons: ["outdated", "wrong"], note: "The date moved again." };
  const stored = { vote: "down", reasons: ["wrong", "outdated"], note: "The date moved again." };
  expect(await owner.api.json("PUT", path(conversation, message), vote)).toEqual(stored);
  expect(await owner.api.json("PUT", path(conversation, message), vote)).toEqual(stored);

  const read = async () => {
    const detail = await owner.api.json("GET", `/v1/conversations/${conversation}`);
    return detail.messages as Array<{ id: string; role: string; feedback?: unknown }>;
  };
  const messages = await read();
  expect(messages.find((m) => m.id === message)?.feedback).toEqual(stored);
  const question = messages.find((m) => m.role === "user");
  expect(question?.feedback).toBeNull();

  expect((await owner.api.call("PUT", path(conversation, message), { vote: "down", reasons: ["bogus"] })).status()).toBe(422);
  expect((await owner.api.call("PUT", path(conversation, question!.id), { vote: "up" })).status()).toBe(422);
  expect((await read()).find((m) => m.id === message)?.feedback).toEqual(stored);

  const missing = await stranger.api.call("PUT", path(crypto.randomUUID(), crypto.randomUUID()), { vote: "up" });
  const refused = await stranger.api.call("PUT", path(conversation, message), { vote: "up" });
  expect(missing.status()).toBe(404);
  expect(refused.status()).toBe(404);
  expect(await refused.text()).toBe(await missing.text());
  expect((await stranger.api.call("DELETE", path(conversation, message))).status()).toBe(404);

  expect((await owner.api.call("DELETE", path(conversation, message))).status()).toBe(204);
  expect((await owner.api.call("DELETE", path(conversation, message))).status()).toBe(204);
  expect((await read()).find((m) => m.id === message)?.feedback).toBeNull();
});
