import { beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "./api";
import { conversationKey, conversationQueryOptions, type QueryCtx } from "./appQueries";
import {
  clearMessageFeedback,
  MessageFeedbackDisabled,
  putMessageFeedback,
  type MessageFeedback,
} from "./messageFeedbackApi";

const wire = vi.hoisted(() => ({ fetch: vi.fn() }));
vi.mock("./transport", () => ({ guardedFetch: wire.fetch }));

const conv = "11111111-1111-4111-8111-111111111111";
const msg = "22222222-2222-4222-8222-222222222222";
const stored: MessageFeedback = { vote: "down", reasons: ["wrong", "outdated"], note: "Q3 numbers changed" };

beforeEach(() => wire.fetch.mockReset().mockResolvedValue(new Response(JSON.stringify(stored))));

it("PUTs the vote to the message's feedback path with the bearer and returns the stored row", async () => {
  const out = await putMessageFeedback("tok", conv, msg, { vote: "down", reasons: ["outdated", "wrong"], note: "Q3 numbers changed" });
  expect(out).toEqual(stored);
  const [path, init] = wire.fetch.mock.calls[0];
  expect(path).toBe(`/v1/conversations/${conv}/messages/${msg}/feedback`);
  expect(init).toMatchObject({ method: "PUT", cache: "no-store", headers: { Authorization: "Bearer tok", "Content-Type": "application/json" } });
  expect(JSON.parse(init.body)).toEqual({ vote: "down", reasons: ["outdated", "wrong"], note: "Q3 numbers changed" });
});

it("sends empty reasons and a null note for a bare up vote", async () => {
  await putMessageFeedback("tok", conv, msg, { vote: "up" });
  expect(JSON.parse(wire.fetch.mock.calls[0][1].body)).toEqual({ vote: "up", reasons: [], note: null });
});

it("DELETEs the same path with no body", async () => {
  wire.fetch.mockResolvedValue(new Response(null, { status: 204 }));
  await expect(clearMessageFeedback("tok", conv, msg)).resolves.toBeUndefined();
  const [path, init] = wire.fetch.mock.calls[0];
  expect(path).toBe(`/v1/conversations/${conv}/messages/${msg}/feedback`);
  expect(init).toMatchObject({ method: "DELETE", headers: { Authorization: "Bearer tok" } });
  expect(init.body).toBeUndefined();
});

it("raises MessageFeedbackDisabled on 404 from either route", async () => {
  wire.fetch.mockImplementation(async () => new Response(JSON.stringify({ error: "not found" }), { status: 404 }));
  await expect(putMessageFeedback("tok", conv, msg, { vote: "up" })).rejects.toBeInstanceOf(MessageFeedbackDisabled);
  await expect(clearMessageFeedback("tok", conv, msg)).rejects.toBeInstanceOf(MessageFeedbackDisabled);
});

it("surfaces a 422 message and keeps other failures generic", async () => {
  wire.fetch.mockResolvedValueOnce(new Response(JSON.stringify({ error: "unknown reason \"bogus\"" }), { status: 422 }));
  await expect(putMessageFeedback("tok", conv, msg, { vote: "down" })).rejects.toMatchObject({ status: 422, message: "unknown reason \"bogus\"" });
  wire.fetch.mockResolvedValueOnce(new Response(JSON.stringify({ error: "internal error" }), { status: 500 }));
  const error = await putMessageFeedback("tok", conv, msg, { vote: "up" }).catch((e: unknown) => e);
  expect(error).toBeInstanceOf(ApiError);
  expect(error).not.toBeInstanceOf(MessageFeedbackDisabled);
  expect(error).toMatchObject({ status: 500, message: "Your feedback was not saved. Try again." });
});

it("refuses without a token and never calls the network", async () => {
  await expect(putMessageFeedback("", conv, msg, { vote: "up" })).rejects.toThrow("Not authenticated");
  expect(wire.fetch).not.toHaveBeenCalled();
});

it("invalidates the opened chat's query, which carries the stored vote", () => {
  const ctx: QueryCtx = { getAuthToken: () => "tok", scope: "user_1:org_1" };
  expect(conversationKey(ctx.scope, conv)).toEqual(conversationQueryOptions(ctx, conv).queryKey);
});
