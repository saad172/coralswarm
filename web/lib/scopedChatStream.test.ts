import { afterEach, expect, it, vi } from "vitest";
import { ApiError } from "./api";
import { askFailure, askScoped, askScopedStream, type AskArgs, type ScopedProgress } from "./scopedChatApi";
import { TransportError } from "./transport";

const graph = "11111111-1111-4111-8111-111111111111";
const source = "22222222-2222-4222-8222-222222222222";
const version = "33333333-3333-4333-8333-333333333333";
const passage = "44444444-4444-4444-8444-444444444444";
const atom = "55555555-5555-4555-8555-555555555555";
const conversation = "66666666-6666-4666-8666-666666666666";
const question = "77777777-7777-4777-8777-777777777777";
const scope = { graph_ids: [graph], outcome_versions: "all_retained" as const };
const citation = { kind: "source_passage", graph_id: graph, source_item_id: source, source_version_id: version, passage_id: passage, semantic_atom_id: atom,
  quote: "Halcyon", start_char: 0, end_char: 7, quote_truncated: false, representation: "exported_text", occurred_at: null, captured_at: "2026-10-11T12:00:00Z" };
const answer = { mode: "synthesized", support_status: "complete", answer: "Halcyon chose annual pricing [1].", text: "Halcyon chose annual pricing [1].", library_scope: scope,
  previous_library_scope: null, scope_changed: true, retained_citations: [citation], outcomes: [], legacy_evidence: [],
  answer_status: "supported", disagreement_status: "not_resolved", truncated: false, truncation_reason: null,
  conversation_id: conversation, message_id: atom, query_id: passage, new_conversation: true, title: "Question", contains_synthetic_demo_evidence: false, egress_blocked: false };
const tile = { source_item_id: source, kind: "meeting", kind_label: "meeting", title: "Halcyon pricing", outcome: null, occurred_at: null, duration_ms: null,
  people: [], people_count: 0, graph: { id: graph, name: "GTM" }, best: true, decisions: [], reefs: [], quotes: [] };
const args: AskArgs = { text: " What did Halcyon decide? ", scope, model: "smart", style: "concise" };
const frame = (event: string, data: unknown) => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
const stages = [
  frame("progress", { stage: "searching", attempt: 1 }),
  frame("progress", { stage: "retrieved", sources: 3, attempt: 1 }),
  frame("progress", { stage: "writing", attempt: 1 }),
  frame("progress", { stage: "checking", attempt: 1 }),
];
const meta = frame("meta", { conversation_id: conversation, new_conversation: true, request_id: "r1" });

function sse(chunks: string[], init?: ResponseInit) {
  const bytes = new TextEncoder();
  return new Response(new ReadableStream({ start(c) { for (const chunk of chunks) c.enqueue(bytes.encode(chunk)); c.close(); } }),
    { headers: { "content-type": "text/event-stream" }, ...init });
}
function serve(response: Response) {
  const fetcher = vi.fn().mockResolvedValue(response);
  vi.stubGlobal("fetch", fetcher);
  return fetcher;
}
async function collect(signal = new AbortController().signal) {
  const seen: ScopedProgress[] = [];
  const result = await askScopedStream("tok", args, signal, (p) => seen.push(p)).catch((e: unknown) => e);
  return { seen, result };
}
afterEach(() => vi.unstubAllGlobals());

it("posts the askScoped body to the stream and reports each stage before the verified final", async () => {
  const joined = [...stages, meta, frame("final", answer)].join("");
  const fetcher = serve(sse([joined.slice(0, 30), joined.slice(30, 95), joined.slice(95)]));
  const { seen, result } = await collect();
  expect(fetcher.mock.calls[0][0]).toMatch(/\/v1\/queries\/stream$/);
  const init = fetcher.mock.calls[0][1];
  expect(init).toMatchObject({ method: "POST", cache: "no-store", headers: { Authorization: "Bearer tok" } });
  expect(JSON.parse(init.body)).toEqual({ text: "What did Halcyon decide?", style: "concise", model: "smart", library_scope: scope });
  expect(seen).toEqual([
    { stage: "searching", attempt: 1 },
    { stage: "retrieved", sources: 3, attempt: 1 },
    { stage: "writing", attempt: 1 },
    { stage: "checking", attempt: 1 },
  ]);
  expect(result).toEqual(answer);
});

it("reports a higher attempt so the caller can reset its stage line", async () => {
  serve(sse([...stages.slice(0, 2), frame("progress", { stage: "searching", attempt: 2 }), meta, frame("final", answer)]));
  const { seen } = await collect();
  expect(seen.map((p) => [p.stage, p.attempt])).toEqual([["searching", 1], ["retrieved", 1], ["searching", 2]]);
});

it("ignores malformed progress and keeps source_results on final", async () => {
  serve(sse([frame("progress", { stage: "guessing", attempt: 1 }), frame("progress", { stage: "retrieved", attempt: 1 }), frame("progress", { stage: "writing", attempt: 0 }),
    meta, frame("final", { ...answer, source_results: [tile] })]));
  const { seen, result } = await collect();
  expect(seen).toEqual([]);
  expect(result).toMatchObject({ source_results: [tile] });
  serve(sse([meta, frame("final", answer)]));
  expect((await collect()).result).not.toHaveProperty("source_results");
});

it("refuses a final that fails the askScoped validation", async () => {
  serve(sse([meta, frame("final", { ...answer, library_scope: { graph_ids: [source] } })]));
  expect((await collect()).result).toMatchObject({ message: expect.stringContaining("could not be verified") });
});

it.each([402, 409, 429, 502])("throws for an error event with status %i what askScoped throws for that HTTP status", async (status) => {
  const body = { error: "scoped ask failed", reason: "model_error", retryable: true, conversation_id: conversation, question_message_id: question, failed_message_id: atom };
  serve(new Response(JSON.stringify(body), { status }));
  const plain = await askScoped("tok", args, new AbortController().signal).catch((e: unknown) => e);
  serve(sse([...stages, frame("error", { ...body, status, code: "model_error" })]));
  const { result } = await collect();
  expect(result).toBeInstanceOf(ApiError);
  expect(plain).toBeInstanceOf(ApiError);
  expect(result).toMatchObject({ status, message: (plain as ApiError).message });
  expect(askFailure(result)).toEqual(askFailure(plain));
  expect(askFailure(result)).toEqual({ reason: "model_error", conversationId: conversation, questionMessageId: question });
});

it("takes the askScoped path for a refusal before the stream opens", async () => {
  serve(new Response(JSON.stringify({ error: "cloud Library Ask does not accept depth or effort controls" }), { status: 400 }));
  expect((await collect()).result).toMatchObject({ status: 400 });
});

it("treats a stream that ends without final as unreachable, not as an answer", async () => {
  serve(sse([...stages, meta]));
  expect((await collect()).result).toBeInstanceOf(TransportError);
});

it("rejects with the abort, not a transport error, when the caller aborts mid-stream", async () => {
  const controller = new AbortController();
  vi.stubGlobal("fetch", vi.fn((_url: string, init: RequestInit) => {
    const body = new ReadableStream({
      start(c) {
        c.enqueue(new TextEncoder().encode(stages[0]));
        init.signal?.addEventListener("abort", () => c.error(new DOMException("aborted", "AbortError")));
      },
    });
    return Promise.resolve(new Response(body));
  }));
  const seen: ScopedProgress[] = [];
  const result = await askScopedStream("tok", args, controller.signal, (p) => { seen.push(p); controller.abort(); }).catch((e: unknown) => e);
  expect(seen).toEqual([{ stage: "searching", attempt: 1 }]);
  expect(result).toBeInstanceOf(DOMException);
  expect((result as DOMException).name).toBe("AbortError");
});

it.each([true, false])("honors abort before buffered final (terminated: %s)", async (terminated) => {
  const controller = new AbortController();
  const final = frame("final", answer);
  serve(sse([stages[0] + stages[1] + (terminated ? final : final.trimEnd())]));
  const seen: ScopedProgress[] = [];
  await expect(askScopedStream("tok", args, controller.signal, (p) => {
    seen.push(p);
    controller.abort();
  })).rejects.toMatchObject({ name: "AbortError" });
  expect(seen).toEqual([{ stage: "searching", attempt: 1 }]);
});
