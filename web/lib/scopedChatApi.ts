import { ApiError, parseSseFrame, type ConversationSummary } from "./api";
import { apiBase } from "./apiBase";
import { TransportError, type RequestPriority } from "./transport";
import { CLIENT_HEADER, clientName } from "./clientHeader";
import { matchingLibraryScope, normalizedLibraryScope, validateLibraryRequest, type LibraryCitation, type LibraryScope } from "./libraryMemoryApi";
import type { AnswerStyle, ModelTier } from "./askScope";
import { FEEDBACK_REASONS, type MessageFeedback } from "./messageFeedbackApi";

export interface ScopedOutcome {
  graph_id: string; semantic_atom_id: string; kind: string; statement: string;
  statement_truncated: boolean; source_item_id: string; source_version_id: string;
  source_version_is_current: boolean; occurred_at: string | null; captured_at: string;
  representation: string; confidence: number | null; synthetic_demo: boolean; citations: LibraryCitation[];
  /** Fact supersession (#1346): present only on a superseded fact, and only
   *  when the backend runs FACT_SUPERSESSION_MODE=live. Parse with
   *  `parseSupersession`. */
  supersession?: unknown;
}
export interface LegacyEvidence {
  kind: "legacy_chunk"; graph_id: string; atom_id: string;
  representation: "unmapped_legacy_chunk"; content: string; content_truncated: boolean;
  occurred_at: string | null; captured_at: string | null;
  /** Jev cite verdict (#1353) when this chunk was cited and checked. */
  jev_verdict?: string;
}
/** Chat find (docs/implementation/find-results-api.md): one tile per SOURCE. */
export type SourceResultKind = "meeting" | "slack" | "agent" | "doc" | "note";
export interface SourceResultQuote { quote: string; passage_id: string | null; occurred_at: string | null }
export interface SourceResultTopic { node_id: string; title: string }
export interface SourceResult {
  source_item_id: string;
  kind: SourceResultKind;
  /** "meeting" | "slack thread" | "agent session" | "doc session" | "note" */
  kind_label: string;
  title: string;
  /** One terse line (meeting summary's first sentence, else the first decision); null when none was stored. */
  outcome: string | null;
  /** RFC3339 UTC; format the day and time in the viewer's zone. */
  occurred_at: string | null;
  /** Recorded duration; meetings only. */
  duration_ms: number | null;
  /** At most 8 names; `people_count` is the total. */
  people: string[];
  people_count: number;
  graph: { id: string; name: string };
  /** Rank 1 of a find answer. Always false for title search. */
  best: boolean;
  decisions: string[];
  reefs: SourceResultTopic[];
  /** Passages of this source the answer cited; empty for title search. */
  quotes: SourceResultQuote[];
}
export interface ScopedAnswer {
  support_status: "complete"; mode: "synthesized" | "retrieval_only";
  answer: string | null; text: string; library_scope: LibraryScope;
  previous_library_scope: LibraryScope | null; scope_changed: boolean;
  retained_citations: LibraryCitation[]; outcomes: ScopedOutcome[]; legacy_evidence: LegacyEvidence[];
  /** `temporal_unresolved`: an inherited historical cutoff found records but no confident state; no prose is generated. */
  answer_status: "supported" | "no_evidence" | "temporal_unresolved"; disagreement_status: "not_resolved";
  truncated: boolean; truncation_reason: string | null; conversation_id: string;
  message_id: string; query_id: string; new_conversation: boolean; title: string;
  contains_synthetic_demo_evidence: boolean;
  egress_blocked: boolean;
  /** Jev pass report (#1353). Passed through but not displayed: the chat shows
   *  per-cite verdicts on the cards, not a turn-level summary. */
  jev?: unknown;
  /** Chat find (flag CHAT_FIND_RESULTS_ENABLED). Absent when the flag is off or
   *  the question is not a find question; `[]` for a find question with no
   *  readable cited source; otherwise 1-3 tiles, best first. Stored conversation
   *  messages can additionally supply freshly recertified tiles. */
  source_results?: SourceResult[];
}
export interface ScopedConversation extends ConversationSummary {
  library_scope: LibraryScope | null;
  scope_status: "resolved" | "unavailable" | "unscoped";
}
export interface ScopedMessage {
  id: string; seq?: number; role: "user" | "assistant"; text: string;
  /** `failed`: the ask for the question before this reply did not finish. No text, no evidence. */
  support_status?: "complete" | "unavailable" | "unknown" | "failed";
  mode: "synthesized" | "retrieval_only" | "failed" | null;
  /** On a `failed` reply: why (`db_busy`, `evidence_changed`, `conversation_changed`, `model_error`, `access_changed`, ...). */
  failure_reason?: string | null;
  /** On a `failed` reply: the saved question it answers, for Retry. */
  question_message_id?: string | null;
  library_scope?: LibraryScope | null; retained_citations?: LibraryCitation[];
  outcomes?: ScopedOutcome[]; legacy_evidence?: LegacyEvidence[];
  contains_synthetic_demo_evidence?: boolean | null;
  timing?: Record<string, unknown> | null;
  feedback?: MessageFeedback | null;
  /** Optional Find tiles recertified with this current admitted message. */
  source_results?: SourceResult[];
}
export interface ScopedConversationDetail {
  conversation: ScopedConversation; messages: ScopedMessage[]; messages_truncated?: boolean;
  /** Keyset cursor for the next older page; present only when `messages_truncated`. */
  older_before_seq?: number | null;
}
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const object = (v: unknown): v is Record<string, unknown> => !!v && typeof v === "object" && !Array.isArray(v);
const id = (v: unknown) => typeof v === "string" && UUID.test(v);
const sameId = (a: unknown, b: unknown) => id(a) && id(b) && String(a).toLowerCase() === String(b).toLowerCase();
const text = (v: unknown) => typeof v === "string";
const nullableText = (v: unknown) => v === null || text(v);
function validScope(v: unknown): v is LibraryScope {
  try { if (!object(v)) return false; validateLibraryRequest({ scope: v as unknown as LibraryScope }); return true; } catch { return false; }
}
function inScope(v: Record<string, unknown>, scope: LibraryScope): boolean {
  return scope.graph_ids.some((g) => g.toLowerCase() === String(v.graph_id).toLowerCase()) &&
    (!scope.source_item_id || sameId(v.source_item_id, scope.source_item_id)) &&
    (!scope.source_version_id || sameId(v.source_version_id, scope.source_version_id));
}
function validCitation(v: unknown, scope: LibraryScope): v is LibraryCitation {
  return object(v) && v.kind === "source_passage" &&
    ["graph_id", "source_item_id", "source_version_id", "passage_id", "semantic_atom_id"].every((k) => id(v[k])) &&
    inScope(v, scope) && text(v.quote) && Array.from(v.quote as string).length <= 4000 &&
    Number.isSafeInteger(v.start_char) && Number(v.start_char) >= 0 && Number.isSafeInteger(v.end_char) &&
    Number(v.end_char) > Number(v.start_char) &&
    Number(v.end_char) - Number(v.start_char) === Array.from(v.quote as string).length &&
    typeof v.quote_truncated === "boolean" && text(v.representation) && nullableText(v.occurred_at) && text(v.captured_at);
}
function validEvidence(v: Record<string, unknown>, scope: LibraryScope): boolean {
  return Array.isArray(v.retained_citations) && v.retained_citations.length <= 2000 && v.retained_citations.every((c) => validCitation(c, scope)) &&
    Array.isArray(v.outcomes) && v.outcomes.length <= 200 && v.outcomes.every((o) => object(o) &&
      ["graph_id", "semantic_atom_id", "source_item_id", "source_version_id"].every((k) => id(o[k])) && inScope(o, scope) &&
      ["fact", "decision", "action", "observation"].includes(String(o.kind)) && text(o.statement) &&
      typeof o.statement_truncated === "boolean" && typeof o.source_version_is_current === "boolean" &&
      (normalizedLibraryScope(scope).outcome_versions !== "current" || o.source_version_is_current === true) &&
      typeof o.synthetic_demo === "boolean" && nullableText(o.occurred_at) && text(o.captured_at) && text(o.representation) &&
      (o.confidence === null || (typeof o.confidence === "number" && Number.isFinite(o.confidence))) &&
      Array.isArray(o.citations) && o.citations.every((c) => validCitation(c, scope) && c.semantic_atom_id === o.semantic_atom_id && c.source_item_id === o.source_item_id && c.source_version_id === o.source_version_id && c.graph_id === o.graph_id)) &&
    Array.isArray(v.legacy_evidence) && v.legacy_evidence.length <= 200 && v.legacy_evidence.every((c) => object(c) &&
      c.kind === "legacy_chunk" && c.representation === "unmapped_legacy_chunk" && id(c.atom_id) && id(c.graph_id) &&
      scope.graph_ids.some((g) => sameId(g, c.graph_id)) && !scope.source_item_id && !scope.source_version_id &&
      !scope.service_id && !scope.connection && !scope.container && !scope.content_kind && !scope.collection &&
      text(c.content) && typeof c.content_truncated === "boolean" && nullableText(c.occurred_at) && nullableText(c.captured_at));
}
function validSourceResults(value: unknown, scope: LibraryScope): value is SourceResult[] {
  return Array.isArray(value) && value.length <= 3 && value.every((tile) => object(tile) && id(tile.source_item_id) &&
    ["meeting", "slack", "agent", "doc", "note"].includes(String(tile.kind)) && text(tile.kind_label) && text(tile.title) && nullableText(tile.outcome) &&
    nullableText(tile.occurred_at) && (tile.duration_ms === null || typeof tile.duration_ms === "number" && Number.isFinite(tile.duration_ms) && tile.duration_ms >= 0) &&
    Array.isArray(tile.people) && tile.people.length <= 8 && tile.people.every(text) && Number.isSafeInteger(tile.people_count) && Number(tile.people_count) >= 0 &&
    object(tile.graph) && id(tile.graph.id) && text(tile.graph.name) && scope.graph_ids.some((graph) => sameId(graph, (tile.graph as Record<string, unknown>).id)) &&
    (!scope.source_item_id || sameId(tile.source_item_id, scope.source_item_id)) && typeof tile.best === "boolean" &&
    Array.isArray(tile.decisions) && tile.decisions.every(text) && Array.isArray(tile.reefs) && tile.reefs.every((reef) => object(reef) && id(reef.node_id) && text(reef.title)) &&
    Array.isArray(tile.quotes) && tile.quotes.every((quote) => object(quote) && text(quote.quote) && (quote.passage_id === null || id(quote.passage_id)) && nullableText(quote.occurred_at)));
}
export function validateScopedAnswer(v: unknown, expected: LibraryScope, style: AnswerStyle): ScopedAnswer {
  if (!object(v) || v.support_status !== "complete" || !matchingLibraryScope(v.library_scope, expected) ||
    !["synthesized", "retrieval_only"].includes(String(v.mode)) || !text(v.text) || !nullableText(v.answer) ||
    !id(v.conversation_id) || !id(v.message_id) || !id(v.query_id) || !text(v.title) ||
    typeof v.egress_blocked !== "boolean" || typeof v.contains_synthetic_demo_evidence !== "boolean" || typeof v.scope_changed !== "boolean" || typeof v.new_conversation !== "boolean" ||
    !(v.previous_library_scope === null || validScope(v.previous_library_scope)) ||
    !["supported", "no_evidence", "temporal_unresolved"].includes(String(v.answer_status)) || v.disagreement_status !== "not_resolved" ||
    (v.answer_status === "temporal_unresolved" && (v.text !== "" || v.answer !== null || !object(v.temporal_state) ||
      !["unknown", "incomplete", "unsupported"].includes(String(v.temporal_state.status)))) ||
    (v.mode === "retrieval_only" && (v.text !== "" || v.answer !== null)) ||
    (v.mode === "synthesized" && (typeof v.answer !== "string" || v.answer !== v.text)) ||
    typeof v.truncated !== "boolean" || !nullableText(v.truncation_reason) ||
    !validEvidence(v, expected) || (v.source_results !== undefined && !validSourceResults(v.source_results, expected)) || (style === "sources-only" && (v.mode !== "retrieval_only" || v.text !== "" || v.answer !== null)) ||
    (v.answer_status === "no_evidence" && (v.text !== "" || (v.retained_citations as unknown[]).length || (v.outcomes as unknown[]).length || (v.legacy_evidence as unknown[]).length)))
    throw new Error("The answer or its saved scope could not be verified. No answer was displayed.");
  return v as unknown as ScopedAnswer;
}
const unreachable = (path: string) => new TransportError("network", path, "The chat could not be reached. Retry when the connection is restored.");
const refusal = (status: number, body: Record<string, unknown> | null) =>
  new ApiError(status === 403 || status === 404 ? "This chat or scope is unavailable." : status === 401 ? "Sign in again to verify this chat." : "The chat could not be verified. Retry when the connection is restored.", status, null, null, null, body);
async function send(token: string, path: string, signal: AbortSignal, body?: unknown, priority: RequestPriority = "normal"): Promise<Response> {
  // Fixed credential, no retry, no global bearer refresh, no replay cache.
  let response: Response;
  try {
    response = await fetch(`${apiBase()}${path}`, { method: body === undefined ? "GET" : "POST", cache: "no-store", signal,
      headers: { Authorization: `Bearer ${token}`, [CLIENT_HEADER]: clientName(), "x-coralswarm-priority": priority, ...(body === undefined ? {} : { "Content-Type": "application/json" }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }) });
  } catch (error) {
    // A rejected fetch is the network, not a refusal: typed so revalidation keeps what is on screen.
    if (signal.aborted) throw error;
    throw unreachable(path);
  }
  if (!response.ok) throw refusal(response.status, await response.json().then((v: unknown) => (object(v) ? v : null), () => null));
  return response;
}
async function request(token: string, path: string, signal: AbortSignal, body?: unknown, priority?: RequestPriority): Promise<unknown> {
  return (await send(token, path, signal, body, priority)).json();
}
/** What a failed ask tells the client: why, and the saved question to retry
 * against. Only the server's own failure body counts; any other error (a lost
 * connection, a refusal before the question was saved) keeps its old handling. */
export interface AskFailure { reason: string; conversationId: string | null; questionMessageId: string | null }
export function askFailure(error: unknown): AskFailure | null {
  if (!(error instanceof ApiError)) return null;
  const body = error.body;
  if (object(body) && typeof body.reason === "string" && body.reason.length <= 64) {
    return { reason: body.reason, conversationId: id(body.conversation_id) ? String(body.conversation_id) : null,
      questionMessageId: id(body.question_message_id) ? String(body.question_message_id) : null };
  }
  return null;
}
export interface AskArgs { text: string; scope: LibraryScope; inherit?: boolean; replace?: boolean; conversationId?: string | null; retryMessageId?: string | null; model: ModelTier; style: AnswerStyle }
function askBody(args: AskArgs) {
  validateLibraryRequest({ scope: args.scope });
  if (!args.text.trim() || args.text.length > 10000) throw new Error("Enter a question of up to 10,000 characters.");
  return {
    text: args.text.trim(), style: args.style, ...(args.style !== "sources-only" ? { model: args.model } : {}),
    ...(args.conversationId ? { conversation_id: args.conversationId } : {}),
    ...(args.retryMessageId && args.conversationId ? { retry_message_id: args.retryMessageId } : {}),
    ...(!args.inherit ? { library_scope: normalizedLibraryScope(args.scope) } : {}),
    ...(args.replace ? { scope_action: "replace" } : {}),
  };
}
function verifiedAnswer(result: unknown, args: AskArgs): ScopedAnswer {
  const answer = validateScopedAnswer(result, args.scope, args.style);
  if (args.conversationId && !sameId(answer.conversation_id, args.conversationId)) throw new Error("The answer belongs to a different chat.");
  return answer;
}
export async function askScoped(token: string, args: AskArgs, signal: AbortSignal): Promise<ScopedAnswer> {
  const body = askBody(args);
  return verifiedAnswer(await request(token, "/v1/queries", signal, body), args);
}

export type ScopedProgress =
  | { stage: "searching" | "writing" | "checking"; attempt: number }
  | { stage: "retrieved"; sources: number; attempt: number };
function progressOf(v: unknown): ScopedProgress | null {
  if (!object(v) || !Number.isSafeInteger(v.attempt) || Number(v.attempt) < 1) return null;
  const attempt = Number(v.attempt);
  if (v.stage === "retrieved") return Number.isSafeInteger(v.sources) && Number(v.sources) >= 0 ? { stage: "retrieved", sources: Number(v.sources), attempt } : null;
  return v.stage === "searching" || v.stage === "writing" || v.stage === "checking" ? { stage: v.stage, attempt } : null;
}
export async function askScopedStream(token: string, args: AskArgs, signal: AbortSignal, onProgress: (progress: ScopedProgress) => void): Promise<ScopedAnswer> {
  const path = "/v1/queries/stream";
  const response = await send(token, path, signal, askBody(args));
  if (!response.body) throw unreachable(path);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  const handle = (frame: string): ScopedAnswer | null => {
    signal.throwIfAborted();
    const sse = parseSseFrame(frame);
    if (sse?.event === "progress") {
      const progress = progressOf(sse.data);
      if (progress) onProgress(progress);
    } else if (sse?.event === "final") {
      return verifiedAnswer(sse.data, args);
    } else if (sse?.event === "error") {
      const body = object(sse.data) ? sse.data : null;
      throw refusal(Number.isSafeInteger(body?.status) ? Number(body?.status) : 500, body);
    }
    return null;
  };
  try {
    for (;;) {
      signal.throwIfAborted();
      let chunk: ReadableStreamReadResult<Uint8Array>;
      try {
        chunk = await reader.read();
      } catch (error) {
        if (signal.aborted) throw error;
        throw unreachable(path);
      }
      signal.throwIfAborted();
      buffer += decoder.decode(chunk.value, { stream: !chunk.done });
      let end: number;
      while ((end = buffer.indexOf("\n\n")) !== -1) {
        const answer = handle(buffer.slice(0, end));
        buffer = buffer.slice(end + 2);
        if (answer) return answer;
      }
      if (chunk.done) {
        const answer = buffer.trim() ? handle(buffer) : null;
        if (answer) return answer;
        throw unreachable(path);
      }
    }
  } finally {
    reader.cancel().catch(() => {});
  }
}
const seqOk = (v: unknown, below: number | null, min = 0) => Number.isSafeInteger(v) && Number(v) >= min && (below === null || Number(v) < below);
/** One page of a chat: the newest page, or the page older than `beforeSeq`.
 * Each page is read through the full server reader/scope/receipt checks. */
export async function readScopedConversation(token: string, conversationId: string, signal: AbortSignal, beforeSeq: number | null = null, priority: RequestPriority = "normal"): Promise<ScopedConversationDetail> {
  if (!id(conversationId)) throw new Error("This chat link is invalid.");
  if (beforeSeq !== null && !seqOk(beforeSeq, null, 1)) throw new Error("This chat's history cursor is invalid.");
  const v = await request(token, `/v1/conversations/${conversationId}${beforeSeq === null ? "" : `?before_seq=${beforeSeq}`}`, signal, undefined, priority);
  return parseScopedConversationDetail(v, conversationId, beforeSeq);
}

/** The same receipt/scope validation serves indexed Stored and interactive pages. */
export function parseScopedConversationDetail(v: unknown, conversationId: string, beforeSeq: number | null = null): ScopedConversationDetail {
  if (!object(v) || !object(v.conversation) || !sameId(v.conversation.id, conversationId) || !text(v.conversation.title) ||
    !["resolved", "unavailable", "unscoped"].includes(String(v.conversation.scope_status)) ||
    (v.conversation.scope_status === "resolved" ? !validScope(v.conversation.library_scope) : v.conversation.library_scope !== null) ||
    !Array.isArray(v.messages) || v.messages.length > 200) throw new Error("This chat's saved scope could not be verified.");
  const cursor = v.older_before_seq ?? null;
  if (v.messages_truncated === true ? !seqOk(cursor, beforeSeq, 1) || !sameSeq(v.messages[0], cursor) : cursor !== null) throw new Error("This chat's history could not be verified.");
  for (const m of v.messages) {
    if (!object(m) || !id(m.id) || !["user", "assistant"].includes(String(m.role)) || !text(m.text) || !seqOk(m.seq, beforeSeq)) throw new Error("This saved chat could not be verified.");
    if (m.source_results !== undefined && (m.role !== "assistant" || m.support_status !== "complete" || !validScope(m.library_scope) || !validSourceResults(m.source_results, m.library_scope)))
      throw new Error("The saved Find results could not be verified.");
    if (m.role === "assistant" && m.support_status === "failed" && (m.mode !== "failed" || m.text !== "" || !(m.failure_reason === null || text(m.failure_reason)) || !(m.question_message_id === null || id(m.question_message_id))))
      throw new Error("The saved failure record could not be verified.");
    if (m.feedback !== undefined && m.feedback !== null && (m.role !== "assistant" || !validFeedback(m.feedback)))
      throw new Error("The saved answer feedback could not be verified.");
    if (m.role === "assistant" && m.support_status === "complete" &&
      (typeof m.contains_synthetic_demo_evidence !== "boolean" || !validScope(m.library_scope) || !validEvidence(m, m.library_scope) || !["synthesized", "retrieval_only"].includes(String(m.mode)) || (m.mode === "retrieval_only" && m.text !== "")))
      throw new Error("The saved answer's evidence could not be verified.");
  }
  return v as unknown as ScopedConversationDetail;
}
function validFeedback(v: unknown): v is MessageFeedback {
  return object(v) && (v.vote === "up" || v.vote === "down") && Array.isArray(v.reasons) &&
    v.reasons.every((r) => (FEEDBACK_REASONS as readonly unknown[]).includes(r)) && (v.note === null || text(v.note));
}
function sameSeq(m: unknown, seq: unknown) { return object(m) && m.seq === seq; }
export const sameSavedScope = (a: ScopedConversation, b: ScopedConversation) =>
  a.scope_status === b.scope_status && (a.library_scope === null ? b.library_scope === null : !!b.library_scope && matchingLibraryScope(a.library_scope, b.library_scope));
/** Re-reads a chat down to `reachSeq` (the oldest message already shown), so a
 * refresh revalidates every loaded page instead of trusting earlier reads. */
export async function readScopedThread(token: string, conversationId: string, signal: AbortSignal, reachSeq: number | null, priority: RequestPriority = "normal"): Promise<ScopedConversationDetail> {
  const newest = await readScopedConversation(token, conversationId, signal, null, priority);
  const messages = [...newest.messages];
  let page = newest;
  while (reachSeq !== null && page.messages_truncated === true && page.older_before_seq != null && page.older_before_seq > reachSeq) {
    page = await readScopedConversation(token, conversationId, signal, page.older_before_seq, priority);
    if (!sameSavedScope(newest.conversation, page.conversation)) throw new Error("This chat's saved scope changed while its history was verified.");
    messages.unshift(...page.messages);
  }
  return { ...newest, messages, messages_truncated: page.messages_truncated === true, older_before_seq: page.messages_truncated === true ? page.older_before_seq ?? null : null };
}
export async function listScopedConversations(token: string, signal: AbortSignal): Promise<ConversationSummary[]> {
  const v = await request(token, "/v1/conversations", signal);
  if (!object(v) || !Array.isArray(v.conversations) || !Array.isArray(v.shared) || v.conversations.length > 200 || v.shared.length > 200 ||
    [...v.conversations, ...v.shared].some((c) => !object(c) || !id(c.id) || !text(c.title))) throw new Error("Recent chats could not be verified.");
  return [...v.conversations, ...v.shared] as ConversationSummary[];
}

export async function deleteScopedConversation(token: string, conversationId: string, signal: AbortSignal): Promise<void> {
  if (!id(conversationId)) throw new Error("This chat link is invalid.");
  const response = await fetch(`${apiBase()}/v1/conversations/${conversationId}`, { method: "DELETE", cache: "no-store", signal,
    headers: { Authorization: `Bearer ${token}`, [CLIENT_HEADER]: clientName() } });
  if (!response.ok) throw new ApiError("This chat could not be deleted. Verify the current account and retry.", response.status);
}

/** Resolution supplies freshly checked selector context only. It is never an
 * authorization token; Ask revalidates every object again on every question. */
export async function resolveScopedChatScope(token: string, scope: LibraryScope, signal: AbortSignal): Promise<LibraryScope> {
  validateLibraryRequest({ scope });
  const value = await request(token, "/v1/library/resolve-scope", signal, { scope: normalizedLibraryScope(scope) });
  if (!object(value) || !matchingLibraryScope(value.scope, scope)) throw new Error("This chat selection could not be verified; its selectors were not widened.");
  return normalizedLibraryScope(value.scope as LibraryScope);
}
