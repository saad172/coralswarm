"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { useReturnRefetch } from "@/lib/useReturnRefetch";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { conversationKey, conversationQueryOptions, conversationsQueryOptions } from "@/lib/appQueries";
import { matchingLibraryScope, normalizedLibraryScope, validateLibraryRequest, type LibraryScope } from "@/lib/libraryMemoryApi";
import { askFailure, askScopedStream, readScopedConversation, readScopedThread, resolveScopedChatScope, sameSavedScope, type ScopedAnswer, type ScopedConversation, type ScopedConversationDetail, type ScopedMessage } from "@/lib/scopedChatApi";
import { defaultChatScope, readChatScope } from "@/lib/scopedChatScope";
import { DEFAULT_EFFORT, DEFAULT_MODEL, type AnswerStyle, type ModelTier } from "@/lib/askScope";
import type { ConversationDetailPayload } from "@/lib/pageSectionTypes/conversationDetail";
import { StoredCertificateError, pageSectionPath, type Stored } from "@/lib/pageSectionWire";
import { authorizedPageRead, pageReadAuthorityRevision } from "@/lib/pageReadAuthority";
import { ApiError, transientFailure } from "@/lib/api";
import { beginCriticalRead } from "@/lib/transport";
import { emptyAssistant, type AssistantTurn, type Turn } from "./types";
import { emitConversationsChanged } from "@/lib/conversationsChanged";
import { useConversationDeletions } from "@/lib/useConversationDeletions";

interface Snapshot {
  readComputedAt?: string | null;
  readVersion?: ConversationDetailPayload; readError?: Error | null;
  routeAdmission: string; routeParams: string | undefined; routeId: string | null;
  key: string; scope: LibraryScope | null; savedScope: LibraryScope | null;
  activeId: string | null; conv: ScopedConversation | null; messages: Turn[];
  busy: boolean; loading: boolean; error: string | null;
  truncated: boolean; scopeUnavailable: boolean; foreign: boolean; scopeVerified: boolean;
  /** Keyset cursor for the next older page, and the oldest seq loaded through paging. */
  olderCursor: number | null; reachSeq: number | null; loadingOlder: boolean;
}
const history = (detail: Pick<ScopedConversationDetail, "messages_truncated" | "older_before_seq">) => ({ truncated: detail.messages_truncated === true,
  olderCursor: detail.messages_truncated === true ? detail.older_before_seq ?? null : null });
type ReceiptExtras = Pick<AssistantTurn, "prompt" | "elapsedMs" | "reveal" | "egressBlocked" | "empty" | "evidenceTruncated" | "evidenceTruncationReason">;
const receiptFingerprint = (turn: AssistantTurn) => JSON.stringify([turn.text, turn.mode, turn.libraryScope, turn.retainedCitations, turn.scopedOutcomes, turn.legacyEvidence, turn.syntheticDemo]);
type GraphName = { id: string; name: string };
function answerTurn(answer: Pick<ScopedAnswer, "text" | "mode" | "library_scope" | "retained_citations" | "outcomes" | "legacy_evidence" | "contains_synthetic_demo_evidence">, key: string, prompt: string, graphs: GraphName[]): AssistantTurn {
  return { ...emptyAssistant(key, prompt), pending: false, text: answer.text, mode: answer.mode,
    retainedCitations: answer.retained_citations, scopedOutcomes: answer.outcomes, legacyEvidence: answer.legacy_evidence,
    graphNames: Object.fromEntries(graphs.map((g) => [g.id, g.name])),
    libraryScope: answer.library_scope, supportStatus: "complete", syntheticDemo: answer.contains_synthetic_demo_evidence,
  };
}
export function scopedMessageTurn(message: ScopedMessage, scope: LibraryScope | null, graphs: GraphName[] = []): Turn {
  if (message.role === "user") return { key: message.id, role: "user", text: message.text };
  // A recorded failed ask: nothing to verify, nothing to show but why and a way to re-ask.
  if (message.support_status === "failed" && message.mode === "failed")
    return { ...emptyAssistant(message.id, ""), pending: false, supportStatus: "failed",
      failure: { reason: message.failure_reason || "unknown", questionMessageId: message.question_message_id || null } };
  if (message.support_status !== "complete" || !message.library_scope || !scope || !matchingLibraryScope(message.library_scope, scope) || !message.retained_citations || !message.outcomes || !message.legacy_evidence || (message.mode !== "synthesized" && message.mode !== "retrieval_only") || typeof message.contains_synthetic_demo_evidence !== "boolean")
    return { ...emptyAssistant(message.id, ""), pending: false, error: "This earlier answer is unavailable in the current verified scope.", supportStatus: message.support_status || "unknown" };
  return { feedback: message.feedback, ...answerTurn({ text: message.text, mode: message.mode, library_scope: message.library_scope,
    retained_citations: message.retained_citations, outcomes: message.outcomes, legacy_evidence: message.legacy_evidence,
    contains_synthetic_demo_evidence: message.contains_synthetic_demo_evidence ?? false }, message.id, "", graphs), sourceResults: message.source_results };
}


/** Cloud Chats reads and questions use one frozen live identity and exact scope.
 * The conversation list and each opened thread are identity-scoped cache
 * entries (lib/appQueries.ts): a reopen paints the last verified read at once
 * and re-reads it in the background. No answer is admitted from timing. */
export function useChatThread(conversationId: string | null, paramsText?: string) {
  const { key: admissionKey, ready: admissionReady, getToken, graphs, queryScope } = useEvidenceAdmission();
  const qc = useQueryClient();
  // Joined so a refetched-but-identical graph list does not reset the thread.
  const readableGraphIds = graphs.map((g) => g.id).join(",");
  const initial = useMemo(() => {
    try { return { scope: readChatScope(new URLSearchParams(paramsText || ""), readableGraphIds ? readableGraphIds.split(",") : []), error: null }; }
    catch (error) { return { scope: null, error: error instanceof Error ? error.message : "This chat scope is invalid." }; }
  }, [paramsText, readableGraphIds]);
  const key = JSON.stringify([admissionKey, conversationId, paramsText]);
  const blank = useCallback((): Snapshot => ({ key, routeAdmission: admissionKey, routeParams: paramsText, routeId: conversationId, scope: initial.scope, savedScope: null, activeId: conversationId, conv: null,
    messages: [], busy: false, loading: !!conversationId, error: initial.error, truncated: false, scopeUnavailable: false, foreign: false, scopeVerified: !conversationId && !!initial.scope && !initial.error,
    olderCursor: null, reachSeq: null, loadingOlder: false }), [key, initial, conversationId, admissionKey, paramsText]);
  const [snapshot, setSnapshot] = useState<Snapshot>(blank);
  const listing = useQuery({ ...conversationsQueryOptions({ getAuthToken: getToken, scope: queryScope }), enabled: admissionReady });
  const deleted = useConversationDeletions(queryScope);
  // Live receipt decorations are separate from the certified cache. A
  // refusal/withholding/scope change clears them before this render projects
  // messages. Find cards belong to the current wire message, never this map.
  const receipts = useRef(new Map<string, { fingerprint: string; extras: ReceiptExtras }>());
  const receiptGeneration = useRef("");
  const decorateReceipt = useCallback((turn: Turn): Turn => {
    if (turn.role !== "assistant") return turn;
    const id = turn.key.toLowerCase();
    const receipt = receipts.current.get(id);
    if (!receipt) return turn;
    if (turn.supportStatus !== "complete" || turn.error || receipt.fingerprint !== receiptFingerprint(turn)) {
      receipts.current.delete(id); return turn;
    }
    return { ...turn, ...receipt.extras };
  }, []);
  const liveCertificate = useRef<string | null>(null);
  const detail = useQuery({ ...conversationQueryOptions({ getAuthToken: getToken, scope: queryScope }, conversationId || ""),
    enabled: admissionReady && !!conversationId && !initial.error });
  // Track actual envelope updates even when structural sharing retains an
  // identical selected payload under a later computed_at certificate.
  void detail.dataUpdatedAt;
  const certificate = conversationId ? qc.getQueryData<Stored<ConversationDetailPayload>>(conversationKey(queryScope, conversationId))?.computed_at ?? null : null;
  liveCertificate.current = certificate;
  const authority = conversationId ? pageReadAuthorityRevision(qc, JSON.stringify([queryScope, pageSectionPath("conversation_detail", { id: conversationId })])) : 0;
  const adoption = useRef<{ from: string; to: string } | null>(null);
  let adoptsOwnId = false;
  if (snapshot.key !== key && snapshot.routeAdmission === admissionKey && admissionReady && conversationId &&
      snapshot.activeId === conversationId && snapshot.conv?.id === conversationId && snapshot.scopeVerified && !snapshot.busy &&
      (snapshot.routeId === null || snapshot.routeId === conversationId) && snapshot.scope && snapshot.savedScope && matchingLibraryScope(snapshot.scope, snapshot.savedScope)) {
    const previous = new URLSearchParams(snapshot.routeParams ?? "");
    previous.set("id", conversationId); previous.delete("library_scope"); previous.sort();
    const next = new URLSearchParams(paramsText ?? ""); next.sort();
    adoptsOwnId = previous.toString() === next.toString();
  }
  if (adoptsOwnId) adoption.current = { from: snapshot.key, to: key };
  const generationKey = JSON.stringify([key, authority]);
  if (receiptGeneration.current !== generationKey) {
    if (!adoptsOwnId || receiptGeneration.current !== JSON.stringify([snapshot.key, authority])) receipts.current.clear();
    receiptGeneration.current = generationKey;
  }
  const projectRead = useCallback((base: Snapshot): Snapshot => {
    if (!conversationId || base.activeId !== conversationId || initial.error) return base;
    const data = detail.data;
    if (!data) {
      const error = detail.error?.message ?? null;
      if (!base.conv && !base.messages.length && !base.scope && base.loading === !detail.error && base.error === error) return base;
      return { ...base, readVersion: undefined, readError: detail.error, conv: null, messages: [], savedScope: null,
        scope: null, scopeVerified: false, loading: !detail.error, loadingOlder: false, error };
    }
    if (base.busy || base.loadingOlder) return base;
    if (base.readVersion === data && base.readComputedAt === certificate) return base.readError === detail.error ? base :
      { ...base, readError: detail.error, error: detail.error?.message ?? (base.scopeUnavailable ? base.error : null) };
    if (!data.conversation) return { ...base, readVersion: data, readError: detail.error, conv: null, messages: [], savedScope: null,
      scope: null, scopeVerified: false, scopeUnavailable: true, loading: false, error: "This saved chat is unavailable." };
    try {
      const saved = data.conversation.library_scope;
      const linked = new URLSearchParams(paramsText || "");
      if (linked.has("graph_id") && saved && !linked.has("library_scope") &&
          (saved.graph_ids.length !== 1 || saved.graph_ids[0].toLowerCase() !== linked.get("graph_id")?.toLowerCase()))
        throw new Error("This saved chat does not match the linked workspace. Choose an explicit Library scope; its selectors were not widened.");
      const requested = linked.has("library_scope") ? initial.scope : data.resolved_scope;
      if (requested) validateLibraryRequest({ scope: requested });
      return { ...base, readVersion: data, readComputedAt: certificate, readError: detail.error, activeId: data.conversation.id, conv: data.conversation,
        scope: requested, scopeVerified: !!requested, savedScope: saved,
        messages: data.messages.map((message) => decorateReceipt(scopedMessageTurn(message, requested, graphs))), loading: false,
        ...history(data), reachSeq: null, loadingOlder: false, foreign: !data.conversation.mine,
        scopeUnavailable: data.conversation.scope_status !== "resolved",
        error: detail.error?.message ?? (data.conversation.scope_status !== "resolved" ? "The saved chat scope is unavailable. Choose an explicit Library scope to start a new chat." : null) };
    } catch (error) {
      return { ...base, readVersion: data, readError: detail.error, scopeUnavailable: true, conv: null, messages: [], savedScope: null, scope: null, scopeVerified: false,
        loading: false, error: error instanceof Error ? error.message : "This chat could not be verified." };
    }
  }, [conversationId, initial, detail.data, detail.error, paramsText, decorateReceipt, graphs, certificate]);
  // The query's current authority fences the presentation in this render,
  // including withheld/refused data, before an effect can clear local turns.
  const visible = admissionReady ? projectRead(snapshot.key === key ? snapshot : adoptsOwnId ? { ...snapshot, key, routeAdmission: admissionKey, routeParams: paramsText, routeId: conversationId } : blank()) : { ...blank(), loading: false };
  const live = useRef({ key, ready: admissionReady, snapshot: visible, graphs });
  live.current = { key, ready: admissionReady, snapshot: visible, graphs };
  const generation = useRef(0);
  const controller = useRef<AbortController | null>(null);
  const [model, setModel] = useState<ModelTier>(DEFAULT_MODEL);
  const [style, setStyle] = useState<AnswerStyle>("detailed");
  const addressMatches = useCallback(() => {
    if (paramsText === undefined || typeof window === "undefined") return true;
    const address = new URL(window.location.href);
    return /^\/chats\/?$/.test(address.pathname) && address.searchParams.toString() === paramsText;
  }, [paramsText]);
  const isFresh = useCallback((gen: number) => {
    if (!live.current.ready || live.current.key !== key || generation.current !== gen || !addressMatches()) return false;
    try { getToken(); return true; } catch { return false; }
  }, [getToken, key, addressMatches]);

  useEffect(() => {
    controller.current?.abort();
    ++generation.current;
    const accepted = adoption.current;
    adoption.current = null;
    if (accepted?.to === key) {
      setSnapshot((current) => current.key === accepted.from ? { ...current, key, routeAdmission: admissionKey, routeParams: paramsText, routeId: conversationId } : current);
    } else { receipts.current.clear(); setSnapshot(blank()); }
    return () => { controller.current?.abort(); ++generation.current; };
  }, [key, admissionReady, blank, admissionKey, paramsText, conversationId]);
  useEffect(() => {
    if (admissionReady) setSnapshot((current) => projectRead(current.key === key ? current : blank()));
  }, [key, admissionReady, blank, projectRead]);

  const interactiveRead = useCallback((id: string, signal: AbortSignal, load: () => Promise<ScopedConversationDetail>) =>
    authorizedPageRead({ client: qc, signal }, JSON.stringify([queryScope, pageSectionPath("conversation_detail", { id })]), async () => {
      try { return await load(); }
      catch (error) {
        if (signal.aborted || error instanceof ApiError || transientFailure(error)) throw error;
        throw new StoredCertificateError(error instanceof Error ? error.message : "This chat could not be verified.");
      }
    }), [qc, queryScope]);
  const recoverThread = useCallback(async (critical = false) => {
    const current = live.current.snapshot;
    if (!current.activeId || live.current.key !== key || !admissionReady) return;
    const id = current.activeId;
    const ctrl = new AbortController(); controller.current?.abort(); controller.current = ctrl;
    const gen = ++generation.current;
    const release = critical ? beginCriticalRead(ctrl.signal) : () => {};
    setSnapshot((s) => ({ ...s, loadingOlder: true }));
    try {
      // This is recovery of a stopped action or of explicitly expanded
      // history, never the first-page mount path. Reverify every shown page.
      const result = await interactiveRead(id, ctrl.signal, async () => {
        const result = await readScopedThread(getToken(), id, ctrl.signal, current.reachSeq, critical ? "critical" : "normal");
        if (!current.scope || result.conversation.scope_status !== "resolved" || !result.conversation.library_scope ||
            !matchingLibraryScope(current.scope, result.conversation.library_scope)) throw new StoredCertificateError("This chat's saved scope changed. Reopen it to verify its history.");
        return result;
      });
      if (!isFresh(gen)) return;
      setSnapshot((s) => ({ ...s, readVersion: qc.getQueryData<Stored<ConversationDetailPayload>>(conversationKey(queryScope, id))?.payload,
        conv: result.conversation, savedScope: result.conversation.library_scope, scopeVerified: true,
        messages: result.messages.map((message) => decorateReceipt(scopedMessageTurn(message, current.scope, live.current.graphs))),
        loading: false, loadingOlder: false, error: null, ...history(result) }));
    } catch (error) {
      if (isFresh(gen)) setSnapshot((s) => transientFailure(error) ? { ...s, loadingOlder: false, error: error instanceof Error ? error.message : "This chat could not be reached." } :
        { ...s, loadingOlder: false, conv: null, messages: [], savedScope: null, scopeVerified: false, error: error instanceof Error ? error.message : "This chat could not be verified." });
    } finally { release(); }
  }, [key, admissionReady, interactiveRead, getToken, isFresh, qc, queryScope, decorateReceipt]);

  // Background re-check (tab return, reconnect, visible minute tick). What is
  // on screen stays until the fresh read lands and is replaced in one update;
  // a denial or a failed verification clears it, and a transient failure
  // keeps it with a note. Identity changes are handled by the key fence above.
  const revalidate = useCallback(async () => {
    if (!admissionReady || live.current.key !== key) return;
    const id = live.current.snapshot.activeId;
    const ctx = { getAuthToken: getToken, scope: queryScope };
    // Both indexed reads register together. The Stored observer owns owed
    // polling; scope resolution belongs to an explicit scope action or Ask.
    await Promise.all([
      qc.fetchQuery({ ...conversationsQueryOptions(ctx), staleTime: 0 }),
      id ? qc.fetchQuery({ ...conversationQueryOptions(ctx, id), staleTime: 0 }) : Promise.resolve(),
      live.current.snapshot.reachSeq !== null ? recoverThread(true) : Promise.resolve(),
    ]).catch(() => { /* Current query errors and authority are rendered by their observers. */ });
  }, [key, admissionReady, getToken, qc, queryScope, recoverThread]);
  // A send, the first load or a paging read in flight is already the fresh read.
  useReturnRefetch(useCallback(() => void revalidate(), [revalidate]), {
    enabled: admissionReady,
    inFlight: () => live.current.snapshot.busy || live.current.snapshot.loading || live.current.snapshot.loadingOlder,
    intervalMs: 60_000,
  });
  useEffect(() => {
    if (!admissionReady) return;
    // Going offline is not a revocation: keep what was verified, say why it may be stale.
    const offline = () => {
      if (live.current.key === key) setSnapshot((s) => s.key !== key ? s : { ...s, error: "Offline. Showing this chat as last verified; it re-checks when the connection returns." });
    };
    window.addEventListener("offline", offline);
    return () => window.removeEventListener("offline", offline);
  }, [key, admissionReady]);

  const changeScope = useCallback((scope: LibraryScope) => {
    if (!live.current.ready || live.current.key !== key || !addressMatches()) return;
    try { getToken(); validateLibraryRequest({ scope }); } catch { return; }
    controller.current?.abort();
    const ctrl = new AbortController(); controller.current = ctrl;
    const gen = ++generation.current;
    live.current.snapshot = { ...live.current.snapshot, scope: normalizedLibraryScope(scope), scopeVerified: false, messages: [], conv: null, busy: false };
    setSnapshot((s) => s.key !== key ? s : { ...s, scope: normalizedLibraryScope(scope), scopeVerified: false, conv: null, messages: [], busy: false, loading: true, error: null });
    void (async () => {
      try {
        const normalized = await resolveScopedChatScope(getToken(), scope, ctrl.signal);
        if (isFresh(gen)) setSnapshot((s) => ({ ...s, scope: normalized, scopeVerified: true, loading: false }));
      } catch (error) {
        if (isFresh(gen)) setSnapshot((s) => ({ ...s, scope: null, scopeVerified: false, loading: false, error: error instanceof Error ? error.message : "This selection could not be verified." }));
      }
    })();
  }, [key, getToken, isFresh, addressMatches]);
  /** `retry`: re-ask a saved question that has no answer. `failedKey` is the failed turn on
   *  screen (null for a question with no reply turn at all); `questionMessageId` the saved
   *  question (null when nothing was saved, so the question is asked afresh and the failed
   *  pair leaves the screen). */
  const ask = useCallback(async (question: string, retry?: { failedKey: string | null; questionMessageId: string | null }): Promise<boolean> => {
    const s = live.current.snapshot;
    if (!live.current.ready || live.current.key !== key || s.busy || s.loading || !s.scopeVerified || !s.scope || s.scopeUnavailable || s.foreign || !question.trim() || !addressMatches()) return false;
    const ctrl = new AbortController(); controller.current?.abort(); controller.current = ctrl;
    const gen = ++generation.current;
    const answerKey = crypto.randomUUID();
    const startedAt = Date.now();
    try {
      const token = getToken();
      if (!isFresh(gen)) return false;
      live.current.snapshot = { ...s, busy: true };
      setSnapshot((cur) => {
        const at = retry?.failedKey ? cur.messages.findIndex((m) => m.key === retry.failedKey) : -1;
        // Same saved question: the failed turn becomes the pending one. Nothing saved: drop the
        // failed turn and its question bubble, and ask again as a new question.
        const fresh = [{ key: crypto.randomUUID(), role: "user" as const, text: question }, emptyAssistant(answerKey, question)];
        const messages = retry && !retry.failedKey ? [...cur.messages, fresh[1]]
          : at < 0 ? [...cur.messages, ...fresh]
          : retry?.questionMessageId ? [...cur.messages.slice(0, at), emptyAssistant(answerKey, question), ...cur.messages.slice(at + 1)]
          : [...cur.messages.slice(0, Math.max(0, at - 1)), ...fresh, ...cur.messages.slice(at + 1)];
        return { ...cur, busy: true, error: null, messages };
      });
      if (s.activeId) void qc.cancelQueries({ queryKey: conversationKey(queryScope, s.activeId), exact: true }, { revert: false });
      if (!isFresh(gen)) return false;
      const inherit = !!s.activeId && !!s.savedScope && matchingLibraryScope(s.scope, s.savedScope);
      const answer = await askScopedStream(token, { text: question, scope: s.scope, conversationId: s.activeId, retryMessageId: retry?.questionMessageId, inherit, replace: !!s.activeId && !inherit, model, style }, ctrl.signal, (progress) => {
        if (!isFresh(gen)) return;
        setSnapshot((cur) => ({ ...cur, messages: cur.messages.map((m) =>
          m.key !== answerKey || m.role !== "assistant" || !m.pending || progress.attempt < (m.attempt ?? 0) ? m :
          { ...m, attempt: progress.attempt, progress: progress.stage === "retrieved" ? { kind: "retrieved", sources: progress.sources } : { kind: progress.stage } }) }));
      });
      const elapsedMs = Date.now() - startedAt;
      if (!isFresh(gen)) return false;
      // A first question creates the chat: tell the other lists now (the sidebar
      // refetches under its own live token), before the reread below — the
      // conversation exists even if this answer's receipt then fails to verify.
      if (answer.new_conversation) emitConversationsChanged();
      // Names, pin state and ownership are recovered from a fresh gated read;
      // client clocks/ambient graph labels never construct a conversation DTO.
      const continuing = s.activeId?.toLowerCase() === answer.conversation_id.toLowerCase();
      const detail = await interactiveRead(answer.conversation_id, ctrl.signal, () => readScopedThread(getToken(), answer.conversation_id, ctrl.signal, continuing ? s.reachSeq : null));
      if (!isFresh(gen)) return false;
      const receipt = detail.messages.find((m) => m.id === answer.message_id && m.role === "assistant");
      if (!receipt || receipt.support_status !== "complete" || !receipt.library_scope || !matchingLibraryScope(receipt.library_scope, answer.library_scope)) throw new Error("This saved answer's evidence is no longer available.");
      if (detail.conversation.scope_status !== "resolved" || !matchingLibraryScope(detail.conversation.library_scope, answer.library_scope)) throw new Error("The saved answer scope changed before it could be displayed.");
      const liveFindResults = receiptFingerprint(scopedMessageTurn(receipt, answer.library_scope, live.current.graphs) as AssistantTurn) ===
        receiptFingerprint(answerTurn(answer, answer.message_id, question, live.current.graphs)) ? answer.source_results : undefined;
      const extras: ReceiptExtras = { prompt: question, elapsedMs, reveal: true, egressBlocked: answer.egress_blocked,
        empty: answer.answer_status === "no_evidence", evidenceTruncated: answer.truncated, evidenceTruncationReason: answer.truncation_reason };
      receipts.current.set(answer.message_id.toLowerCase(), { fingerprint: receiptFingerprint(scopedMessageTurn(receipt, answer.library_scope, live.current.graphs) as AssistantTurn),
        extras });
      setSnapshot((cur) => ({ ...cur, readComputedAt: liveCertificate.current, readVersion: qc.getQueryData<Stored<ConversationDetailPayload>>(conversationKey(queryScope, answer.conversation_id))?.payload, activeId: answer.conversation_id, scope: answer.library_scope, scopeVerified: true, savedScope: answer.library_scope, busy: false, conv: detail.conversation,
        // The reread can mask older receipts while admitting this new answer.
        // Project every canonical message, never retain pre-reread prose/cards.
        messages: detail.messages.map((m) => m.id === answer.message_id && m.role === "assistant" ? { ...scopedMessageTurn(m, answer.library_scope, live.current.graphs), prompt: question, elapsedMs, reveal: true, sourceResults: liveFindResults, egressBlocked: answer.egress_blocked, empty: answer.answer_status === "no_evidence", evidenceTruncated: answer.truncated, evidenceTruncationReason: answer.truncation_reason } : scopedMessageTurn(m, answer.library_scope, live.current.graphs)).map(decorateReceipt),
        ...history(detail), reachSeq: continuing ? s.reachSeq : null }));
      void qc.invalidateQueries({ queryKey: conversationKey(queryScope, answer.conversation_id), exact: true });
      // The server renames a new chat once its generated title lands; whichever
      // read sees that first passes it on the same way (revalidate does too).
      if (detail.conversation.title !== (answer.new_conversation ? answer.title : s.conv?.title)) emitConversationsChanged();
      return true;
    } catch (error) {
      // The ask itself failed (as opposed to its answer failing verification): keep the
      // thread, show why on the question, and let the user re-ask it.
      const failed = askFailure(error);
      if (failed && isFresh(gen)) {
        const conversationId = failed.conversationId ?? s.activeId;
        let detail: ScopedConversationDetail | null = null;
        if (failed.questionMessageId && conversationId) {
          try {
            const continuing = s.activeId?.toLowerCase() === conversationId.toLowerCase();
            detail = await interactiveRead(conversationId, ctrl.signal, () => readScopedThread(getToken(), conversationId, ctrl.signal, continuing ? s.reachSeq : null));
          } catch { detail = null; }
        }
        if (!isFresh(gen)) return false;
        if (failed.conversationId && !s.activeId) emitConversationsChanged();
        if (detail && detail.conversation.scope_status === "resolved" && matchingLibraryScope(detail.conversation.library_scope, s.scope)) {
          const continuing = s.activeId?.toLowerCase() === detail.conversation.id.toLowerCase();
          const shown = detail;
          setSnapshot((cur) => ({ ...cur, readComputedAt: liveCertificate.current, readVersion: qc.getQueryData<Stored<ConversationDetailPayload>>(conversationKey(queryScope, shown.conversation.id))?.payload, activeId: shown.conversation.id, conv: shown.conversation, savedScope: shown.conversation.library_scope, busy: false, error: null,
            messages: shown.messages.map((m) => decorateReceipt(scopedMessageTurn(m, s.scope, live.current.graphs))), ...history(shown), reachSeq: continuing ? s.reachSeq : null }));
        } else {
          const turn: AssistantTurn = { ...emptyAssistant(answerKey, question), pending: false, startedAt: undefined, supportStatus: "failed",
            failure: { reason: failed.reason, questionMessageId: failed.questionMessageId } };
          setSnapshot((cur) => ({ ...cur, busy: false, error: null, activeId: failed.conversationId ?? cur.activeId,
            savedScope: failed.conversationId ? cur.savedScope ?? s.scope : cur.savedScope,
            messages: cur.messages.map((m) => m.key === answerKey ? turn : m) }));
        }
        if (conversationId) void qc.invalidateQueries({ queryKey: conversationKey(queryScope, conversationId), exact: true });
        return false;
      }
      if (isFresh(gen)) setSnapshot((cur) => ({ ...cur, busy: false, conv: null, messages: [],
        savedScope: null, scopeVerified: false, error: error instanceof Error ? error.message : "This answer could not be verified." }));
      return false;
    }
  }, [key, getToken, isFresh, model, style, addressMatches, qc, queryScope, conversationId, interactiveRead]);
  const send = useCallback((question: string): Promise<boolean> => ask(question), [ask]);
  /** Re-ask the question a failed turn answers (or an unanswered trailing question, by its
   *  own key), against its saved copy when there is one. */
  const retry = useCallback((turnKey: string): Promise<boolean> => {
    const messages = live.current.snapshot.messages;
    const at = messages.findIndex((m) => m.key === turnKey);
    const turn = at < 0 ? null : messages[at];
    if (turn?.role === "user" && at === messages.length - 1) return ask(turn.text, { failedKey: null, questionMessageId: turn.key });
    if (turn?.role === "assistant" && turn.stopped) {
      if (!live.current.snapshot.activeId) {
        setSnapshot((s) => ({ ...s, error: "This request may have been saved. Open it from chat history before retrying; its conversation ID did not reach this browser." }));
        emitConversationsChanged();
        return Promise.resolve(false);
      }
      return recoverThread().then(() => false);
    }
    const question = at > 0 ? messages[at - 1] : null;
    if (turn?.role !== "assistant" || !turn.failure || question?.role !== "user") return Promise.resolve(false);
    return ask(question.text, { failedKey: turnKey, questionMessageId: turn.failure.questionMessageId });
  }, [ask, recoverThread]);
  const loadOlder = useCallback(async (): Promise<boolean> => {
    const s = live.current.snapshot;
    if (!live.current.ready || live.current.key !== key || s.busy || s.loading || s.loadingOlder || !s.activeId || !s.conv || s.olderCursor === null || !addressMatches()) return false;
    const gen = generation.current;
    const ctrl = new AbortController(); controller.current?.abort(); controller.current = ctrl;
    live.current.snapshot = { ...s, loadingOlder: true };
    setSnapshot((cur) => ({ ...cur, loadingOlder: true }));
    try {
      const page = await interactiveRead(s.activeId, ctrl.signal, async () => {
        const page = await readScopedConversation(getToken(), s.activeId!, ctrl.signal, s.olderCursor);
        if (!s.conv || !sameSavedScope(s.conv, page.conversation)) throw new StoredCertificateError("This chat's saved scope changed. Reopen it to verify its history.");
        return page;
      });
      if (!isFresh(gen)) return false;
      const shown = live.current.snapshot.conv;
      if (!shown || !sameSavedScope(shown, page.conversation)) throw new Error("This chat's saved scope changed. Reopen it to verify its history.");
      setSnapshot((cur) => ({ ...cur, loadingOlder: false, ...history(page), reachSeq: page.messages[0]?.seq ?? cur.reachSeq,
        messages: [...page.messages.map((m) => scopedMessageTurn(m, cur.scope, live.current.graphs)), ...cur.messages] }));
      return true;
    } catch (error) {
      if (isFresh(gen)) setSnapshot((cur) => ({ ...cur, loadingOlder: false, conv: null, messages: [], savedScope: null, scopeVerified: false,
        error: error instanceof Error ? error.message : "This chat's earlier messages could not be verified." }));
      return false;
    }
  }, [key, getToken, isFresh, addressMatches, interactiveRead]);
  const stop = useCallback(() => {
    controller.current?.abort(); ++generation.current;
    const stopped = (s: Snapshot): Snapshot => ({ ...s, busy: false, messages: s.messages.map((m) =>
      m.role === "assistant" && m.pending ? { ...m, pending: false, stopped: true, progress: null, elapsedMs: Math.max(0, Date.now() - (m.startedAt ?? Date.now())) } : m) });
    live.current.snapshot = stopped(live.current.snapshot);
    setSnapshot(stopped);
  }, []);
  const newChat = useCallback(() => {
    stop();
    const next = { ...blank(), activeId: null, conv: null, loading: false };
    live.current.snapshot = next; setSnapshot(next);
    if (next.scope) changeScope(next.scope);
  }, [stop, blank, changeScope]);
  /** The composer chip's ×: back to the default, every readable workspace
   *  with no other selector, so no link or saved chat strands the asker in a
   *  hidden subset. */
  const clearNarrowing = useCallback(() => {
    const scope = defaultChatScope(live.current.graphs.map((g) => g.id));
    if (scope && live.current.ready) changeScope(scope);
  }, [changeScope]);
  return { graphs: admissionReady ? graphs : [], currentGraph: null, conversations: admissionReady ? (listing.data ?? []).filter((row) => !deleted.has(row.id)) : [],
    activeId: visible.activeId, activeConv: visible.conv, messages: visible.messages, busy: visible.busy,
    err: visible.error, setErr: (error: string | null) => setSnapshot((s) => ({ ...s, error })), loadingConv: visible.loading,
    readOnly: !admissionReady || !visible.scopeVerified || visible.scopeUnavailable || visible.foreign,
    libraryScope: admissionReady && visible.scopeVerified ? visible.scope : null, scopeAccepted: visible.scopeVerified && !!visible.savedScope && !!visible.scope && matchingLibraryScope(visible.savedScope, visible.scope),
    admissionReady: admissionReady, admissionKey: admissionKey, messagesTruncated: visible.truncated,
    canLoadOlder: visible.olderCursor !== null && !!visible.conv && !visible.loading, loadingOlder: visible.loadingOlder, loadOlder,
    getNavigationId: () => { if (!live.current.ready || live.current.key !== key || !addressMatches()) return null; try { getToken(); } catch { return null; } return live.current.snapshot.activeId; },
    model, setModel, effort: DEFAULT_EFFORT, setEffort: () => {}, style, setStyle,
    send, retry, stop, newChat, regenerate: send, clearNarrowing,
  };
}
