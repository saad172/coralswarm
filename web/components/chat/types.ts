import type { LibraryCitation, LibraryScope } from "@/lib/libraryMemoryApi";
import type { LegacyEvidence, ScopedOutcome, SourceResult } from "@/lib/scopedChatApi";
import type {
  AskSource,
  ChatFinal,
  CitationDetail,
  ConversationDetail,
  ConversationMessageItem,
  Contradiction,
  StreamedCitation,
  TraversalWalk,
  UnreachableOcean,
} from "@/lib/api";

/* One turn in the on-screen thread. Ported from archive/web-legacy-ui's
   ask.page.tsx `Turn` type — same shape, trimmed of the fields this rebuild
   doesn't wire up yet (on-device private search hits, inline action cards). */

export interface UserTurn {
  key: string;
  role: "user";
  text: string;
}

export interface AssistantTurn {
  feedback?: import("@/lib/messageFeedbackApi").MessageFeedback | null;
  key: string;
  role: "assistant";
  /** The question that produced this answer — for regenerate. */
  prompt: string;
  text: string;
  pending: boolean;
  /** Client clock for the live waiting state; never inferred from backend stages. */
  startedAt?: number;
  elapsedMs?: number;
  reveal?: boolean;
  attempt?: number;
  progress?: import("./StageLine").AnswerStage | null;
  error?: string;
  stopped?: boolean;
  disconnected?: boolean;
  rateLimited?: boolean;
  /** 402 from `POST /v1/queries[/stream]` (`billing::enforcement::is_blocked`)
   *  — this account's credit balance is at or below zero. */
  outOfCredits?: boolean;
  /** The 402 body's `refusal_reason`; absent on older backends. */
  outOfCreditsReason?: string | null;
  /** Honesty guard (ADR-0011 §G24): nothing cleared the relevance floor. */
  empty?: boolean;
  retainedCitations?: LibraryCitation[];
  /** Graph id to ocean name, from the admitted graph list. */
  graphNames?: Record<string, string>;
  scopedOutcomes?: ScopedOutcome[];
  legacyEvidence?: LegacyEvidence[];
  /** Chat find tiles (1-3, best first). Live answers only: never persisted, so a reloaded thread has none. */
  sourceResults?: SourceResult[];
  libraryScope?: LibraryScope;
  supportStatus?: "complete" | "unknown" | "unavailable" | "failed";
  /** The ask for this turn's question did not finish. Never an answer: no text, no evidence.
   *  `questionMessageId` is the saved question to re-ask (null when nothing was saved). */
  failure?: { reason: string; questionMessageId: string | null };
  syntheticDemo?: boolean;
  egressBlocked?: boolean;
  evidenceTruncated?: boolean;
  evidenceTruncationReason?: string | null;
  citations: string[];
  citationDetails: CitationDetail[];
  contradictions: Contradiction[];
  unreachable: UnreachableOcean[];
  sourceOceans: Record<string, string>;
  sources: AskSource[];
  /** `"sources"`: the LLM call failed server-side after retry — retrieval
   *  succeeded and `citations`/`sources` are populated, but there's no prose.
   *  Distinct from `"retrieval_only"`, the intentional free/offline tier. */
  mode: "synthesized" | "retrieval_only" | "sources" | null;
  timing: { embed_ms?: number; retrieve_ms?: number; synthesize_ms?: number };
  suggestions: string[];
  /** Subgraph this question walked. Live from `final.traversal_walk`; on
   *  reopen, restored from `timing.walk`. */
  walk: TraversalWalk | null;
  /** Live SSE `status` stage (`retrieving` / `packing` / `synthesizing` /
   *  `citing`) while `pending`. Unused after `final`. */
  stage?: string | null;
}

export type Turn = UserTurn | AssistantTurn;

export function emptyAssistant(key: string, prompt: string): AssistantTurn {
  return {
    key,
    role: "assistant",
    prompt,
    text: "",
    pending: true,
    startedAt: Date.now(),
    citations: [],
    citationDetails: [],
    contradictions: [],
    unreachable: [],
    sourceOceans: {},
    sources: [],
    mode: null,
    timing: {},
    suggestions: [],
    walk: null,
    stage: null,
  };
}

/** Label shown while an assistant turn is still on the wire. */
export function pendingLabel(stage: string | null | undefined, hasText: boolean): string {
  if (hasText) return "writing…";
  switch (stage) {
    case "embedding":
      return "Understanding your question…";
    case "retrieving":
      return "Retrieving sources…";
    case "packing":
      return "Preparing sources…";
    case "synthesizing":
      return "Synthesizing answer…";
    case "citing":
      return "Checking citations…";
    default:
      return "Starting your answer…";
  }
}

/** Restore a persisted conversation's messages into on-screen turns. */
export function turnsFromDetail(detail: ConversationDetail): Turn[] {
  const out: Turn[] = [];
  for (const m of detail.messages) out.push(...turnFromMessage(m));
  return out;
}

function turnFromMessage(m: ConversationMessageItem): Turn[] {
  if (m.role === "user") return [{ key: m.id, role: "user", text: m.text }];
  return [
    {
      key: m.id,
      role: "assistant",
      prompt: "",
      text: m.text,
      pending: false,
      citations: m.citations ?? [],
      citationDetails: m.citation_details ?? [],
      contradictions: m.contradictions ?? [],
      unreachable: [],
      sourceOceans: {},
      sources: m.sources ?? [],
      mode: m.mode,
      timing: m.timing ?? {},
      suggestions: [],
      walk: m.timing?.walk ?? null,
    },
  ];
}

/** The answer the source drawer and rail should be describing: the newest
 *  assistant turn, INCLUDING one still streaming.
 *
 *  This deliberately does not skip `pending` turns. Citations now arrive
 *  mid-answer, so a drawer pinned to the last COMPLETED turn sits on the
 *  previous answer's sources for the whole generation and then swaps at the
 *  end — which is both stale and out of step with the thread, where the same
 *  cards are already appearing. Following the in-flight turn keeps the two
 *  surfaces showing one answer's sources at a time. */
/** The thread's trailing question when nothing answers it (saved before a failure could be
 *  recorded, or the record itself could not be written): it can be re-asked in place. */
export function unansweredQuestion(messages: Turn[]): { key: string; text: string } | null {
  const last = messages.at(-1);
  return last?.role === "user" ? { key: last.key, text: last.text } : null;
}

export function latestAssistantTurn(messages: Turn[]): AssistantTurn | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    const m = messages[i];
    if (m.role === "assistant") return m;
  }
  return null;
}

/** Slot a mid-stream `citation` into an in-flight turn at its display position.
 *  Indexed by `n` rather than appended, so an out-of-order or repeated frame
 *  cannot shuffle the cards out of step with the `[n]` chips in the prose. */
export function applyCitation(t: AssistantTurn, c: StreamedCitation): AssistantTurn {
  const { n, ...detail } = c;
  if (n < 1) return t;
  const citationDetails = t.citationDetails.slice();
  // Gaps are possible in principle (a frame lost mid-flight); pad so index n-1
  // stays the nth card rather than silently promoting a later source.
  while (citationDetails.length < n - 1) {
    citationDetails.push({ atom_id: "", graph_id: "", title: "" });
  }
  citationDetails[n - 1] = detail;
  return {
    ...t,
    citationDetails,
    citations: citationDetails.map((d) => d.atom_id),
  };
}

/** Merge a streamed `final` payload onto an in-flight assistant turn. The
 *  authoritative, reef/ocean-enriched list replaces whatever the mid-stream
 *  `citation` frames built up. */
export function applyFinal(t: AssistantTurn, f: ChatFinal): AssistantTurn {
  return {
    ...t,
    text: f.text ?? t.text,
    pending: false,
    citations: f.citations ?? [],
    citationDetails: f.citation_details ?? t.citationDetails,
    contradictions: f.contradictions ?? t.contradictions,
    unreachable: f.unreachable_oceans ?? t.unreachable,
    empty: f.honesty_guard ?? false,
    sourceOceans: f.source_oceans ?? t.sourceOceans,
    sources: f.sources ?? [],
    mode: f.mode,
    timing: f.timing,
    suggestions: f.suggestions ?? [],
    walk: f.traversal_walk ?? f.timing?.walk ?? t.walk,
  };
}
