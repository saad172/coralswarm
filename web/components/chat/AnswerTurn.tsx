"use client";

import { AnswerFeedback } from "./AnswerFeedback";
import { PaywallNote } from "@/components/billing/PaywallNote";
import { useState } from "react";
import { AnswerCitation, AnswerMarkdown } from "@/components/AnswerMarkdown";
import {
  AnswerMeta,
  Button,
  DotMandala,
  ErrorNote,
  Dot,
  SuggestionPill,
  workspaceHue,
} from "@/components/ui";
import { citationHref } from "@/lib/provenance";
import { IconCopy, IconRefresh } from "@/components/icons";
import { type AssistantTurn, type Turn } from "./types";
import { citationVerdict, evidenceSources, sourceCount } from "./evidence";
import { ScopedOutcomes } from "./ScopedOutcomes";
import { SourcePills } from "./SourcePills";
import { FindResults } from "./FindResults";
import { StageLine } from "./StageLine";
import { RevealAnswer } from "./RevealAnswer";
import { failureCopy } from "./failure";

/** The find-tile DetailPanel is owned by the page so only one is ever open. */
export interface FindPanel {
  openId: string | null;
  onOpenChange: (id: string | null) => void;
  oceanKindOf?: (graphId: string) => string | undefined;
}

function UserBubble({ text }: { text: string }) {
  return (
    <h2 className="text-pretty font-display text-[28px] leading-[1.15] text-ink">{text}</h2>
  );
}

/** One assistant answer: meta line, prose, contradictions, sources, actions,
 *  follow-up suggestions. Renders the honesty-guard / rate-limit / error /
 *  disconnected / stopped states in place of prose when they apply. */
function AssistantAnswer({
  turn,
  onCite,
  activeCitation,
  onRegenerate,
  onAsk,
  onRetry,
  find,
  conversationId,
  readOnly,
}: {
  turn: AssistantTurn;
  activeCitation?: number | null;
  onCite: (n: number) => void | boolean;
  onRegenerate: (prompt: string) => void;
  onAsk: (text: string) => void;
  onRetry?: (key: string) => void;
  find?: FindPanel;
  conversationId?: string | null;
  readOnly?: boolean;
}) {
  const [hot, setHot] = useState<number | null>(null);
  const [copied, setCopied] = useState(false);

  // The ask for this question ended without an answer (a recorded failure, or a
  // question saved with no reply). Retry re-asks the saved question: it does not
  // add a second copy of it. Only the latest turn is offered Retry.
  if (turn.failure) {
    return (
      <ErrorNote action={onRetry ? <Button size="sm" onClick={() => onRetry(turn.key)}>Retry</Button> : undefined}>
        {failureCopy(turn.failure.reason)}
      </ErrorNote>
    );
  }

  const seconds = turn.timing?.synthesize_ms
    ? ((turn.timing.embed_ms ?? 0) + (turn.timing.retrieve_ms ?? 0) + (turn.timing.synthesize_ms ?? 0)) / 1000
    : null;

  const cards = evidenceSources(turn);
  // Find tiles replace the evidence strip for this message (see FindResults).
  const hasTiles = !turn.pending && (turn.sourceResults?.length ?? 0) > 0;
  const waiting = turn.pending && !turn.text && !turn.error && !turn.rateLimited && !turn.outOfCredits && !turn.disconnected && !turn.stopped;

  /* Synthesis-tier turn whose LLM call failed after retry: retrieval
   *  succeeded (HTTP 200, citations/sources populated) but the server
   *  couldn't generate prose, so it degraded to citations-only instead of
   *  dropping the connection. Distinct from `turn.disconnected` (the
   *  transport actually dropped) and from `retrieval_only` (the intentional
   *  free/offline tier, which never attempts prose in the first place) —
   *  conflating this with either mislead the user into debugging a network
   *  issue that wasn't the real failure.
   *
   *  Derived from the `final`/`AskResult` payload's `mode` field, not the SSE
   *  event type — `create_query_stream` currently reports a synthesis
   *  failure via its own `error` event (`turn.error`, handled below), not a
   *  `final` frame, so `mode: "sources"` today mostly reaches this component
   *  through the non-streaming `AskResult` path or a reopened conversation's
   *  persisted turn. Keeping this keyed on the payload rather than the event
   *  type means it renders correctly either way, and stays correct if the
   *  stream route starts emitting it too. */
  const sourcesOnly = turn.mode === "sources";
  // Retrieval came back empty too — nothing to show alongside "no answer".
  // Reads exactly like the generic ask-failure path below (`turn.error`'s
  // useChatThread.ts fallback, "That question didn't go through.") instead
  // of a technically-true-but-misleading "0 matching sources" count.
  const sourcesOnlyEmpty = sourcesOnly && cards.length === 0;

  const metaLabel = turn.pending
    ? "answering…"
    : turn.mode === "retrieval_only"
      ? `${cards.length} source${cards.length === 1 ? "" : "s"} · not synthesized`
      : sourcesOnlyEmpty
        ? "no answer generated"
        : sourcesOnly
          ? `${cards.length} source${cards.length === 1 ? "" : "s"} · no answer generated`
          : `answered from ${cards.length} source${
              (cards.length) === 1 ? "" : "s"
            }${seconds ? ` · ${seconds.toFixed(1)}s` : ""}`;

  // Scoped answers: the distinct-source count and nothing else. Passages,
  // raw chunks and Jev tallies live on the cards and in the drawer.
  const sources = sourceCount(cards);
  const scopedLabel = turn.retainedCitations ? `${sources} source${sources === 1 ? "" : "s"}` : null;
  return (
    <div className="flex flex-col gap-[14px]">
      {waiting ? (
        <StageLine state="pending" stage={turn.progress ?? null} startedAt={turn.startedAt ?? Date.now()} />
      ) : (
        turn.stopped ? <StageLine state="stopped" elapsedMs={turn.elapsedMs ?? 0} onAskAgain={onRetry ? () => onRetry(turn.key) : undefined} /> : turn.retainedCitations && turn.mode === "synthesized" ? <StageLine state="answered" sources={sources} elapsedMs={turn.elapsedMs} /> : <AnswerMeta mark={<DotMandala size={18} density={0.75} />}>{scopedLabel ?? metaLabel}</AnswerMeta>
      )}

      {turn.error && (
        <ErrorNote action={<Button size="sm" onClick={() => onRegenerate(turn.prompt)}>Retry</Button>}>
          {turn.error}
        </ErrorNote>
      )}
      {turn.rateLimited && (
        <ErrorNote>You&rsquo;re asking faster than the plan allows — give it a moment and retry.</ErrorNote>
      )}
      {turn.outOfCredits && <PaywallNote reason={turn.outOfCreditsReason} />}
      {sourcesOnly && !sourcesOnlyEmpty && !turn.error && (
        <ErrorNote action={<Button size="sm" onClick={() => onRegenerate(turn.prompt)}>Retry</Button>}>
          Couldn&rsquo;t generate an answer — showing the {cards.length} matching source
          {cards.length === 1 ? "" : "s"} instead.
        </ErrorNote>
      )}
      {sourcesOnlyEmpty && !turn.error && (
        <ErrorNote action={<Button size="sm" onClick={() => onRegenerate(turn.prompt)}>Retry</Button>}>
          That question didn&rsquo;t go through.
        </ErrorNote>
      )}
      {turn.disconnected && !turn.error && !sourcesOnly && (
        <ErrorNote action={<Button size="sm" onClick={() => onRegenerate(turn.prompt)}>Retry</Button>}>
          The connection dropped before this finished.
        </ErrorNote>
      )}


      {!turn.error && !turn.rateLimited && !turn.outOfCredits && turn.text && (
        <RevealAnswer active={!!turn.reveal}><div className="text-[16px] leading-[1.72] text-ink">
          <AnswerMarkdown text={turn.text} hot={hot ?? activeCitation ?? null} setHot={setHot} onCite={onCite} citeHref={(n) => citationHref(turn, n)} citeVerdict={(n) => citationVerdict(turn, n)} />
        </div></RevealAnswer>
      )}

      {!turn.pending && turn.empty && (
        <ErrorNote>
          {turn.libraryScope ? "No matching readable evidence was returned in this scope." : "Nothing in this scope clears the bar for a confident answer."}
        </ErrorNote>
      )}

      {turn.contradictions.map((c, i) => (
        <div key={i} className="flex gap-3 rounded-card border border-line bg-bg-subtle px-[15px] py-[13px]">
          <Dot hue="amber" size={5} className="mt-[6px]" />
          <div className="min-w-0 flex-1">
            <div className="mb-[8px] text-[13px] font-semibold text-ink">{c.topic}</div>
            <div className="flex flex-col gap-[7px]">
              {c.positions.map((p, j) => (
                <div key={j} className="flex items-center gap-[9px] text-[13px] leading-[1.5] text-ink-2">
                  <span className="inline-flex flex-none items-center gap-[5px] font-mono text-[10px] uppercase text-ink-3">
                    <Dot hue={workspaceHue(p.ocean_id)} size={5} />
                    {p.ocean_name}
                  </span>
                  <span className="flex-1">{p.claim}</span>
                  <AnswerCitation n={p.citation_index} href={citationHref(turn, p.citation_index)} hot={hot ?? activeCitation ?? null} setHot={setHot} onCite={onCite} />
                </div>
              ))}
            </div>
            <div className="mt-[10px]">
              <Button size="sm" onClick={() => onAsk(`Which is current: ${c.topic}?`)}>
                Ask which is current
              </Button>
            </div>
          </div>
        </div>
      ))}

      {turn.empty && turn.libraryScope && <p className="text-xs text-ink-3">No matching readable evidence was returned for this selection. Inspect its captures in Library to check extraction status; this does not establish that retained history is empty.</p>}
      {turn.egressBlocked && <p className="text-xs text-ink-3">Answer generation is unavailable under this workspace’s processing settings. Readable captured evidence is shown.</p>}
      {turn.syntheticDemo && <p className="text-xs font-medium text-warn">Contains synthetic demo evidence</p>}
      {turn.evidenceTruncated && <p role="status" className="text-xs text-ink-3">Evidence is bounded: {turn.evidenceTruncationReason || "additional readable support was omitted"}.</p>}
      {turn.scopedOutcomes && <ScopedOutcomes outcomes={turn.scopedOutcomes} />}
      <RevealAnswer active={!!turn.reveal} delay={640}><SourcePills hot={hot ?? activeCitation ?? null} setHot={setHot} sources={cards} text={turn.text} onSelect={onCite} tileKeys={hasTiles ? turn.sourceResults?.map(r => `${r.graph.id}:${r.source_item_id}`) : undefined}>{hasTiles ? (
        <FindResults
          results={turn.sourceResults ?? []}
          openId={find?.openId ?? null}
          onOpenChange={(id) => find?.onOpenChange(id)}
          oceanKindOf={find?.oceanKindOf}
        />
      ) : (
        undefined
      )}</SourcePills></RevealAnswer>

      <RevealAnswer active={!!turn.reveal} delay={760}>{!turn.pending && turn.text && (
        <div className="flex items-center gap-[14px] text-ink-3">
          <button
            type="button"
            onClick={() => {
              navigator.clipboard?.writeText(turn.text).catch(() => {});
              setCopied(true);
              setTimeout(() => setCopied(false), 1400);
            }}
            className="inline-flex h-[30px] w-[30px] items-center justify-center rounded-field hover:bg-bg-hover"
          >
            <IconCopy width={13} height={13} />
            <span className="sr-only">{copied ? "Copied" : "Copy"}</span>
          </button>
          <button
            type="button"
            onClick={() => onRegenerate(turn.prompt)}
            className="inline-flex h-[30px] w-[30px] items-center justify-center rounded-field hover:bg-bg-hover"
          >
            <IconRefresh width={13} height={13} />
            <span className="sr-only">Regenerate</span>
          </button>
          {conversationId && !turn.stopped && !turn.error && !turn.disconnected && !turn.rateLimited && !turn.outOfCredits && <AnswerFeedback conversationId={conversationId} messageId={turn.key} initial={turn.feedback} readOnly={readOnly} />}
        </div>
      )}

      </RevealAnswer>
      {turn.suggestions.length > 0 && (
        <div className="flex flex-wrap gap-[7px]">
          {turn.suggestions.map((s) => (
            <SuggestionPill key={s} onClick={() => onAsk(s)}>
              {s}
            </SuggestionPill>
          ))}
        </div>
      )}
    </div>
  );
}

export function ThreadTurn({
  turn,
  onCite,
  activeCitation,
  onRegenerate,
  onAsk,
  onRetry,
  find,
  conversationId,
  readOnly,
}: {
  turn: Turn;
  activeCitation?: number | null;
  onCite: (n: number) => void | boolean;
  onRegenerate: (prompt: string) => void;
  onAsk: (text: string) => void;
  onRetry?: (key: string) => void;
  find?: FindPanel;
  conversationId?: string | null;
  readOnly?: boolean;
}) {
  if (turn.role === "user") return <UserBubble text={turn.text} />;
  return (
    <AssistantAnswer
      turn={turn}
      activeCitation={activeCitation}
      onCite={onCite}
      onRegenerate={onRegenerate}
      onAsk={onAsk}
      onRetry={onRetry}
      conversationId={conversationId}
      readOnly={readOnly}
      find={find}
    />
  );
}
