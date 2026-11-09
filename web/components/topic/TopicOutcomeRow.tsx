"use client";
import type { LibraryOutcome } from "@/lib/libraryMemoryApi";
import { nodeLabel } from "@/lib/topicLabels";
import { Badge, Card, Chip, Verdict } from "@/components/ui";
import { parseSupersession, supersessionLine } from "@/lib/graphSurfaces";
import { SessionLink } from "./TopicSessionLink";
const LINK = "rounded-row text-left text-brand-2 underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand";
const dateLabel = (value: string | null) => value && Number.isFinite(new Date(value).getTime()) ? new Date(value).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" }) : "Unknown";

export function OutcomeRow({
  outcome,
  sessionLink = true,
  onNode,
  onEvidence,
  resolveStatement,
}: {
  outcome: LibraryOutcome;
  /** Statement of another outcome on the same page, to name what superseded this one. */
  resolveStatement?: (semanticAtomId: string) => string | null | undefined;
  /** Off where the surrounding card already links this outcome's version session. */
  sessionLink?: boolean;
  onNode: (id: string, graph: string, source?: string, version?: string) => void;
  onEvidence: (
    source: string,
    graph: string,
    version: string,
    atom: string,
    passage: string,
  ) => void;
}) {
  const supersession = parseSupersession(outcome.supersession);
  return (
    <Card className={supersession ? "border-dashed" : undefined}>
      <div className="flex flex-wrap items-center gap-2">
        {supersession && <Verdict tone="muted" glyph="↺" title="A later recorded fact replaced this one. It is history, not the current state.">superseded</Verdict>}
        <Chip
          hue={outcome.kind === "decision" ? "coral" : outcome.kind === "action" ? "amber" : "teal"}
        >
          {outcome.kind}
        </Chip>
        <Chip>{outcome.source_version_is_current === undefined ? "Capture pointer status unknown" : outcome.source_version_is_current ? "Current capture" : "Earlier retained capture"}</Chip>
        {outcome.origin?.transport === "synthetic" && <Badge tone="warn">Synthetic demo</Badge>}
        <span className="text-xs text-ink-3">
          {outcome.origin?.service_id || "Origin unknown"} · Captured{" "}
          {dateLabel(outcome.captured_at)}
        </span>
      </div>
      <p className={`mt-3 whitespace-pre-wrap break-words text-sm leading-7 ${supersession ? "text-ink-2" : "text-ink"}`}>
        {outcome.statement}
      </p>
      {supersession && <p className="mt-1 text-xs text-ink-2">{supersessionLine(supersession, resolveStatement)}</p>}
      <p className="mt-2 text-xs text-ink-3">Source event: {dateLabel(outcome.occurred_at)}</p>
      {sessionLink && outcome.session && <p className="mt-2"><SessionLink session={outcome.session} focus={`outcome-session:${outcome.semantic_atom_id}`} /></p>}
      {outcome.nodes.length > 0 && (
        <div className="mt-3 flex flex-wrap gap-x-4 gap-y-2">
          {outcome.nodes.map((node) => (
            <button
              type="button"
              key={node.node_id}
              data-library-focus={`outcome-node:${outcome.semantic_atom_id}:${node.node_id}`}
              className={LINK}
              onClick={() => onNode(node.node_id, outcome.graph_id, outcome.source_item_id, outcome.source_version_id)}
            >
              {nodeLabel(node.title)}
            </button>
          ))}
        </div>
      )}
      {outcome.nodes_truncated && (
        <p className="mt-2 text-xs text-ink-3">
          Additional readable node links were omitted from this response.
        </p>
      )}
      <details className="mt-4 border-t border-line-2 pt-3">
        <summary data-library-focus={`outcome:${outcome.semantic_atom_id}`} className="cursor-pointer rounded-row text-sm text-brand-2 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand">
          Inspect exact evidence · {outcome.citations.length} passage
          {outcome.citations.length === 1 ? "" : "s"}
          {outcome.citations_truncated ? " shown" : ""}
        </summary>
        <div className="mt-3 space-y-4">
          {outcome.citations.map((citation) => (
            <div key={citation.passage_id}>
              <blockquote className="whitespace-pre-wrap break-words border-l-2 border-brand bg-brand-soft p-3 text-sm leading-7 text-ink">
                {citation.quote}
              </blockquote>
              {citation.quote_truncated && (
                <p className="mt-2 text-xs text-ink-2">
                  Beginning of the retained passage; this quote preview is shortened.
                </p>
              )}
              <p className="my-2 font-mono text-xs text-ink-3">
                {citation.representation.replaceAll("_", " ")} · Source event{" "}
                {dateLabel(citation.occurred_at)} · Captured {dateLabel(citation.captured_at)}
              </p>
              <button
                type="button"
                data-library-focus={`passage:${outcome.semantic_atom_id}:${citation.passage_id}`}
                className={LINK}
                onClick={() =>
                  onEvidence(
                    citation.source_item_id,
                    citation.graph_id,
                    citation.source_version_id,
                    citation.semantic_atom_id,
                    citation.passage_id,
                  )
                }
              >
                Read surrounding captured text
              </button>
            </div>
          ))}
        </div>
        {outcome.citations_truncated && (
          <p className="mt-3 text-xs text-ink-3">
            More readable passage references exist than this response includes.
          </p>
        )}
        {outcome.citations.length === 0 && (
          <p className="mt-3 text-sm text-ink-2">
            No readable exact passage was returned for this outcome.
          </p>
        )}
      </details>
    </Card>
  );
}

