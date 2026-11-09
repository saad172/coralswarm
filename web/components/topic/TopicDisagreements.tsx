"use client";

import type { ReactNode } from "react";
import { scopeRefused } from "@/lib/api";
import { readLibraryDisagreements, type LibraryDisagreementPage, type LibraryDisagreementSide, type LibraryEffectiveInterval } from "@/lib/libraryHistoryApi";
import { nodeLabel } from "@/lib/topicLabels";
import { useLibraryRead } from "@/lib/useLibraryRead";
import { Button, ErrorNote, Loading } from "@/components/ui";
import { OutcomeRow } from "./TopicOutcomeRow";

type OnNode = (id: string, graph: string, source?: string, version?: string) => void;
type OnEvidence = (source: string, graph: string, version: string, atom: string, passage: string) => void;
const LINK = "min-w-0 max-w-full rounded-row break-words text-left text-brand-2 underline underline-offset-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand";
const dateLabel = (value: string) => new Date(value).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });

function statement(side: LibraryDisagreementSide): string {
  return side.title !== null ? nodeLabel(side.title) : side.evidence[0]?.outcome.statement || "Statement text unavailable";
}
function interval(e: LibraryEffectiveInterval): string {
  if (e.operation === "withdraw") return `${e.state_key}: withdrawn, ${e.valid_from ? `from ${dateLabel(e.valid_from)}` : "start not recorded"}`;
  return `${e.state_key}${e.operation === "assert" ? "" : ` (${e.operation})`}: ${e.value ?? "no value"}, ${e.valid_from ? `from ${dateLabel(e.valid_from)}` : "start not recorded"}, ${e.valid_until ? `until ${dateLabel(e.valid_until)}` : "end not recorded"}`;
}
function withheldSentence({ count, complete }: LibraryDisagreementPage["withheld"]): string {
  const n = `${complete ? "" : "At least "}${count}`;
  return count === 1 && complete
    ? "1 unresolved disagreement involves a side you cannot read. Its statement and evidence are withheld."
    : `${n} unresolved disagreements involve sides you cannot read. Their statements and evidence are withheld.`;
}

function SideView({ side, heading, graph, onNode, onEvidence }: { side: LibraryDisagreementSide; heading: ReactNode; graph: string; onNode: OnNode; onEvidence: OnEvidence }) {
  return <div className="min-w-0 space-y-3">
    <h3 className="font-display text-xl font-normal text-ink">{heading}</h3>
    {side.evidence.length === 0 && <p className="text-sm text-ink-2">No retained sourced statement is readable for this side.</p>}
    {side.evidence.map((e) => <div key={e.outcome.semantic_atom_id} className="space-y-2">
      <OutcomeRow outcome={e.outcome} onNode={onNode} onEvidence={onEvidence} />
      <dl className="grid gap-2 text-sm text-ink-2 sm:grid-cols-2">
        <div><dt className="font-medium text-ink">Effective time</dt><dd>{e.effective_time.length === 0 ? "Not recorded. Capture and source dates do not establish it." : <ul>{e.effective_time.map((t) => <li key={`${t.passage_id}:${t.state_key}:${t.recorded_at}`}>{interval(t)}</li>)}</ul>}{e.effective_time_truncated && " More recorded intervals are omitted."}</dd></div>
        <div><dt className="font-medium text-ink">Knowledge time</dt><dd>{e.knowledge_time ? `First recorded by CoralSwarm ${dateLabel(e.knowledge_time)}` : "Not recorded for this retained version."}</dd></div>
      </dl>
    </div>)}
    {side.evidence_truncated && <p className="text-xs text-ink-3">More readable outcomes support this side. <button type="button" className={LINK} onClick={() => onNode(side.node_id, graph)}>Open this node</button> to read them.</p>}
  </div>;
}

export function TopicDisagreements({ identity, getToken, graphId, nodeId, after, onPage, onNode, onEvidence, onUnavailable }: {
  identity: string;
  getToken: () => string | null;
  graphId: string;
  nodeId: string;
  after?: string | null;
  onPage: (cursor: string | null) => void;
  onNode: OnNode;
  onEvidence: OnEvidence;
  onUnavailable?: (message: string) => void;
}) {
  const read = useLibraryRead(JSON.stringify([identity, graphId, nodeId, after]), !!getToken(), async (signal) => {
    const refuse = (message: string): never => {
      if (!signal.aborted) onUnavailable?.(message);
      throw new Error(message);
    };
    const token = getToken();
    if (!token) return refuse("Verify the current Library account.");
    const result = await readLibraryDisagreements(token, { graph_id: graphId, node_id: nodeId, page_size: 5, ...(after ? { after } : {}) }, signal).catch((error: unknown) => {
      if (!signal.aborted && scopeRefused(error)) onUnavailable?.(error.message);
      throw error;
    });
    return getToken() ? result : refuse("The Library account changed. Retry the disagreement.");
  });
  const page = read.data;
  const nextCursor = page?.next_cursor;
  return <section aria-label="Unresolved disagreement" className="space-y-4 border-t border-line pt-6">
    <h2 className="font-display text-2xl font-normal text-ink">Unresolved disagreement</h2>
    <p className="max-w-prose text-sm leading-6 text-ink-2">Each side is shown with its own sourced evidence. Neither side has been treated as resolved; a newer capture does not settle the disagreement.</p>
    {read.loading && <Loading what="verifying the disagreement" />}
    {(read.error ?? read.refreshError) && <ErrorNote action={<Button onClick={read.refresh}>Retry disagreement</Button>}>{read.error ?? read.refreshError}</ErrorNote>}
    {page && <>
      <SideView side={page.anchor} heading={<>This node: {statement(page.anchor)}</>} graph={page.graph_id} onNode={onNode} onEvidence={onEvidence} />
      {page.items.length > 0 && <ul className="divide-y divide-line border-y border-line">
        {page.items.map((d) => <li key={d.other.node_id} className="space-y-2 py-4">
          <SideView side={d.other} graph={page.graph_id} onNode={onNode} onEvidence={onEvidence} heading={<>Disagrees with: <button type="button" className={LINK} onClick={() => onNode(d.other.node_id, page.graph_id)}>{statement(d.other)}</button></>} />
          <p className="text-xs text-ink-3">Disagreement recorded {dateLabel(d.recorded_at)}</p>
        </li>)}
      </ul>}
      {page.withheld.count > 0 && <p className="text-sm text-ink-2">{withheldSentence(page.withheld)}</p>}
    </>}
    <div className="flex flex-wrap gap-2">
      {nextCursor && <Button onClick={() => onPage(nextCursor)}>Next disagreements</Button>}
      {after && <Button variant="ghost" onClick={() => onPage(null)}>First disagreements</Button>}
    </div>
  </section>;
}
