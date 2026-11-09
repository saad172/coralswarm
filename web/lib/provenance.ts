import type { AssistantTurn } from "@/components/chat/types";

/** Evidence IDs identify stored passages; citation ordinals are only display positions. */
export function evidenceHref(atomId: string, graphId?: string | null): string {
  const params = new URLSearchParams({ id: atomId });
  if (graphId) params.set("graph_id", graphId);
  return `/coral?${params}`;
}

export function meetingHref(sourceId: string, graphId: string): string {
  return `/meeting?${new URLSearchParams({ source_id: sourceId, graph_id: graphId })}`;
}

/** Semantic atoms live with their preserved source version, not at /coral
 * (whose IDs still address retrieval chunks). */
export function meetingEvidenceHref(sourceId: string, graphId: string, versionId: string, atomId?: string, passageId?: string): string {
  const params = new URLSearchParams({ source_id: sourceId, graph_id: graphId, version_id: versionId });
  if (atomId) params.set("semantic_atom_id", atomId);
  if (passageId) params.set("passage_id", passageId);
  return `/meeting?${params}#meeting-evidence`;
}

/** A source-item UUID retains provider/workspace identity even when native IDs collide. */
export function sourceEvidenceHref(sourceItemId: string, graphId: string, versionId?: string | null, atomId?: string, passageId?: string): string {
  const params = new URLSearchParams({ source_item_id: sourceItemId, graph_id: graphId });
  if (versionId) params.set("version_id", versionId);
  if (atomId) params.set("semantic_atom_id", atomId);
  if (passageId) params.set("passage_id", passageId);
  return `/source?${params}#source-evidence`;
}

/** Never borrow an ambient workspace: an answer can cite several workspaces.
 * Legacy bare IDs remain navigable because the evidence endpoint resolves its owner. */
export function citationHref(turn: Pick<AssistantTurn, "citationDetails" | "citations" | "sources"> & Partial<Pick<AssistantTurn, "sourceOceans" | "retainedCitations" | "legacyEvidence" | "supportStatus">>, n: number): string | undefined {
  if (!Number.isInteger(n) || n < 1) return undefined;
  if (turn.retainedCitations !== undefined) {
    if (turn.supportStatus !== "complete") return undefined;
    const citation = turn.retainedCitations[n - 1];
    if (citation) return sourceEvidenceHref(citation.source_item_id, citation.graph_id, citation.source_version_id, citation.semantic_atom_id, citation.passage_id);
    const legacy = turn.legacyEvidence?.[n - turn.retainedCitations.length - 1];
    return legacy ? evidenceHref(legacy.atom_id, legacy.graph_id) : undefined;
  }
  const detail = turn.citationDetails[n - 1];
  if (detail?.atom_id) return evidenceHref(detail.atom_id, detail.graph_id);
  const atomId = turn.citations[n - 1];
  if (atomId) return evidenceHref(atomId, turn.sourceOceans?.[atomId]);
  // Sparse streamed details reserve their ordinal, even before the evidence arrives.
  if (turn.citationDetails.length) return undefined;
  const source = turn.sources[n - 1];
  return source?.atom_id ? evidenceHref(source.atom_id, source.ocean_id) : undefined;
}
