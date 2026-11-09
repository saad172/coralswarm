import type { SourceEvidencePage } from "../api";
import { assertEvidenceResponse, parseP6LegacyCapture, type LegacyCaptureProjection } from "../p6EvidenceApi";
import type { SectionParams } from "../pageSectionWire";

export interface CaptureDetailPayload { summary: unknown | null; evidence: SourceEvidencePage | null }
export interface CaptureSelectors {
  graphId: string; sourceItemId?: string; sourceId?: string;
  versionId?: string; atomId?: string;
}
export function captureSectionParams(args: CaptureSelectors): SectionParams {
  return { graph: args.graphId, source: args.sourceItemId ?? args.sourceId ?? "", kind: args.sourceItemId ? "source" : "meeting",
    version_id: args.versionId, atom_id: args.atomId };
}

/** The meeting's narrow source mapping proves which source the evidence may
 * describe. It never admits an unattributed summary or correction overlay. */
export function captureDetailPayload(value: unknown, params: SectionParams): CaptureDetailPayload {
  if (!value || typeof value !== "object" || !("summary" in value) || !("evidence" in value)) throw new Error("This retained capture could not be verified.");
  const payload = value as CaptureDetailPayload;
  if ((params.version_id || params.atom_id) && payload.evidence === null) throw new Error("The selected retained evidence is unavailable.");
  const graphId = String(params.graph ?? params.graph_id ?? "");
  const source = String(params.source ?? "");
  let item = source;
  if (params.kind === "meeting") {
    const summary = payload.summary === null ? null : parseP6LegacyCapture(payload.summary, { graphId, id: source });
    if (payload.evidence !== null) {
      item = payload.evidence.source_item_id;
      if (!summary?.source_items.some((pointer) => pointer.source_item_id === item)) throw new Error("This retained evidence does not belong to the selected capture.");
    }
  } else if (payload.summary !== null) throw new Error("This source has an unexpected capture summary.");
  if (payload.evidence !== null) assertEvidenceResponse(payload.evidence, { graphId, sourceItemId: item,
    ...(params.version_id ? { versionId: String(params.version_id) } : {}), ...(params.atom_id ? { atomId: String(params.atom_id) } : {}) });
  return payload;
}

export function captureSummary(payload: CaptureDetailPayload, graphId: string, sourceId: string): LegacyCaptureProjection | null {
  return payload.summary === null ? null : parseP6LegacyCapture(payload.summary, { graphId, id: sourceId });
}
