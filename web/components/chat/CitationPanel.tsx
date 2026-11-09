"use client";
import { DetailPanel } from "../detail/DetailPanel";
import { fromCitation } from "../detail/detailModel";
import { evidenceSources } from "./evidence";
import { citationHref } from "@/lib/provenance";
import type { AssistantTurn } from "./types";
export function CitationPanel({
  turn,
  n,
  onClose = () => {},
}: {
  turn: AssistantTurn;
  n: number;
  onClose?: () => void;
}) {
  const source = evidenceSources(turn).find((s) => s.n === n);
  return source ? (
    <DetailPanel
      item={fromCitation(source, turn)}
      sourceHref={citationHref(turn, n)}
      onClose={onClose}
    />
  ) : null;
}
