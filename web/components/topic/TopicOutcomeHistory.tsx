"use client";

import type { ComponentProps } from "react";
import type { LibraryOutcome } from "@/lib/libraryMemoryApi";
import { splitBySupersession } from "@/lib/graphSurfaces";
import { OutcomeRow } from "./TopicOutcomeRow";

type RowProps = ComponentProps<typeof OutcomeRow>;

/** One page of Library outcomes: current facts first, then facts a later
 *  recorded fact superseded (#1346) under a "history" heading. The split is
 *  within this page only. Without `supersession` on the wire (every read
 *  while FACT_SUPERSESSION_MODE is not live) this renders exactly the plain
 *  list, in server order. */
export function OutcomeHistoryList({ outcomes, onNode, onEvidence }: {
  outcomes: LibraryOutcome[];
  onNode: RowProps["onNode"];
  onEvidence: RowProps["onEvidence"];
}) {
  const { current, superseded } = splitBySupersession(outcomes);
  const statements = new Map(outcomes.map((o) => [o.semantic_atom_id.toLowerCase(), o.statement]));
  const resolve = (id: string) => statements.get(id.toLowerCase());
  return (
    <>
      {current.map((outcome) => (
        <OutcomeRow key={outcome.semantic_atom_id} outcome={outcome} onNode={onNode} onEvidence={onEvidence} />
      ))}
      {superseded.length > 0 && (
        <section aria-label="Fact history" className="space-y-3 pt-2">
          <h3 className="font-mono text-[10px] uppercase tracking-[0.13em] text-ink-3">
            History · {superseded.length} superseded fact{superseded.length === 1 ? "" : "s"}
          </h3>
          {superseded.map((outcome) => (
            <OutcomeRow key={outcome.semantic_atom_id} outcome={outcome} onNode={onNode} onEvidence={onEvidence} resolveStatement={resolve} />
          ))}
        </section>
      )}
    </>
  );
}
