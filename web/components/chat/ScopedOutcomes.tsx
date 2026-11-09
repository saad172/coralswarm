"use client";

import type { ScopedOutcome } from "@/lib/scopedChatApi";
import { parseSupersession, splitBySupersession, supersessionLine } from "@/lib/graphSurfaces";
import { Verdict } from "@/components/ui";

/** The sourced outcomes behind a scoped answer. Current facts first; facts a
 *  later recorded fact superseded (#1346) follow as history, never mixed in
 *  as current state. With no `supersession` on the wire (every answer while
 *  FACT_SUPERSESSION_MODE is not live) this is the plain list it always was. */
export function ScopedOutcomes({ outcomes }: { outcomes: ScopedOutcome[] }) {
  if (outcomes.length === 0) return null;
  const { current, superseded } = splitBySupersession(outcomes);
  const statements = new Map(outcomes.map((o) => [o.semantic_atom_id.toLowerCase(), o.statement]));
  const resolve = (id: string) => statements.get(id.toLowerCase());
  return (
    <details className="text-sm">
      <summary className="cursor-pointer rounded-row text-brand-2 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand">
        Inspect sourced outcomes · {outcomes.length}
        {superseded.length > 0 && ` · ${superseded.length} superseded`}
      </summary>
      <div className="mt-3 space-y-3">
        {current.map((outcome) => <OutcomeCard key={outcome.semantic_atom_id} outcome={outcome} />)}
      </div>
      {superseded.length > 0 && (
        <section aria-label="Fact history" className="mt-4 border-t border-line-2 pt-3">
          <h4 className="mb-2 font-mono text-[10px] uppercase tracking-[0.13em] text-ink-3">History · superseded facts</h4>
          <div className="space-y-3">
            {superseded.map((outcome) => {
              const s = parseSupersession(outcome.supersession);
              return <OutcomeCard key={outcome.semantic_atom_id} outcome={outcome} history={s ? supersessionLine(s, resolve) : "Superseded by a later fact"} />;
            })}
          </div>
        </section>
      )}
    </details>
  );
}

function OutcomeCard({ outcome, history }: { outcome: ScopedOutcome; history?: string }) {
  return (
    <div id={`outcome-${outcome.semantic_atom_id}`} className={history ? "rounded-row border border-dashed border-line p-3" : "rounded-row border border-line-2 p-3"}>
      <div className="flex flex-wrap items-center gap-2">
        {history && <Verdict tone="muted" glyph="↺">superseded</Verdict>}
        <p className="text-xs text-ink-3">{outcome.kind} · {outcome.source_version_is_current ? "Current pointer at retrieval" : "Earlier capture at retrieval"}{outcome.synthetic_demo ? " · Synthetic demo" : ""}</p>
      </div>
      <p className={history ? "mt-1 whitespace-pre-wrap break-words leading-7 text-ink-2" : "whitespace-pre-wrap break-words leading-7"}>{outcome.statement}</p>
      {history && <p className="mt-1 text-xs text-ink-2">{history}</p>}
      {outcome.statement_truncated && <p className="text-xs text-ink-3">Statement preview shortened.</p>}
      <p className="mt-2 text-xs text-ink-3">{outcome.representation.replaceAll("_", " ")} · Source event: {outcome.occurred_at || "Unknown"} · Captured: {outcome.captured_at}</p>
    </div>
  );
}
