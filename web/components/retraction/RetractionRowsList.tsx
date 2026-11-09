import type { Graph, RetractionRow } from "@/lib/api";
import { Badge, Card, CardHead, Mono } from "@/components/ui";
import {
  NOTHING_REPORTED_BODY,
  NOTHING_REPORTED_TITLE,
  groupByOcean,
  outcomeLabel,
  outcomeReason,
  outcomeTone,
  surfaceLabel,
  survivorsNote,
} from "@/lib/retraction";

/* D6's own bar (docs/plans/capture/02-b2-retract-fixes.md): reached and
   unreached copies render in ONE list, never a success state with a
   footnote — every row here, whatever its outcome, goes through the exact
   same markup. The only thing that varies is the Badge's tone and whether a
   reason line is present (`removed` has none; the other two always do). */

/** One (ocean, surface) row — the atomic unit both `SessionRetracted` and
 *  `RetractionHistory` render. */
export function RetractionSurfaceRow({ row }: { row: RetractionRow }) {
  const reason = outcomeReason(row.outcome);
  const survivors = survivorsNote(row);
  return (
    <div className="flex items-start gap-[11px] border-b border-line-3 px-[14px] py-[10px] last:border-b-0">
      <div className="min-w-0 flex-1">
        <div className="text-[12.5px] font-medium text-ink">{surfaceLabel(row.surface)}</div>
        {reason && <p className="mt-[3px] text-[11.5px] leading-[1.5] text-ink-3">{reason}</p>}
        {/* D7's `survivors[]`, as a count and never as ids. Has no live
            producer today — see `survivorsNote`'s own doc; it is rendered
            so the "what a record keeps" claim on Settings › Retraction
            history has a surface that would actually show one. */}
        {survivors && <p className="mt-[3px] text-[11.5px] leading-[1.5] text-ink-3">{survivors}</p>}
      </div>
      <Badge tone={outcomeTone(row.outcome)} className="flex-none">
        {outcomeLabel(row.outcome)}
      </Badge>
    </div>
  );
}

/** The one state this list must never render as an empty box: the retract
 *  came back with NO (ocean, surface) rows at all.
 *
 *  `RetractionOceansList` used to map over `groupByOcean([])`, which is `[]`
 *  — so a zero-row report rendered a heading ("Where this reached" / "what
 *  that retract reached — reached and unreached copies, together") followed
 *  by literally nothing, which reads as "no problems found". The backend
 *  answers 200 with `surface_outcomes: []` on every already-absent retract
 *  (see `lib/retraction.ts`'s "An EMPTY report is not a clean one"), so this
 *  is a routine response, not an exotic one. */
export function RetractionEmptyReport() {
  return (
    <Card className="border-warn/40">
      <div className="mb-[4px] flex items-center gap-[8px]">
        <Badge tone="warn" className="flex-none">
          nothing reported
        </Badge>
        <span className="text-[12.5px] font-medium text-ink">{NOTHING_REPORTED_TITLE}</span>
      </div>
      <p className="text-[11.5px] leading-[1.55] text-ink-2">{NOTHING_REPORTED_BODY}</p>
    </Card>
  );
}

/** "Where this reached" — one card per ocean, its surfaces listed
 *  underneath, reached and unreached together. This is `SessionRetracted`'s
 *  central list (rendered where the retract act happened, ticket 09
 *  Q15=(a)) and doubles as `RetractionHistory`'s per-entry detail — the
 *  same shape either way, because the content is the same D7 rows either
 *  way. */
export function RetractionOceansList({ rows, graphs }: { rows: RetractionRow[]; graphs: Graph[] }) {
  // Absence is never success — an empty `rows` gets a NAMED state, not an
  // empty container. See `RetractionEmptyReport` above.
  if (rows.length === 0) return <RetractionEmptyReport />;
  const groups = groupByOcean(rows, graphs);
  return (
    <div className="flex flex-col gap-[10px]">
      {groups.map((g) => (
        <Card key={g.graphId} flush className="overflow-hidden">
          <CardHead
            title={g.name}
            action={<Mono>{g.isPersonalOcean ? "only you" : "shared"}</Mono>}
          />
          <div>
            {g.rows.map((row, i) => (
              <RetractionSurfaceRow key={`${row.surface}-${row.atom_id ?? i}`} row={row} />
            ))}
          </div>
        </Card>
      ))}
    </div>
  );
}
