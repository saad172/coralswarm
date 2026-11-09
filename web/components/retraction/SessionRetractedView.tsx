import Link from "next/link";
import type { Graph, RetractionEntry } from "@/lib/api";
import { Badge, Card, Eyebrow } from "@/components/ui";
import { RetractionOceansList } from "./RetractionRowsList";
import { fmtWhen } from "@/lib/meetingFormat";
import { D8_READERS, whatRemainsText } from "@/lib/retraction";

/* SessionRetracted — a second STATE of SessionDetail, not a new screen
   (ticket 09 Q15=(a)), rendered where the retract act happened. Three
   binding properties (docs/plans/capture/02-b2-retract-fixes.md, "D6 UI"):

     1. Reached and unreached copies render in ONE list (RetractionOceansList
        below already carries this — see D6's own bar, "an unexplained ⚠ is
        worse than the green check it replaced").
     2. The session's CONTENT is absent on purpose — no summary, no quotes.
        Rendering one would be rendering FM-6, the failure this state exists
        to prove is fixed.
     3. Visibility follows D8: the author across every ocean; admins of the
        ocean a row belongs to see who/when/which coral/what outcome, never
        what it said (D7). */

export function SessionRetractedView({
  title,
  entry,
  graphs,
}: {
  /** The session's own title, held in memory from BEFORE the retract call —
   *  the underlying `coding_sessions` row is gone by the time this state
   *  renders (FM-6), so this is the last copy anywhere still has it. */
  title: string;
  entry: RetractionEntry;
  graphs: Graph[];
}) {
  return (
    <div className="flex flex-col gap-[24px]">
      <div>
        <div className="mb-[12px] flex flex-wrap items-center gap-[9px]">
          <Badge tone="ink">retracted</Badge>
          {entry.retracted_at && <Eyebrow as="span">{fmtWhen(entry.retracted_at)}</Eyebrow>}
        </div>
        <h1 className="font-display text-[34px] font-normal leading-[1.2] tracking-[-0.01em] text-ink">
          {title}
        </h1>
      </div>

      <section>
        <h2 className="mb-[6px] font-display text-[22px] font-normal leading-[1.25] tracking-[-0.01em] text-ink">
          Where this <em className="italic text-brand">reached</em>
        </h2>
        <p className="mb-[16px] max-w-[66ch] text-[13.5px] leading-[1.6] text-ink-2">
          Every place this was copied to, and what happened in each. Copies that were removed and
          copies that were not are listed together, because the second list is the one that matters.
        </p>
        <RetractionOceansList rows={entry.rows} graphs={graphs} />
      </section>

      <section className="grid gap-[14px] md:grid-cols-2">
        <Card>
          <Eyebrow className="mb-[8px]">what remains</Eyebrow>
          {/* Finding 2 (independent review of PR #729, BLOCKING): this used
              to be a STATIC string asserting total removal unconditionally —
              rendered directly beneath rows (above) that can prove it false.
              `whatRemainsText` computes this from `entry.rows`, the same
              source `RetractionOceansList` above renders — see that
              function's own doc in `lib/retraction.ts`. */}
          <p className="text-[12.5px] leading-[1.6] text-ink-2">{whatRemainsText(entry.rows)}</p>
        </Card>
        <Card>
          <Eyebrow className="mb-[8px]">who can see this record</Eyebrow>
          {/* D8's three clauses, from the SAME constant Settings ›
              Retraction history renders — the two surfaces are describing
              one RLS policy, so a divergence between them would be a lie on
              at least one of them. The third clause ("nobody else") was
              missing here: it is the one that protects the person
              retracting, and a two-bullet list silently drops it. */}
          <ul className="flex flex-col gap-[8px] text-[11.5px] leading-[1.5] text-ink-2">
            {D8_READERS.map((text) => (
              <li key={text}>{text}</li>
            ))}
          </ul>
        </Card>
      </section>

      <Card className="text-[12.5px] leading-[1.55] text-ink-2">
        <span className="font-medium text-ink">This report does not expire.</span> It is kept in{" "}
        <Link href="/settings/privacy" className="text-brand-2 hover:underline">
          Settings › Privacy
        </Link>{" "}
        with every other retraction you have made, so the copies that could not be removed stay
        findable rather than being shown once and dismissed.
      </Card>
    </div>
  );
}
