"use client";

// Settings › Privacy (ticket 02, D6/D7/D8; ticket 09 Q15=(c)).
//
// A PERSISTED surface, never a dismissable modal alone — Q15 rejected that
// explicitly, verbatim: "out_of_window remains findable rather than shown
// once and dismissed". Reads `GET /v1/retractions` (`routes::retractions`,
// built separately from B2 unit 02/#718 for exactly this page), whose RLS
// already carries D8's full visibility rule (author, every ocean; admin of
// a shared ocean a row touched; personal rows author-only) — this page asks
// for nothing narrower than "what can I see".

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { ApiError, type Graph, type RetractionEntry } from "@/lib/api";
import { retractionsQueryOptions } from "@/lib/appQueries";
import { Badge, Button, Card, Empty, ErrorNote, Loading, Mono, Select } from "@/components/ui";
import { SettingsShell } from "@/components/settings/SettingsShell";
import { SettingsHeading } from "@/components/settings/SettingsHeading";
import { ContentProcessingDisclosure } from "@/components/settings/ContentProcessingDisclosure";
import { RetractionOceansList } from "@/components/retraction/RetractionRowsList";
import { fmtWhen } from "@/lib/meetingFormat";
import { D8_READERS, entryOutcomeSummary, sourceTypeLabel } from "@/lib/retraction";

const PAGE_SIZE = 20;

/** Independent review of PR #729, finding 13 (LOW): this page used to own
 *  its OWN `SOURCE_TYPE_LABELS`, keyed with SURFACE values (`"content"`,
 *  `"journal_rollup"`) instead of real `source_type` values — two of its
 *  four entries were dead code, and real source types (`"slack"`,
 *  `"coding_session"`, `"gmail"`, …) fell through unlabeled. `lib/
 *  retraction.ts` already owns the surface/outcome vocabulary split; the
 *  source-type map now lives there beside them, not on this page.
 *
 *  `entry.rows[0]` is still an arbitrary PICK among a group's rows — that
 *  half of the finding is fixed at the source (`services::retraction_audit::
 *  rows_for_retraction_ids`'s `ORDER BY` now carries a deterministic
 *  `(surface, source_id)` tiebreak, so this pick is at least STABLE across
 *  re-reads of the same data now, not merely "whichever row Postgres
 *  happened to return first"). */
function entryTitle(entry: RetractionEntry): string {
  const type = entry.rows[0]?.source_type;
  return type ? sourceTypeLabel(type) : "retraction";
}

/* The per-entry verdict badge moved to `lib/retraction.ts`
   (`entryOutcomeSummary`) so its ZERO-ROW case is covered by a test. The
   version that lived here computed `unreached === 0 -> "fully removed"`,
   which an EMPTY `rows` array satisfies vacuously — a green badge from no
   evidence, on the page whose whole job is to show what was NOT reached. */

function EntryCard({ entry, graphs }: { entry: RetractionEntry; graphs: Graph[] }) {
  const [open, setOpen] = useState(false);
  const oceanCount = new Set(entry.rows.map((r) => r.graph_id)).size;
  const summary = entryOutcomeSummary(entry.rows);
  return (
    <Card flush className={`overflow-hidden ${summary.clean ? "" : "border-warn/40"}`}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        className="flex w-full items-center gap-[10px] px-[14px] py-[12px] text-left transition-colors hover:bg-bg-hover"
      >
        <span className="min-w-0 flex-1">
          <span className="block truncate text-[13px] font-medium capitalize text-ink">
            {entryTitle(entry)}
            {/* Finding 4 (BLOCKING): D8 admits an org admin to entries that
                are NOT their own (a shared workspace's OTHER members' rows) —
                without this, every visible entry read as "yours". No name
                lookup on the wire (see `RetractionEntry.is_own`'s own doc);
                distinguishing own-vs-other is the load-bearing fix. */}
            {!entry.is_own && (
              <span className="ml-[8px] normal-case font-mono text-[10px] tracking-normal text-ink-4">
                by another member
              </span>
            )}
          </span>
          <Mono className="mt-[3px] block">
            {entry.retracted_at ? fmtWhen(entry.retracted_at) : "unknown time"} · {oceanCount} workspace
            {oceanCount === 1 ? "" : "s"}
          </Mono>
        </span>
        <Badge tone={summary.clean ? "ok" : "warn"} className="flex-none">
          {summary.text}
        </Badge>
        <span className="flex-none text-[11px] text-ink-4">{open ? "▾" : "▸"}</span>
      </button>
      {open && (
        <div className="border-t border-line-3 bg-bg-subtle p-[10px]">
          <RetractionOceansList rows={entry.rows} graphs={graphs} />
          {entry.truncated && (
            <p className="mt-[8px] font-mono text-[10.5px] text-ink-3">
              Showing the first rows only — this retraction reached more than fit here.
            </p>
          )}
        </div>
      )}
    </Card>
  );
}

export default function RetractionHistoryPage() {
  const { graphs, authReady, getAuthToken, queryScope } = useApp();
  const [page, setPage] = useState(1);
  // The artboard's own header control ("Not fully removed ▾"). Applied
  // SERVER-side (`?unreached_only=`) rather than over the loaded page:
  // filtering page 1 alone would answer "nothing left behind" while page 3
  // still held an `out_of_window` row, which is the exact
  // absence-renders-as-success shape this surface exists to prevent — and
  // Q15=(c)'s stated reason for persisting it at all is that
  // `out_of_window` stays FINDABLE.
  const [unreachedOnly, setUnreachedOnly] = useState(false);
  // Each (page, filter) is its own cache key, so a slow response for a
  // previous selection lands in that selection's entry and can never
  // repopulate the list under the one the user is looking at (the race a
  // hand-rolled request-id guard used to cover here).
  const query = useQuery({
    ...retractionsQueryOptions({ getAuthToken, scope: queryScope }, page, PAGE_SIZE, unreachedOnly),
    enabled: authReady,
  });
  // Independent review of PR #729, finding 12 (MEDIUM): a failure must not
  // leave an ErrorNote and an endless spinner together. An error with no data
  // reads as an empty list; the error (with its Retry) explains why.
  const entries: RetractionEntry[] | null = query.data?.entries ?? (query.isError ? [] : null);
  const hasMore = (query.data?.entries.length ?? 0) === PAGE_SIZE;
  const err = query.error ? (query.error instanceof ApiError ? query.error.message : "Couldn't load retraction history.") : null;

  return (
    <SettingsShell>
      <SettingsHeading title="Privacy">
        {/* Finding 4 (BLOCKING): D8 deliberately admits an org admin to
            entries OTHER members made in a shared workspace this account touches
            — the previous "Everything YOU have retracted" claimed every
            visible entry as the viewer's own act. `EntryCard` above marks
            each non-own entry "by another member"; this line now matches. */}
        Every retraction you have made, plus — for a shared workspace you administer — retractions
        made there by other members. A copy that could not be removed stays on this page; it is not
        shown once and then dismissed.
      </SettingsHeading>

      {/* The artboard's header filter. `Not fully removed` is the reason
          this page is persisted at all (Q15=(c)) — without a way to ask for
          them, an `out_of_window` row from three months ago is "findable"
          only by paging through everything since. */}
      <div className="mb-[14px] flex items-center gap-[10px]">
        <Select
          aria-label="Filter retractions"
          wrapperClassName="w-[220px]"
          value={unreachedOnly ? "unreached" : "all"}
          onChange={(e) => {
            // Page 1 on every filter change: page 4 of the unfiltered list
            // is almost never page 4 of the filtered one, and landing on an
            // empty page would read as "no unreached copies".
            setPage(1);
            setUnreachedOnly(e.target.value === "unreached");
          }}
        >
          <option value="all">All retractions</option>
          <option value="unreached">Not fully removed</option>
        </Select>
        {unreachedOnly && (
          <Mono className="text-ink-3">showing only retractions that left a copy behind</Mono>
        )}
      </div>

      {err && (
        <ErrorNote className="mb-[14px]" action={<Button size="sm" variant="secondary" onClick={() => void query.refetch()}>Retry</Button>}>
          {err}
        </ErrorNote>
      )}

      {entries === null ? (
        <Loading what="your retraction history" />
      ) : entries.length === 0 ? (
        // Finding 11 (MEDIUM): distinguish "you've never retracted anything"
        // from "you've paged past the end" (exactly `PAGE_SIZE` retractions
        // makes `hasMore` true, and page 2 legitimately comes back empty) —
        // the pager below now renders whenever `page > 1` regardless of this
        // page's own row count, so "Previous" is always reachable, but the
        // copy here should not claim "nothing retracted yet" when the truth
        // is just "not on THIS page".
        <Empty
          title={
            page > 1
              ? "No more retractions on this page"
              : unreachedOnly
                ? "Nothing matched “Not fully removed”"
                : "Nothing retracted yet"
          }
          hint={
            page > 1
              ? "You've paged past the end of your history — go back to see it."
              : unreachedOnly
                ? // An empty FILTERED list has two causes and this page cannot
                  // tell them apart without a second request — so it names
                  // both rather than picking the flattering one. "Every
                  // retraction was clean" and "you have never retracted
                  // anything" render identically here, and a reader who is
                  // not told that will read the first.
                  "Either every retraction you can see was fully removed, or you have not retracted anything yet — this filter cannot tell those apart. Switch to All retractions to see which."
                : "Retract an item or a session and it will show up here — reached and unreached copies both."
          }
        />
      ) : (
        <div className="flex flex-col gap-[9px]">
          {entries.map((e) => (
            <EntryCard key={e.retraction_id} entry={e} graphs={graphs} />
          ))}
        </div>
      )}

      {/* Finding 11: no longer gated on `entries.length > 0` — an empty page
          2 (or later) must still offer "Previous", or the user is stranded
          on an empty page with no way back to page 1. */}
      {entries !== null && (page > 1 || hasMore) && (
        <div className="mt-[14px] flex items-center gap-[8px]">
          <Button
            variant="secondary"
            size="sm"
            disabled={page <= 1}
            onClick={() => setPage((p) => Math.max(1, p - 1))}
          >
            Previous
          </Button>
          <Mono>page {page}</Mono>
          <Button variant="secondary" size="sm" disabled={!hasMore} onClick={() => setPage((p) => p + 1)}>
            Next
          </Button>
        </div>
      )}

      <div className="mt-[26px] grid gap-[12px] sm:grid-cols-2">
        <Card>
          <div className="mb-[6px] text-[12.5px] font-medium text-ink">What a record keeps</div>
          <ul className="flex flex-col gap-[6px] text-[11.5px] leading-[1.5] text-ink-2">
            <li>What was retracted, and which session it came from.</li>
            <li>Who retracted it, and when.</li>
            <li>The outcome in every workspace, and on every surface inside it.</li>
            <li>Anything derived from it that is still standing.</li>
          </ul>
        </Card>
        <Card className="border-brand/30">
          <div className="mb-[4px] text-[12.5px] font-semibold text-ink">It never keeps the content</div>
          <p className="text-[11.5px] leading-[1.55] text-ink-2">
            Not the text, and not a fingerprint of the text. A fingerprint would let anyone with the
            database guess back the thing you asked to destroy — this page can prove WHAT was
            retracted, never that a particular sentence was in it.
          </p>
        </Card>
        {/* D8, the other half of this rail. The artboard draws it and the
            page shipped without it — which left "who can read it" answered
            only implicitly, on a surface whose own accepted cost (D8) is
            that an administrator gets a volume-and-timing signal from it.
            Same constant `SessionRetracted` renders, so the two surfaces
            cannot drift apart while describing one RLS policy. */}
        <Card className="sm:col-span-2">
          <div className="mb-[6px] text-[12.5px] font-medium text-ink">Who can read it</div>
          <ul className="flex flex-col gap-[6px] text-[11.5px] leading-[1.5] text-ink-2">
            {D8_READERS.map((text) => (
              <li key={text}>{text}</li>
            ))}
          </ul>
        </Card>
      </div>

      <ContentProcessingDisclosure />
    </SettingsShell>
  );
}
