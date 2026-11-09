"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import { Avatar, Eyebrow, FacetRow, hueVar, type Hue } from "@/components/ui";
import type { ReefAtom, ReefDigest } from "@/lib/api";
import { fmtDate, isRawSlackUserId, srcName } from "@/lib/coral";
import { hueForSource, iconKindFor, facetLabelFor, SourceGlyph } from "@/components/library/sourceKind";

/* "How it was reached" — the reef's coral timeline, day-grouped, filterable by
   source type, capped with a "show N earlier" expander. Ported from
   archive/web-legacy-ui/screens/reef.page.tsx's data flow (digest excerpt →
   ocean-wide preview → neutral type label, in that order), restyled onto the
   mock's left-rule-with-dot timeline card. */

function participantDisplay(p: string): { avatarName: string; label: string } {
  return isRawSlackUserId(p) ? { avatarName: "?", label: "Unknown participant" } : { avatarName: p, label: p };
}

function atomLine(
  a: ReefAtom,
  digest: ReefDigest | null,
  previewByAtomId: Map<string, string>,
): { hue: Hue; body: string; meta: string } {
  const hue = hueForSource(a.source_type);
  const d = digest?.excerpts.find((e) => e.atom_id === a.atom_id);
  // `ReefAtom` (`GET /v1/nodes/{id}/atoms`) itself carries no title — the
  // digest's per-atom "why this atom matters to this reef" contribution is
  // preferred first (richer than a bare title for "how it was reached"), then
  // `previewByAtomId`, which the caller (topics/page.tsx) now builds from the
  // ocean-wide corals' own API `title` (title-always contract, via
  // `coralTitle`'s explicit-title preference) rather than a body heuristic —
  // so this fallback is already the real title, not a derived snippet.
  const body = d?.contribution || d?.excerpt || previewByAtomId.get(a.atom_id) || `${srcName(a.source_type)} item`;
  const person = a.participants[0];
  const who = person ? participantDisplay(person).label : null;
  const meta = [who, srcName(a.source_type), fmtDate(a.timestamp)].filter(Boolean).join(" · ");
  return { hue, body, meta };
}

const INITIAL_VISIBLE = 7;

export function TopicTimeline({
  atoms,
  digest,
  previewByAtomId,
}: {
  atoms: ReefAtom[];
  digest: ReefDigest | null;
  previewByAtomId: Map<string, string>;
}) {
  const [filter, setFilter] = useState("all");
  const [expanded, setExpanded] = useState(false);

  const sourceTypes = useMemo(() => Array.from(new Set(atoms.map((a) => a.source_type))), [atoms]);
  const filtered = useMemo(
    () =>
      atoms
        .filter((a) => filter === "all" || a.source_type === filter)
        .slice()
        .sort((a, b) => {
          const ta = a.timestamp ? Date.parse(a.timestamp) : -Infinity;
          const tb = b.timestamp ? Date.parse(b.timestamp) : -Infinity;
          return tb - ta;
        }),
    [atoms, filter],
  );
  const visible = expanded ? filtered : filtered.slice(0, INITIAL_VISIBLE);
  const remaining = filtered.length - visible.length;

  const groups: { label: string; atoms: ReefAtom[] }[] = [];
  for (const a of visible) {
    const label = a.timestamp ? fmtDate(a.timestamp) : "undated";
    const last = groups[groups.length - 1];
    if (last && last.label === label) last.atoms.push(a);
    else groups.push({ label, atoms: [a] });
  }

  return (
    <div>
      <div className="mb-[12px] flex items-center gap-[10px]">
        <Eyebrow>how it was reached</Eyebrow>
        <span className="h-px flex-1 bg-line-2" />
        {sourceTypes.length > 1 && (
          <FacetRow
            options={[
              { key: "all", label: "All" },
              ...sourceTypes.map((t) => ({ key: t, label: facetLabelFor(t) })),
            ]}
            value={filter}
            onChange={setFilter}
          />
        )}
      </div>

      <div className="flex flex-col">
        {groups.map((group, gi) => (
          <div key={`${group.label}-${gi}`}>
            <div className="mb-[6px] mt-[16px] font-mono text-[10px] uppercase tracking-[0.12em] text-ink-3 first:mt-0">
              {group.label}
            </div>
            {group.atoms.map((a) => {
              const { hue, body, meta } = atomLine(a, digest, previewByAtomId);
              return (
                // minmax(0,1fr), not 1fr: a `1fr` track carries an implicit
                // `min-width: auto`, so the row's non-shrinking metadata made
                // the column wider than its parent and the timeline rendered
                // ON TOP of the facts/source-mix rail beside it.
                <div key={a.atom_id} className="grid grid-cols-[70px_minmax(0,1fr)] gap-[14px]">
                  <div className="pt-[10px] text-right font-mono text-[10px] text-ink-4">{fmtDate(a.timestamp)}</div>
                  <div className="relative border-l border-line-2 py-0 pb-[10px] pl-[18px]">
                    <span
                      className="absolute -left-[4.5px] top-[15px] h-[8px] w-[8px] rounded-pill"
                      style={{ background: hueVar(hue), boxShadow: "0 0 0 3px var(--bg)" }}
                    />
                    <Link
                      href={`/coral?id=${a.atom_id}`}
                      className="flex items-center gap-[9px] rounded-control border border-line-2 bg-bg px-[13px] py-[10px] transition-colors hover:border-line"
                    >
                      <span className="flex-none" style={{ color: hueVar(hue) }}>
                        <SourceGlyph kind={iconKindFor(a.source_type)} size={14} />
                      </span>
                      <span className="min-w-0 flex-1 truncate text-[13.5px] leading-[1.5] text-ink">{body}</span>
                      <Avatar name={a.participants[0] ? participantDisplay(a.participants[0]).avatarName : "?"} size={16} />
                      {/* Shrinkable, not flex-none: the provenance line can be
                          long (email · source · date) and must give way to the
                          body rather than push the row past its column. */}
                      <span className="hidden min-w-0 shrink truncate font-mono text-[9.5px] text-ink-4 sm:block">
                        {meta}
                      </span>
                    </Link>
                  </div>
                </div>
              );
            })}
          </div>
        ))}
        {remaining > 0 && (
          <button
            type="button"
            onClick={() => setExpanded(true)}
            className="mt-[10px] inline-flex self-start rounded-pill border border-line bg-bg px-[14px] py-[6px] font-mono text-[11px] text-ink-2 transition-colors hover:border-ink-3"
          >
            show {remaining} earlier item{remaining === 1 ? "" : "s"} ↓
          </button>
        )}
      </div>
    </div>
  );
}
