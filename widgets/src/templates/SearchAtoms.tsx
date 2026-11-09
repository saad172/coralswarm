import { useMemo, useState } from "preact/hooks";
import { tokensFor, FONT_MONO, sourceTypeColor, sourceTypeLabel } from "../theme";
import { useToolOutput, useToolInput, useTheme, openExternalLink } from "../openai";
import type { SearchAtomsInput, SearchAtomsOutput, SearchAtomResult } from "../types";
import { Card } from "../components/Card";
import { isToolFailure, ToolFailure } from "../components/ToolFailure";
import { Empty } from "../components/Empty";
import { Working } from "../components/Working";
import { AnswerUi } from "../answerUi";
import { relativeAgeFromDays, shortDate, pluralize } from "../format";
import { libraryHref, coralHref } from "../links";
import { gutter, useHostLayout } from "../layout";

type Bucket = "everything" | "meetings" | "slack" | "coding" | "docs" | "email";

const BUCKET_LABEL: Record<Exclude<Bucket, "everything">, string> = {
  meetings: "Meetings",
  slack: "Slack",
  coding: "Coding",
  docs: "Docs",
  email: "Email",
};

function bucketOf(sourceType: string): Exclude<Bucket, "everything"> {
  switch (sourceType) {
    case "meeting":
    case "transcript":
    case "zoom":
    case "meet":
    case "teams_transcript":
      return "meetings";
    case "slack":
    case "chat":
    case "teams_chat":
    case "teams_channel":
      return "slack";
    case "coding_session":
    case "mcp":
      return "coding";
    case "email":
      return "email";
    default:
      return "docs";
  }
}

export function SearchAtomsTemplate() {
  const output = useToolOutput<SearchAtomsOutput>();
  const input = useToolInput<SearchAtomsInput>();
  const theme = useTheme();
  const t = tokensFor(theme);
  const [bucket, setBucket] = useState<Bucket>("everything");
  const { size } = useHostLayout();
  if (isToolFailure(output)) return <ToolFailure t={t} output={output} tool="search_atoms" />;

  if (!output) {
    return <Working t={t} label="Searching corals…" />;
  }

  const results = output.results ?? [];
  const query = input?.query ?? "";
  // Distinct ocean count among the RETURNED results only — search_atoms has
  // no total-match/oceans-searched count distinct from what it returns; see
  // README "Backend fields needed".
  const oceanCount = new Set(results.map((r) => r.ocean_id).filter((x): x is string => !!x)).size;

  const counts = useMemo(() => {
    const c: Record<Exclude<Bucket, "everything">, number> = { meetings: 0, slack: 0, coding: 0, docs: 0, email: 0 };
    for (const r of results) c[bucketOf(r.source_type)]++;
    return c;
  }, [results]);

  // If the host replaces `results` after the user picked a category (a new
  // search, or a refined one), the previously-selected bucket may no longer
  // have any entries in the new response — falling back to "everything"
  // here (rather than leaving `bucket` stuck pointing at an empty category)
  // means the widget shows the new matches instead of a false "no results"
  // for a category the new response simply doesn't contain.
  const effectiveBucket: Bucket = bucket !== "everything" && counts[bucket] === 0 ? "everything" : bucket;
  const shown = effectiveBucket === "everything" ? results : results.filter((r) => bucketOf(r.source_type) === effectiveBucket);

  const statusParts = [`${results.length} coral${results.length === 1 ? "" : "s"} match “${query}”`];
  if (oceanCount > 0) statusParts.push(`${oceanCount} ocean${oceanCount === 1 ? "" : "s"}`);

  return (
    <Card t={t} status={statusParts.join(" · ")} linkLabel="open library" linkHref={libraryHref()}>
      <div style={{ display: "flex", alignItems: "center", gap: 6, padding: `10px ${gutter(size, 16)}px`, borderBottom: `1px solid ${t.lineSoft}`, flexWrap: "wrap" }}>
        <FilterChip t={t} label="Everything" count={results.length} active={effectiveBucket === "everything"} onClick={() => setBucket("everything")} />
        {(Object.keys(BUCKET_LABEL) as Array<Exclude<Bucket, "everything">>)
          .filter((b) => counts[b] > 0)
          .map((b) => (
            <FilterChip key={b} t={t} label={BUCKET_LABEL[b]} count={counts[b]} active={effectiveBucket === b} dotColor={sourceTypeColor(t, sampleSourceType(b))} onClick={() => setBucket(b)} />
          ))}
      </div>
      {results.length === 0 ? (
        <Empty t={t} />
      ) : shown.length === 0 ? (
        <Empty t={t} title="Nothing matches that filter." body="Try a different category, or clear the filter." />
      ) : (
        shown.map((r) => <ResultRow key={r.atom_id} t={t} r={r} />)
      )}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: `10px ${gutter(size, 16)}px`, fontFamily: FONT_MONO, fontSize: 11, color: t.mutedSoft, background: t.headerBg }}>
        <span>
          {shown.length} of {results.length} shown
        </span>
        <span>sorted by relevance</span>
      </div>
    </Card>
  );
}

function sampleSourceType(b: Exclude<Bucket, "everything">): string {
  switch (b) {
    case "meetings":
      return "meeting";
    case "slack":
      return "slack";
    case "coding":
      return "coding_session";
    case "email":
      return "email";
    case "docs":
      return "document";
  }
}

interface FilterChipProps {
  t: ReturnType<typeof tokensFor>;
  label: string;
  count: number;
  active: boolean;
  dotColor?: string;
  onClick: () => void;
}

function FilterChip({ t, label, count, active, dotColor, onClick }: FilterChipProps) {
  return (
    <span
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") onClick();
      }}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        borderRadius: 999,
        padding: "5px 12px",
        fontSize: 12.5,
        border: `1px solid ${active ? t.ink : t.line}`,
        color: active ? t.ink : t.mutedSoft,
        fontWeight: active ? 500 : 400,
        cursor: "pointer",
      }}
    >
      {dotColor && <span style={{ display: "inline-block", flex: "none", width: 6, height: 6, borderRadius: 999, background: dotColor }} />}
      {label}
      <span style={{ fontFamily: FONT_MONO, fontSize: 10, color: t.mutedSoft }}>{count}</span>
    </span>
  );
}

function ResultRow({ t, r }: { t: ReturnType<typeof tokensFor>; r: SearchAtomResult }) {
  const { size } = useHostLayout();
  const compact = size === "compact";
  const context = r.channel_name || r.project || sourceTypeLabel(r.source_type);
  const settled = r.reefs.length > 0 ? `settled into ${r.reefs.length} ${pluralize(r.reefs.length, "topic")}` : "not settled into a topic yet";
  const title = r.title?.trim() || r.snippet?.trim() || "Untitled coral";
  const source = {
    id: r.atom_id, title,
    excerpt: r.snippet && r.snippet !== title ? r.snippet : undefined,
    provenance: [
      {label:"Who",value:r.author_name || ""}, {label:"Context",value:context},
      {label:"Type",value:context === sourceTypeLabel(r.source_type) ? "" : sourceTypeLabel(r.source_type)},
      {label:"Date",value:shortDate(r.timestamp)},
    ],
    accent: sourceTypeColor(t, r.source_type),
  };
  return <div className="widget-search-card" data-clamp={compact ? "2" : "1"} style={{ padding: `8px ${gutter(size, 16)}px`, "--csau-preview-lines": compact ? 2 : 1, "--csau-search-title-lines": compact ? 2 : 1 }}>
    <AnswerUi.EvidenceCard source={source} showExcerpt onSelect={() => openExternalLink(coralHref(r.atom_id))}
      after={<span style={{display:"block",marginTop:4,fontFamily:FONT_MONO,fontSize:9.5,color:t.muted}}>{settled} · {relativeAgeFromDays(r.age_days)}</span>} />
  </div>;
}
