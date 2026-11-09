import { tokensFor, FONT_MONO, FONT_SERIF, reefTypeStyle, sourceTypeColor, sourceTypeLabel } from "../theme";
import { useToolOutput, useTheme } from "../openai";
import type { GetReefOutput } from "../types";
import { Card } from "../components/Card";
import { isToolFailure, ToolFailure } from "../components/ToolFailure";
import { Empty } from "../components/Empty";
import { Working } from "../components/Working";
import { AnswerUi } from "../answerUi";
import { RelationshipPill } from "../components/ReefPills";
import { shortDate, relativeAgeFromIso } from "../format";
import { topicHref } from "../links";
import { gutter, useHostLayout } from "../layout";

export function GetReefTemplate() {
  const output = useToolOutput<GetReefOutput>();
  const theme = useTheme();
  const t = tokensFor(theme);
  const { size } = useHostLayout();
  const compact = size === "compact";
  const bodyGutter = gutter(size, 18);
  if (isToolFailure(output)) return <ToolFailure t={t} output={output} tool="get_reef" />;

  if (!output) {
    return <Working t={t} label="Loading reef…" />;
  }

  const style = reefTypeStyle(t, output.type);
  const status = `${output.type} reef · ${output.atom_count} coral${output.atom_count === 1 ? "" : "s"}`;
  const excerpts = output.digest?.excerpts ?? [];
  const coralsById = new Map(output.corals.map((c) => [c.atom_id, c]));

  return (
    <Card t={t} status={status} linkLabel="open reef" linkHref={topicHref(output.reef_id)}>
      <div style={{ padding: `16px ${bodyGutter}px 4px`, display: "flex", flexDirection: "column", gap: 10 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 9 }}>
          <span
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 4,
              borderRadius: 999,
              padding: "2px 8px",
              background: style.bg,
              color: style.fg,
              fontFamily: FONT_MONO,
              fontSize: 9,
              letterSpacing: "0.06em",
              textTransform: "uppercase",
              whiteSpace: "nowrap",
            }}
          >
            {style.glyph}
          </span>
          <span style={{ fontFamily: FONT_MONO, fontSize: 9.5, color: t.mutedSoft }}>
            updated {relativeAgeFromIso(output.updated_at) || "recently"}
          </span>
        </div>
        <div style={{ fontFamily: FONT_SERIF, fontWeight: 400, fontSize: compact ? 21 : 26, lineHeight: 1.15, letterSpacing: "-0.5px", color: t.ink }}>
          {output.title}
        </div>
        <div style={{ fontSize: 15, lineHeight: 1.7, color: t.ink, maxWidth: "70ch" }}>
          {output.digest?.summary || "No digest yet — this reef hasn't been summarized."}
        </div>
      </div>

      {excerpts.length > 0 && (
        <div className="widget-reef-timeline" style={{ padding: `12px ${bodyGutter}px 0`, display: "flex", flexDirection: "column", gap: 10 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontFamily: FONT_MONO, fontSize: 9.5, letterSpacing: "0.13em", textTransform: "uppercase", color: t.muted }}>
              how it was reached
            </span>
            <span style={{ flex: 1, height: 1, background: t.lineSoft }} />
            <span style={{ fontFamily: FONT_MONO, fontSize: 10, color: t.mutedSoft }}>
              {excerpts.length} of {output.atom_count} corals
            </span>
          </div>
          <div>
            {excerpts.map((ex, i) => {
              const coral = coralsById.get(ex.atom_id);
              const color = sourceTypeColor(t, coral?.source_type);
              const context = coral?.participants?.[0] ?? sourceTypeLabel(coral?.source_type);
              return (
                <div key={ex.atom_id + i} data-date-column={compact ? "stacked" : "gutter"} style={{ display: "grid", gridTemplateColumns: compact ? "minmax(0, 1fr)" : "70px minmax(0, 1fr)", gap: compact ? 2 : 14 }}>
                  <div style={{ textAlign: compact ? "left" : "right", paddingTop: compact ? 0 : 10, fontFamily: FONT_MONO, fontSize: 10, color: t.mutedSoft }}>
                    {shortDate(coral?.timestamp)}
                  </div>
                  <div style={{ position: "relative", borderLeft: `1px solid ${t.lineSoft}`, padding: "0 0 14px 18px" }}>
                    <span style={{ position: "absolute", left: -4.5, top: 12, width: 8, height: 8, borderRadius: 999, background: color, boxShadow: `0 0 0 3px ${t.bg}` }} />
                    <AnswerUi.EvidenceCard source={{
                      id: `${ex.atom_id}:${i}`,
                      title: ex.contribution || "Source excerpt",
                      excerpt: ex.excerpt || undefined,
                      provenance: [{label:"Who",value:context}, {label:"Type",value:sourceTypeLabel(coral?.source_type)}],
                      accent: color,
                    }} showExcerpt />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {output.relationships.length > 0 && (
        <div style={{ padding: `4px ${bodyGutter}px 16px`, display: "flex", flexDirection: "column", gap: 10 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <span style={{ fontFamily: FONT_MONO, fontSize: 9.5, letterSpacing: "0.13em", textTransform: "uppercase", color: t.muted }}>
              relationships
            </span>
            <span style={{ flex: 1, height: 1, background: t.lineSoft }} />
            <span style={{ fontFamily: FONT_MONO, fontSize: 10, color: t.mutedSoft }}>{output.relationships.length}</span>
          </div>
          <div style={{ display: "grid", gridTemplateColumns: compact ? "minmax(0, 1fr)" : "repeat(2, minmax(0, 1fr))", gap: 8 }}>
            {output.relationships.map((r) => {
              // Which side names THIS reef — see README "Backend fields
              // needed": get_reef's relationships carry title strings only,
              // no node ids, so this is a best-effort string match.
              const other = r.subject === output.title ? r.object : r.subject;
              const label = r.subject === output.title ? r.predicate.replace(/_/g, " ") : `${r.predicate.replace(/_/g, " ")} (inverse)`;
              return <RelationshipPill key={r.edge_id} t={t} label={label} title={other} />;
            })}
          </div>
        </div>
      )}

      {excerpts.length === 0 && output.relationships.length === 0 && !output.digest?.summary && (
        <Empty t={t} title="This reef hasn't settled yet." body="Corals link into a reef a few minutes after capture." />
      )}
    </Card>
  );
}
