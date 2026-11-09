import type { MeetingEvidenceVersion } from "@/lib/api";
import { sourceMeta } from "@/components/library/sourceKind";

export function sourceTitle(metadata: Record<string, unknown> | null | undefined, fallback: string) {
  for (const key of ["title", "subject", "name"]) {
    const value = metadata?.[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return fallback;
}

export function representationLabel(value?: string | null) {
  switch (value) {
    case "original_text": return "Original text";
    case "normalized_text": return "Normalized text";
    case "generated_summary": return "Generated summary";
    case "provider_summary": return "Provider summary";
    case "legacy_preserved_text": return "Previously saved text";
    default: return value?.replaceAll("_", " ") || "Saved text";
  }
}

export function SourceSnapshotMetadata({ version }: { version: MeetingEvidenceVersion }) {
  const title = sourceTitle(version.source_metadata, "");
  const occurred = version.occurred_at ? new Date(version.occurred_at) : null;
  return <div className="space-y-3 border-b border-line pb-4 text-sm leading-6 text-ink-2">
    {title && <p className="max-w-prose break-words font-medium text-ink">{title}</p>}
    <dl className="flex flex-wrap gap-x-6 gap-y-2">
      <div><dt className="text-xs">Source kind</dt><dd>{sourceMeta(version.source_type || "unknown").label}</dd></div>
      <div><dt className="text-xs">Saved representation</dt><dd>{representationLabel(version.representation)}</dd></div>
      <div><dt className="text-xs">Source event time</dt><dd>{occurred && Number.isFinite(occurred.getTime()) ? occurred.toLocaleString() : "Unknown"}</dd></div>
    </dl>
    {(version.representation === "generated_summary" || version.representation === "provider_summary") && <p className="max-w-prose">Evidence points into the saved summary. A complete original transcript or document was not captured with this version.</p>}
    {!!version.participants?.length && <p className="max-w-prose break-words">Participants: {version.participants.join(", ")}</p>}
    <details className="max-w-prose">
      <summary className="cursor-pointer rounded-row text-brand-2 underline underline-offset-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand">Source identity</summary>
      <dl className="mt-2 space-y-2 break-all font-mono text-xs">
        <div><dt className="font-sans">Provider account or workspace</dt><dd>{version.provider_namespace || "Original provider identity unavailable"}</dd></div>
        <div><dt className="font-sans">Original source ID</dt><dd>{version.source_id || "Unavailable"}</dd></div>
      </dl>
    </details>
  </div>;
}
