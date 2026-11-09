// Label helpers for the topic page (moved verbatim from the Library's
// `libraryMemoryState.ts`; the rest of that module drove the Library browser).
import type { LibraryEvidenceCounts } from "./libraryMemoryApi";

export function nodeLabel(title: string | null): string {
  return title?.trim() || "Unnamed node";
}

export function librarySourceSupportLabel(counts: LibraryEvidenceCounts): string {
  return counts.unknown_independence_record_count === undefined
    ? `${counts.source_support_count} supporting source item records · independence unknown`
    : `${counts.source_support_count} independently attributed supporting source items · ${counts.unknown_independence_record_count} records with unknown independence`;
}
