"use client";

import { SourceEvidence } from "@/components/sources/SourceEvidence";
export { exactPassageParts, PreservedTranscript } from "@/components/sources/SourceEvidence";

/** Compatibility entry point for existing meeting and transcript-correction links. */
export function MeetingEvidence(props: { token: string | null; graphId: string; sourceId: string }) {
  return <SourceEvidence {...props} />;
}
