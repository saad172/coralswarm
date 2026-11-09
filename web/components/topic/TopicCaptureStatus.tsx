import type { LibraryCurrentCapture } from "@/lib/libraryMemoryApi";
import { Badge } from "@/components/ui";

export function TopicCaptureStatus({ capture }: { capture?: LibraryCurrentCapture }) {
  if (!capture) return <p role="status" className="text-sm leading-6 text-ink-2">Current capture extraction metadata is unavailable. No processing or validity state has been inferred from retained capture dates.</p>;
  const copy = {
    ready: "Current captured versions have finished extraction. Their statements remain sourced evidence; capture readiness does not establish current validity.",
    pending: "Some current captures are awaiting extraction. Earlier retained evidence remains inspectable; missing current outcomes do not mean there is no history.",
    incomplete: "Some current captures could not finish extraction. Inspect the retained history and retry processing where available.",
    unavailable: "A current captured version is unavailable for some supporting sources. Retained evidence may still be readable.",
  };
  return <div className="space-y-2 text-sm leading-6 text-ink-2" role="status">
    <Badge tone={capture.status === "ready" ? "ink" : "warn"}>Current captures for this contribution: {capture.status}</Badge>
    <p>{copy[capture.status]}</p>
    <p className="font-mono text-xs">{capture.source_count} retained supporting sources · {capture.current_version_count} live current captured versions · {capture.current_semantic_atom_count} outcomes in this contribution from current captures</p>
    {(capture.pending_version_count > 0 || capture.incomplete_version_count > 0 || capture.unavailable_version_count > 0) && <p className="font-mono text-xs">{capture.pending_version_count} awaiting extraction · {capture.incomplete_version_count} incomplete · {capture.unavailable_version_count} without a current version</p>}
    <p className="text-xs">Latest current capture: {capture.latest_captured_at ? new Date(capture.latest_captured_at).toLocaleString() : "Unknown"}. Effective validity time is unknown.</p>
    <p className="text-xs">Readiness describes these supporting sources’ live current captures. Selecting an older exact version keeps its own statements and extraction status separate.</p>
  </div>;
}
