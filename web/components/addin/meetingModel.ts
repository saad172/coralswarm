/* The meeting pane's status derivation, kept pure so every state has direct
 * unit coverage. States and meanings are the ones the original embedded capture
 * pane had (waiting for transcript, window ended, Microsoft connection needed,
 * ...), restated in the Gmail add-on's voice. */

import { copy } from "@/lib/addin/copy";
import type { api } from "@/lib/api";

export type MeetingSnapshot = Awaited<ReturnType<typeof api.msTeams.meetingStatus>>;

export type MeetingState = "off" | "waiting" | "saved" | "stopped" | "ended" | "connect_needed" | "connect_first";

export interface MeetingStatusView {
  state: MeetingState;
  line: string;
  hint: string;
  /** Extra lines under the hint (retrieval count, last check, last error). */
  details: string[];
  active: boolean;
  windowEnded: boolean;
  /** The primary action: save when off, stop when active, nothing when it cannot run. */
  primary: "save" | "stop";
  primaryDisabled: boolean;
}

export function meetingStatusView(snapshot: MeetingSnapshot, connectRequired: boolean, formatTime: (iso: string) => string = (iso) => new Date(iso).toLocaleString()): MeetingStatusView {
  const capture = snapshot.capture;
  const active = capture?.status === "active";
  const windowEnded = snapshot.availability_window_open === false;
  const retrieved = capture?.last_retrieved_at;
  const hasTranscript = Boolean(retrieved && capture?.retrieved_transcript_count);
  const S = copy.meeting.status;
  const pick = (state: MeetingState, entry: { line: string; hint: string }) => ({ state, line: entry.line, hint: entry.hint });
  const head =
    windowEnded ? pick("ended", S.ended)
    : active && connectRequired ? pick("connect_needed", S.connectNeeded)
    : active ? (hasTranscript ? pick("saved", S.saved) : pick("waiting", S.waiting))
    : connectRequired ? pick("connect_first", S.connectFirst)
    : capture ? pick("stopped", S.stopped)
    : pick("off", S.off);
  const details: string[] = [];
  if (retrieved) details.push(hasTranscript ? copy.meeting.retrieved(capture?.retrieved_transcript_count ?? 0) : copy.meeting.noneYet);
  if (capture?.last_checked_at) details.push(copy.meeting.lastChecked(formatTime(capture.last_checked_at)));
  if (capture?.last_error_code) details.push(!active || windowEnded ? copy.meeting.lastErrorIdle : connectRequired ? copy.meeting.lastErrorReconnect : copy.meeting.lastErrorRetry);
  return { ...head, details, active, windowEnded, primary: active ? "stop" : "save", primaryDisabled: !active && (windowEnded || connectRequired) };
}
