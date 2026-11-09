import Link from "next/link";
import { Card, Chip, Mono } from "@/components/ui";
import type { CodingSessionSummary } from "@/lib/api";
import { fmtWhen } from "@/lib/meetingFormat";

/** Human title for a session: the captured title, else "{project} ·
 *  {date}", else just the date — never the raw session id. */
function sessionTitle(s: CodingSessionSummary): string {
  if (s.title) return s.title;
  const date = fmtWhen(s.started_at);
  return s.project_key ? `${s.project_key} · ${date}` : date;
}

/** One coding session in the list — mirrors the mock's card: italic serif
 *  title, project + last-active meta, coral count, plum provenance chips. */
export function SessionCard({ session }: { session: CodingSessionSummary }) {
  return (
    <Link href={`/session?id=${encodeURIComponent(session.session_id)}`}>
      <Card className="flex flex-col gap-[8px] transition-colors hover:border-ink-3">
        <div className="flex items-start justify-between gap-[12px]">
          <div className="min-w-0 flex-1">
            <div className="truncate font-display text-[17px] italic leading-[1.2] text-ink">{sessionTitle(session)}</div>
            <div className="mt-[3px] flex flex-wrap items-center gap-x-[8px] font-mono text-[11px] text-ink-3">
              {session.project_key && <span>{session.project_key}</span>}
              <span>active {fmtWhen(session.last_activity_at)}</span>
            </div>
          </div>
          <Mono className="flex-none text-ink-3">
            {session.coral_count} item{session.coral_count === 1 ? "" : "s"}
          </Mono>
        </div>
        {(session.platform || session.task || session.harness_version || session.model || session.repo_path || session.remote_sanitized) && (
          <div className="flex min-w-0 flex-wrap gap-[6px]">
            {session.platform && (
              <Chip hue="plum" className="max-w-full min-w-0 break-all">
                {session.platform}
              </Chip>
            )}
            {session.task && (
              <Chip hue="amber" className="max-w-full min-w-0 break-all">
                {session.task}
              </Chip>
            )}
            {session.harness_version && <Chip hue="plum">{session.harness_version}</Chip>}
            {session.model && <Chip hue="plum">{session.model}</Chip>}
            {session.repo_path && (
              <Chip hue="plum" className="max-w-full min-w-0 break-all">
                {session.repo_path}
              </Chip>
            )}
            {session.remote_sanitized && session.remote_sanitized !== session.repo_path && (
              <Chip hue="plum" className="max-w-full min-w-0 break-all">
                {session.remote_sanitized}
              </Chip>
            )}
          </div>
        )}
      </Card>
    </Link>
  );
}
