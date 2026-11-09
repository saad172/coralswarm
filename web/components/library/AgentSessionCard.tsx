import Link from "next/link";
import { Badge, Card, Chip, Mono } from "@/components/ui";
import { IconSessions } from "@/components/icons";
import type { CodingSessionSummary } from "@/lib/api";
import { fmtWhen } from "@/lib/meetingFormat";

/* One agent session row — the `agent_session` facet (`session_kind=agent`).

   MODELLED ON `SessionCard` (Coding sessions), with the agent identity the
   row exists to show: who the agent is (`agent_name`), where it runs
   (`platform`), what it is working on (`task`), and how many subagents it
   spawned. Every one of these is nullable on the wire and the row degrades
   field by field rather than hiding — the backend guarantees only `title`.

   The platform chip carries an "unverified" marker when `platform_verified`
   is false: the harness the corals actually came through (the request's
   User-Agent) named a different platform than the agent declared. Nothing is
   rewritten server-side — the declared value stays, the disagreement is
   surfaced — so the marker is informational, never a reason to drop the row. */
export function AgentSessionCard({ session }: { session: CodingSessionSummary }) {
  const subagents = session.subagent_count ?? 0;
  const unverified = session.platform_verified === false;
  // The same sentence feeds the hover title AND the screen-reader text, so
  // keyboard / AT users get the reason, not just the word.
  const unverifiedReason = session.platform_detected
    ? `The request came from ${session.platform_detected}, not ${session.platform}.`
    : "The request did not identify a known harness.";

  return (
    <Link href={`/session?id=${encodeURIComponent(session.session_id)}`}>
      <Card className="flex flex-col gap-[8px] transition-colors hover:border-ink-3">
        <div className="flex items-start gap-[10px]">
          <span className="mt-[2px] flex-none text-ink-3">
            <IconSessions width={17} height={17} aria-hidden />
          </span>
          <div className="min-w-0 flex-1">
            <div className="truncate font-display text-[17px] italic leading-[1.2] text-ink">
              {session.title || session.agent_name || fmtWhen(session.started_at)}
            </div>
            <div className="mt-[3px] flex flex-wrap items-center gap-x-[8px] font-mono text-[11px] text-ink-3">
              {session.agent_name && <span className="truncate text-ink-2">{session.agent_name}</span>}
              {session.project_key && <span className="truncate">{session.project_key}</span>}
              <span>active {fmtWhen(session.last_activity_at)}</span>
            </div>
          </div>
          <div className="flex flex-none flex-col items-end gap-[3px]">
            <Mono className="text-ink-3">
              {session.coral_count} item{session.coral_count === 1 ? "" : "s"}
            </Mono>
            {subagents > 0 && (
              <Mono className="text-ink-3">
                {subagents} subagent{subagents === 1 ? "" : "s"}
              </Mono>
            )}
          </div>
        </div>

        {(session.platform || session.task || session.model) && (
          <div className="flex flex-wrap items-center gap-[6px] pl-[27px]">
            {session.platform && (
              <span className="inline-flex items-center gap-[4px]">
                <Chip hue="plum">{session.platform}</Chip>
                {unverified && (
                  <span className="inline-flex" title={unverifiedReason}>
                    <Badge tone="warn" outline>
                      <span aria-hidden="true">unverified</span>
                      <span className="sr-only">{`unverified — ${unverifiedReason}`}</span>
                    </Badge>
                  </span>
                )}
              </span>
            )}
            {session.task && <Chip hue="amber">{session.task}</Chip>}
            {session.model && <Chip hue="plum">{session.model}</Chip>}
          </div>
        )}
      </Card>
    </Link>
  );
}
