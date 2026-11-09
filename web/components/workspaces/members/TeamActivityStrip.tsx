"use client";

import { describeMembershipEvent, timeAgo } from "@/lib/teamEvents";
import type { MembershipEvent, TeamMemberRow } from "@/lib/api";
import { Button, Empty, ErrorNote, Eyebrow, Loading } from "@/components/ui";

/* Recent changes — a compact, presentation-only rendering of one team ocean's
 * membership audit trail (`GET /v1/teams/{team_id}/events`). All fetching,
 * pagination and the admin-only visibility gate live in the parent
 * (`WorkspaceMembers`) — this component never calls the API itself, so the
 * exact JSX under review here is the exact JSX that ships, same split
 * `SlackChannelActions.tsx`'s body/wrapper components use and for the same
 * reason.
 *
 * Never rendered at all for a non-admin — the events endpoint 403s for them
 * (team-admin/org-admin only, same gate as the roster WRITES), so a plain
 * member structurally cannot ever populate this, and showing an empty/error
 * state to them would misrepresent a permissions boundary as a fetch
 * failure. See `WorkspaceMembers`'s gating. */
export function TeamActivityStrip({
  events,
  roster,
  selfId,
  loading,
  err,
  hasMore,
  loadingMore,
  onLoadMore,
  onRetry,
}: {
  events: MembershipEvent[] | null;
  roster: TeamMemberRow[];
  selfId: string | null;
  loading: boolean;
  err: string | null;
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore: () => void;
  onRetry: () => void;
}) {
  return (
    <div className="rounded-card border border-line bg-bg p-[16px] shadow-card">
      <Eyebrow className="mb-[10px]">recent changes</Eyebrow>

      {loading ? (
        <Loading what="loading recent changes" />
      ) : err ? (
        <ErrorNote
          action={
            <Button size="sm" onClick={onRetry}>
              Retry
            </Button>
          }
        >
          {err}
        </ErrorNote>
      ) : !events || events.length === 0 ? (
        <Empty title="Nothing yet" hint="Membership changes to this workspace will show up here." />
      ) : (
        <div className="flex flex-col gap-[8px]">
          {events.map((e) => (
            <div key={e.id} className="flex flex-wrap items-baseline justify-between gap-x-[10px] gap-y-[2px]">
              <span className="min-w-0 flex-1 text-[12.5px] leading-[1.5] text-ink-2">
                {describeMembershipEvent(e, roster, selfId)}
              </span>
              <span
                className="flex-none font-mono text-[10px] text-ink-4"
                title={new Date(e.created_at).toLocaleString()}
              >
                {timeAgo(e.created_at)}
              </span>
            </div>
          ))}
          {hasMore && (
            <Button size="sm" variant="ghost" className="mt-[4px] self-start" onClick={onLoadMore} disabled={loadingMore}>
              {loadingMore ? "loading…" : "show more ↓"}
            </Button>
          )}
        </div>
      )}
    </div>
  );
}
