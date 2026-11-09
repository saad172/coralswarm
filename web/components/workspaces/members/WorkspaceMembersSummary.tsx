"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import type { Graph } from "@/lib/api";
import { oceansIndexQueryOptions } from "@/lib/oceansIndex";
import { resolveTeamId } from "@/lib/teamId";
import { Avatar, Dot, hueForName } from "@/components/ui";

/** Compact, read-only stand-in for the Members panel that used to render
 *  inline on `/workspaces?id=` — that surface now lives entirely at
 *  `/workspaces/manage?id=` (workspace-manage design doc, "What stays on the
 *  workspace page": "every mutation lives on the manage page"). This
 *  component shares the account-certified OceansIndex (to show a count and a few faces) and
 *  renders nothing that can mutate anything — no rename, no add/remove, no
 *  role control, not even a Leave button. Rendered only for `kind === "team"`
 *  by the caller, same gate the old inline panel used. */
export function WorkspaceMembersSummary({ graph }: { graph: Graph }) {
  const { getAuthToken, authReady, queryScope } = useApp();
  // Select this team only from its own authorized component certificate.
  // No additional roster or per-account request is mounted.
  const teamId = resolveTeamId({ id: graph.id, team_id: graph.team_id });
  const query = useQuery({ ...oceansIndexQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady && !!teamId });
  const component = query.data?.components.find((entry) => entry.stored.computed_at !== null &&
    entry.stored.payload.oceans.some((row) => row.graph.id === graph.id));
  const roster = component && teamId ? component.stored.payload.team_members[teamId] ?? [] : null;
  const err = query.error || query.data && !component && query.data.components.every((entry) => entry.stored.computed_at !== null)
    ? "Couldn't load member count." : null;

  const href = `/workspaces/manage?id=${encodeURIComponent(graph.id)}`;
  const shown = roster?.slice(0, 3) ?? [];

  return (
    <Link
      href={href}
      className="mb-[16px] flex items-center gap-[10px] rounded-control border border-line bg-bg px-[12px] py-[9px] text-[12.5px] text-ink-2 transition-colors hover:border-ink-3 hover:text-ink"
    >
      {shown.length > 0 ? (
        <span className="flex -space-x-[6px]">
          {shown.map((m) => (
            <span key={m.user_id} className="rounded-pill ring-2 ring-bg" title={m.name || m.email}>
              <Avatar name={m.name || m.email} hue={hueForName(m.name || m.email)} size={20} />
            </span>
          ))}
        </span>
      ) : (
        <Dot hue="teal" size={7} />
      )}
      <span className="min-w-0 flex-1">
        {roster
          ? `${roster.length} member${roster.length === 1 ? "" : "s"}`
          : err
            ? "Members"
            : "Loading members…"}
      </span>
      <span className="flex-none font-mono text-[10px] uppercase tracking-[0.1em] text-ink-3">Manage →</span>
    </Link>
  );
}
