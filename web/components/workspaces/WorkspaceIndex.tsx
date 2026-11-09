"use client";

import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { oceansIndexQueryOptions } from "@/lib/oceansIndex";
import { isOwnerOnlyOcean } from "@/lib/askScope";
import { Button, Empty, ErrorNote, Loading, PageHead, SectionHead } from "@/components/ui";
import { WorkspaceCard } from "./WorkspaceCard";
import { NewWorkspace } from "./NewWorkspace";
import { ArchivedWorkspaces } from "./ArchivedWorkspaces";

/* /workspaces with no `?id=` — the index of workspaces you can reach.

   NOT IN THE MOCK. The handoff only ever draws a single workspace (screen 1e),
   reached from the sidebar's "your workspaces" sub-list,
   so the top-level rail item had no destination of its own and landed you in
   one arbitrary workspace's topic grid — which reads as a worse copy of
   Library, since it is the same topic cards over a narrower set.

   Grouping owner-only workspaces apart from shared ones is the point of the
   screen rather than decoration: the whole promote/share model rests on
   knowing which side of that line a thing is on. This is display grouping
   ONLY — it no longer constrains what you can ask across (see askScope.ts). */

export function WorkspaceIndex() {
  const { authReady, getAuthToken, accountId, queryScope, refreshGraphs } = useApp();
  const query = useQuery({ ...oceansIndexQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady });
  const components = query.data?.components ?? [];
  const rows = components.flatMap((component) => component.stored.computed_at === null ? [] :
    component.stored.payload.oceans.map((row) => ({ ...row, component })));
  const pending = !query.data || components.some((component) => component.stored.computed_at === null);
  const error = query.error && <ErrorNote action={<Button size="sm" onClick={() => void query.refetch()}>Retry workspaces</Button>}>{query.error.message}</ErrorNote>;
  if (query.error && rows.length === 0) return error;
  if (!query.data) return <Loading what="reading workspaces" />;
  if (rows.length === 0 && !pending) return <Empty title="No workspaces yet" hint="A workspace is a boundary: who can see what you capture." />;
  // Unknown components render no bootstrap names or invented empty groups.
  // Each visible card belongs to that account's actual computed certificate.
  const card = (row: (typeof rows)[number]) => <WorkspaceCard
    key={`${queryScope}:${row.component.account_id}:${row.graph.id}`} graph={row.graph}
    stats={row.counts} members={row.component.stored.payload.members} loading={false} />;
  const ownerOnly = rows.filter((row) => isOwnerOnlyOcean(row.graph));
  const shared = rows.filter((row) => !isOwnerOnlyOcean(row.graph));

  return (
    <>
      <PageHead
        title="Workspaces"
        action={<NewWorkspace onCreated={() => { void refreshGraphs(); void query.refetch(); }} />}
      >
        Every workspace is a boundary. Nothing crosses one until you promote it.
      </PageHead>

      {error}
      {pending && !query.error && <Loading what="reading remaining workspaces" />}
      {ownerOnly.length > 0 && (
        <section className="mb-[28px]">
          <SectionHead title="Only you">
            Where capture lands by default. Nothing here is reachable by anyone else.
          </SectionHead>
          <div className="grid grid-cols-1 gap-[12px] md:grid-cols-2 xl:grid-cols-3">
            {ownerOnly.map(card)}
          </div>
        </section>
      )}

      <section>
        <SectionHead eyebrow="shared" title="Shared with others">
          Promote a topic into one of these to make it reachable by the people in it.
        </SectionHead>
        {shared.length === 0 ? pending ? null : (
          <Empty
            title="No shared workspaces"
            hint="Shared workspaces come from an organization or a team. Until then, everything you capture stays yours alone."
          />
        ) : (
          <div className="grid grid-cols-1 gap-[12px] md:grid-cols-2 xl:grid-cols-3">
            {shared.map(card)}
          </div>
        )}
      </section>

      {/* Account-wide archive administration comes only from the active
          account's certified component, never a reachable foreign account. */}
      <ArchivedWorkspaces supplied={{ rows: components.find((c) => c.account_id === accountId && c.stored.computed_at !== null)?.stored.payload.archived ?? null, onChanged: () => void query.refetch() }} />
    </>
  );
}
