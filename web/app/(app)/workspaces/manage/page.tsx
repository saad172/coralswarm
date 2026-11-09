"use client";

import { Suspense } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useApp } from "@/lib/providers";
import { useOrgAdmin } from "@/components/org/useOrgAdmin";
import { resolveManageAccess } from "@/lib/workspaceManage";
import { Badge, Empty, ErrorNote, Loading } from "@/components/ui";
import { WorkspaceMembers } from "@/components/workspaces/members/WorkspaceMembers";

/* /workspaces/manage?id=<graph_id> — the dedicated management surface for one
   SHARED workspace (a "workspace" IS an ocean; a SHARED one is
   `kind === "team"` — see `web/app/(app)/workspaces/page.tsx`'s vocabulary
   note). Query-param routed, NOT a dynamic `[id]` segment, matching the
   `?id=` pattern used everywhere here — see that same page's own comment.

   Everything that MUTATES this workspace's roster (rename, add/remove
   members, change roles, leave, archive) now lives on this one page — the
   workspace page itself (`/workspaces?id=`) only shows a compact, read-only
   summary that links here (workspace-manage design doc, "What stays on the
   workspace page": "one home for management, so it cannot drift or need
   fixing twice"). Archive returns here alongside the archived-workspaces
   list and restore (workspace-manage design doc, "Archive and restore") —
   `ArchiveWorkspaceModal` is rendered by `WorkspaceMembers` below. */
function ManageWorkspaceInner() {
  const params = useSearchParams();
  const idParam = params.get("id");
  const intentParam = params.get("intent");
  // Only three intents exist (see WorkspaceCard's kebab); anything else — a
  // hand-edited URL, a future typo — is silently ignored rather than passed
  // through, so `WorkspaceMembers` never has to defend against an unknown
  // string.
  const intent =
    intentParam === "rename" || intentParam === "leave" || intentParam === "archive" ? intentParam : undefined;

  const { graphs, ready } = useApp();
  const orgAdmin = useOrgAdmin();

  // Graphs (and Clerk's org state) haven't resolved yet — this is the ONE
  // transient state; every branch below is a real, settled answer, not a
  // loading flicker. Waiting on `orgAdmin.loaded` too avoids a false
  // "unauthorized" flash for an org-admin-but-not-a-member viewer before
  // Clerk's org role has loaded in.
  if (!ready || !orgAdmin.loaded) return <Loading what="loading workspace" />;

  const graph = idParam ? (graphs.find((g) => g.id === idParam) ?? null) : null;

  // A missing/unknown `?id=` gets this same "not found" state — deliberately
  // NOT the `/workspaces` (no `?id=`) behavior of falling back to the index;
  // this route always names a specific workspace. Checked directly on
  // `graph` (rather than calling `resolveManageAccess` first and branching
  // on `status === "not-found"`) so TypeScript narrows `graph` to non-null
  // for every branch below — no non-null assertion needed past this point.
  if (!graph) {
    return (
      <Empty
        title="Workspace not found"
        hint="This link may be old, or you may no longer have access to it."
        action={
          <Link href="/workspaces" className="font-mono text-[11px] uppercase tracking-[0.1em] text-brand-2">
            Back to workspaces
          </Link>
        }
      />
    );
  }

  const g = graph;
  const access = resolveManageAccess(g, orgAdmin);
  const backHref = `/workspaces?id=${encodeURIComponent(g.id)}`;

  if (access.status === "not-shared") {
    return (
      <Empty
        title="Not a shared workspace"
        hint={`${g.name} is yours alone — only shared (team) workspaces have members to manage.`}
        action={
          <Link href={backHref} className="font-mono text-[11px] uppercase tracking-[0.1em] text-brand-2">
            Back to {g.name}
          </Link>
        }
      />
    );
  }

  if (access.status === "missing-team-id") {
    return (
      <ErrorNote>
        This workspace is missing its team id — couldn&apos;t load its management page.
      </ErrorNote>
    );
  }

  if (access.status === "unauthorized") {
    return (
      <Empty
        title="You don't have access to this"
        hint="Only this workspace's admins, or an organization admin, can manage its members."
        action={
          <Link href={backHref} className="font-mono text-[11px] uppercase tracking-[0.1em] text-brand-2">
            Back to {g.name}
          </Link>
        }
      />
    );
  }

  // access.status is "member" | "admin" — the two states `WorkspaceMembers`
  // itself already knows how to render (it derives its own `canManage` from
  // the freshly loaded roster, which is the belt-and-suspenders check for
  // exactly the role this page just decided from `graph.my_team_role`).
  return (
    <>
      <div className="mb-[14px] flex flex-wrap items-center gap-[8px]">
        <Link href="/workspaces" className="text-[13px] text-ink-3 transition-colors hover:text-ink">
          Workspaces
        </Link>
        <span className="text-ink-4">/</span>
        <Link href={backHref} className="truncate text-[13px] text-ink-3 transition-colors hover:text-ink">
          {g.name}
        </Link>
        <span className="text-ink-4">/</span>
        <span className="text-[13px] text-ink">Manage</span>
      </div>

      <div className="mb-[16px] flex items-center gap-[11px]">
        <span className="text-[15px] font-semibold tracking-[-0.01em] text-ink">Manage {g.name}</span>
        <Badge tone="teal" outline>
          {g.kind}
        </Badge>
      </div>

      <WorkspaceMembers graph={g} autoIntent={intent} />
    </>
  );
}

export default function ManageWorkspacePage() {
  return (
    <Suspense fallback={<Loading what="loading workspace" />}>
      <ManageWorkspaceInner />
    </Suspense>
  );
}
