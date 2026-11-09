"use client";

import { useRef, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useOrgAdmin } from "@/components/org/useOrgAdmin";
import { Avatar, Badge, Dot, Eyebrow, MenuItem, Popover, Skeleton, workspaceHue } from "@/components/ui";
import { IconDots } from "@/components/icons";
import type { Graph, Member, OceanStats } from "@/lib/api";
import { isOwnerOnlyOcean, oceanBadge } from "@/lib/askScope";
import { kebabMenuItems, type KebabAction } from "@/lib/workspaceManage";

/* One workspace in the /workspaces index.

   Hue is `workspaceHue(graph.id)`, the same dot this workspace gets in the
   chat composer and chat lists, so the colour means one thing everywhere. */

const KEBAB_LABEL: Record<KebabAction, string> = {
  manage: "Manage members",
  rename: "Rename",
  archive: "Archive",
  leave: "Leave",
};

export function WorkspaceCard({
  graph,
  stats,
  members,
  loading,
  statsError,
  onRetryStats,
}: {
  graph: Graph;
  stats?: OceanStats;
  /** Account members. Only meaningful for an ORG ocean — see WhoCanSee. */
  members?: Member[];
  loading: boolean;
  statsError?: boolean;
  onRetryStats?: () => void;
}) {
  const hue = workspaceHue(graph.id);
  const ownerOnly = isOwnerOnlyOcean(graph);
  const href = `/workspaces?id=${encodeURIComponent(graph.id)}`;

  const orgAdmin = useOrgAdmin();
  const items = kebabMenuItems(graph, orgAdmin);

  return (
    // Not an `<a>` wrapping the whole card. The kebab is a real, independent
    // `<button>` that must never sit INSIDE an anchor — `<button>` is
    // interactive content and the HTML content model forbids nesting
    // interactive elements inside `<a>` (screen readers announce it as one
    // broken control, and a raw click on the button would still fire the
    // anchor's navigation as the event bubbles). Instead this uses the
    // "stretched link" pattern: `href` lives on a real, visible `<Link>`
    // wrapping just the workspace name, widened to cover the whole card via
    // an absolutely-positioned `::after` pseudo-element
    // (`after:absolute after:inset-0`) anchored to this `relative` div. CSS
    // paints positioned content (the pseudo) after normal in-flow siblings
    // in the same stacking context, so that overlay sits ABOVE the
    // non-interactive card content (badge, stats, "who can see it") and
    // makes the whole card clickable — but the kebab button, given its own
    // `relative` (and coming later in the DOM within the header row), paints
    // above the overlay in turn and captures its own clicks first. Net
    // result: one real link (keyboard focus lands on the name, screen
    // readers announce "Engineering, link"), one real, independently
    // tabbable button, full-card click-to-navigate for sighted mouse users,
    // and the kebab never triggers a navigation — no nesting, no
    // preventDefault/stopPropagation gymnastics required for it to work,
    // though the trigger's onClick still calls stopPropagation defensively
    // (belt-and-suspenders, in case a future refactor nests these two after
    // all).
    <div className="relative flex flex-col gap-[12px] rounded-card border border-line bg-bg p-[16px] shadow-card transition-colors hover:border-ink-3">
      <div className="flex items-center gap-[9px]">
        <Dot hue={hue} size={9} />
        <Link
          href={href}
          className="min-w-0 flex-1 truncate text-[15px] font-medium text-ink after:absolute after:inset-0 after:content-['']"
        >
          {graph.name}
        </Link>
        <Badge tone={ownerOnly ? "brand" : "teal"} outline>
          {oceanBadge(graph)}
        </Badge>
        {items && <WorkspaceCardKebab graph={graph} items={items} />}
      </div>

      <p className="text-[12.5px] leading-[1.55] text-ink-2">
        {ownerOnly
          ? "Only you. Nothing here reaches anyone else until you promote it."
          : "Shared — everyone with access to this workspace can see what it holds."}
      </p>

      <WhoCanSee graph={graph} members={members} />

      <div className="mt-auto grid grid-cols-3 gap-[10px] border-t border-line-3 pt-[11px]">
        <Stat label="topics" value={stats?.reef_total} loading={loading} />
        <Stat label="items" value={stats?.atom_count} loading={loading} />
        <Stat label="sources" value={stats?.source_count} loading={loading} />
      </div>
      {statsError && (
        <p role="status" className="text-[12px] text-ink-2">
          Counts unavailable. <button type="button" onClick={onRetryStats}
            className="relative z-10 underline hover:text-ink">Retry counts</button>
        </p>
      )}
    </div>
  );
}

/* The kebab trigger + its menu. A sibling of the card's stretched `<Link>`
 * (see the doc comment above), not a descendant, given its own `relative` so
 * it paints above the link's full-card `::after` overlay and receives its
 * own clicks. Contents are entirely determined by `kebabMenuItems` — see
 * that function's doc comment (lib/workspaceManage.ts) for the exact role
 * matrix, Archive's placement on it, and the asymmetry between who can
 * archive and who can later restore. */
function WorkspaceCardKebab({ graph, items }: { graph: Graph; items: KebabAction[] }) {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);

  function go(action: KebabAction) {
    setOpen(false);
    const id = encodeURIComponent(graph.id);
    // Rename, Archive, and Leave all deep-link into the manage page with an
    // `intent` rather than acting in place from the card — see the report
    // for why: short version, Leave needs the full roster to know whether
    // the caller is the team's sole admin (the manage page already loads it;
    // adding that fetch here would be exactly the per-card fetch this
    // component is built to avoid), Archive's confirmation is a real modal
    // with load-bearing copy that belongs on one screen rather than
    // duplicated per-card, and keeping every mutation on one page is the
    // same "one home for management" reasoning the workspace-manage design
    // doc already applied to the Members panel.
    if (action === "manage") router.push(`/workspaces/manage?id=${id}`);
    else router.push(`/workspaces/manage?id=${id}&intent=${action}`);
  }

  return (
    <div className="relative flex-none">
      <button
        ref={triggerRef}
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label={`Actions for ${graph.name}`}
        onClick={(e) => {
          // Defensive only — this button is a SIBLING of the card's anchor,
          // not a descendant of it, so its click can never bubble into a
          // navigation in the first place. See the card's own doc comment.
          e.preventDefault();
          e.stopPropagation();
          setOpen((v) => !v);
        }}
        className="flex-none rounded-row p-[4px] text-ink-3 transition-colors hover:bg-bg-hover hover:text-ink"
      >
        <IconDots width={16} height={16} />
      </button>
      <Popover open={open} onClose={() => setOpen(false)} triggerRef={triggerRef} align="right">
        {items.map((action) => (
          <MenuItem
            key={action}
            onClick={() => go(action)}
            danger={action === "leave" || action === "archive"}
          >
            {KEBAB_LABEL[action]}
          </MenuItem>
        ))}
      </Popover>
    </div>
  );
}

function Stat({ label, value, loading }: { label: string; value?: number; loading: boolean }) {
  return (
    <div className="flex flex-col gap-[3px]">
      {loading && value === undefined ? (
        <Skeleton w={34} h={15} />
      ) : (
        <span className="font-mono text-[15px] text-ink">{value?.toLocaleString() ?? "—"}</span>
      )}
      <Eyebrow>{label}</Eyebrow>
    </div>
  );
}

/* Who can actually reach this workspace.
 *
 * Answered per KIND, because the backend's reachability predicate
 * (`resolve_reachable_graphs`, store/mod.rs) treats them differently:
 *
 *   personal / member  owner only.
 *   org                every member of the account — so the account member
 *                      list IS the answer, and showing it is truthful.
 *   team               gated on an active `team_members` row. NOT every
 *                      account member.
 *
 * That last case is why this doesn't just render the member list everywhere:
 * for a team ocean, account members would OVERSTATE who can see it, which is
 * exactly the wrong way to be wrong about a privacy boundary. A per-team
 * roster endpoint now exists (`api.teams.roster`, `GET /v1/teams/{id}/members`
 * — see the full management surface at `/workspaces/manage?id=`), but this
 * index card stays a lightweight summary rather than fetching every shared
 * workspace's roster just to render one line here. */
function WhoCanSee({ graph, members }: { graph: Graph; members?: Member[] }) {
  const label = (text: string, body?: React.ReactNode) => (
    <div className="border-t border-line-3 pt-[10px]">
      <Eyebrow className="mb-[6px]">who can see it</Eyebrow>
      {body ?? <span className="text-[12px] text-ink-2">{text}</span>}
    </div>
  );

  if (isOwnerOnlyOcean(graph)) return label("Only you.");

  if (graph.kind === "org") {
    if (!members) return label("Everyone in this organization.");
    const shown = members.slice(0, 5);
    return label(
      "",
      <div className="flex items-center gap-[6px]">
        <div className="flex -space-x-[6px]">
          {shown.map((m) => (
            <span key={m.user_id} className="rounded-pill ring-2 ring-bg" title={m.email}>
              <Avatar name={m.email} size={20} />
            </span>
          ))}
        </div>
        <span className="text-[12px] text-ink-2">
          {members.length} {members.length === 1 ? "person" : "people"} in this organization
        </span>
      </div>,
    );
  }

  // kind === "team" — a per-team roster endpoint exists (see the doc comment
  // above), it just isn't fetched HERE; this index card stays a lightweight
  // summary and points at the workspace's own manage surface for the actual
  // list, rather than claiming a roster that's a click away doesn't exist.
  return label("Its members — open the workspace to see the full roster.");
}
