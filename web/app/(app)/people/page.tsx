"use client";

/* People — members and transition briefs. Unlisted: not in the rail, reached
   from Settings → Organization → Members (and old /team, /library/people
   links). Data flow ported
   from archive/web-legacy-ui/screens/team.page.tsx: the org/personal tenancy
   branch (`lib/teamPanel.ts`'s `decideTeamMembersPanel`, since org
   `account_members` rows are Clerk-webhook-only — see that file's own
   comment for why a direct-write member list can't serve org accounts) and
   the transition-brief `api.ask.run` flow, both unchanged; only the surface
   is new.

   Real member data has no `sources`/`topics`/`active` per-person breakdown —
   the mock's grid shows five columns of illustrative numbers this API
   doesn't expose. Rather than fabricate them, this shows member + role only.

   MEMBER LIST is a react-query (perf/library-tab-cache) keyed on
   `accountId`, not `graphId` — `api.teams.members` is account-wide, not
   ocean-scoped (see `lib/libraryQueries.ts`'s `peopleQueryOptions` doc). A
   revisit inside the 60s staleness window costs zero requests. Inviting a
   teammate invalidates the same key, the same way a promote/delete on any
   other Library list invalidates its own. */

import { useState, type ReactNode } from "react";
import Link from "next/link";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useOrganization, OrganizationProfile } from "@clerk/nextjs";
import { useApp } from "@/lib/providers";
import { api } from "@/lib/api";
import {
  PageHead,
  Empty,
  ErrorNote,
  Loading,
  Avatar,
  ListHead,
  ListHeadCell,
  Input,
  Button,
  ListRow,
  RowTitle,
  Dot,
  hueForName,
  type Hue,
} from "@/components/ui";
import { decideTeamMembersPanel, selfserveOrgSignal, teamOrgProfileElements, TEAM_ORG_MEMBERS_HREF } from "@/lib/teamPanel";
import { orgSelfserveActiveQueryOptions } from "@/lib/appQueries";
import { PeopleBrief } from "@/components/people/PeopleBrief";
import { libraryInvalidation, peopleQueryOptions } from "@/lib/libraryQueries";

// `elements` hides Clerk's own membership management for a team org (A2
// security review S4): see `teamOrgProfileElements` in lib/teamPanel.ts.
// Used only for orgs that are NOT self-serve (#1540 review SF-a).
const clerkOrgProfileAppearance = {
  elements: teamOrgProfileElements(),
  variables: {
    colorPrimary: "var(--brand)",
    colorBackground: "var(--bg)",
    colorText: "var(--ink)",
    colorTextSecondary: "var(--ink-2)",
    colorInputBackground: "var(--bg)",
    colorInputText: "var(--ink)",
    borderRadius: "var(--r-card)",
    fontFamily: "var(--sans)",
  },
};

export default function PeoplePage() {
  // `getAuthToken` + `authReady`, never `token` — see the note on
  // `AppState.token` in lib/providers.tsx.
  const { getAuthToken, authReady, currentGraph, queryScope } = useApp();
  const queryClient = useQueryClient();
  const { isLoaded: orgLoaded, organization } = useOrganization();
  // #1540 review SF-a: whether the active org is a self-serve team org, from
  // the server (`GET /v1/bootstrap` → `selfserve_org`, seeded by boot). A
  // failed read counts as "could not say" (`null`), which keeps Clerk's
  // OrganizationProfile: only an explicit `true` shows our own list.
  const selfserve = useQuery({
    ...orgSelfserveActiveQueryOptions({ getAuthToken, scope: queryScope }),
    enabled: authReady && !!organization,
  });
  const membersPanel = decideTeamMembersPanel({
    isLoaded: orgLoaded,
    organization,
    selfserveOrg: selfserveOrgSignal(selfserve),
  });

  const query = useQuery(peopleQueryOptions({ getAuthToken, enabled: authReady, scopeId: queryScope }));
  const members = query.data ?? [];
  // Cold-load only — see the Topics page's identical comment.
  const loading = query.isPending;

  const [invite, setInvite] = useState("");
  const [msg, setMsg] = useState<string | null>(null);

  async function sendInvite() {
    const token = authReady ? getAuthToken() : null;
    if (!token || !invite) return;
    setMsg(null);
    try {
      await api.teams.invite(token, invite, "member");
      setMsg(`Invited ${invite}`);
      setInvite("");
      void queryClient.invalidateQueries({ queryKey: libraryInvalidation.people() });
    } catch (e) {
      setMsg(e instanceof Error ? e.message : "Invite failed");
    }
  }

  return (
    <>
      <PageHead title="People">
        Who is in {currentGraph?.name ?? "this workspace"}, and a hand-over brief for anyone who is leaving.
      </PageHead>

      <div className="grid grid-cols-1 items-start gap-[24px] xl:grid-cols-[minmax(0,1fr)_360px]">
        <div>
          {membersPanel === "loading" && <Loading what="loading members" />}

          {membersPanel === "org" && (
            <div>
              <div className="mb-[12px] font-mono text-[10px] uppercase tracking-[0.13em] text-ink-3">
                {currentGraph?.name ?? "organization"} members
              </div>
              <p className="mb-[12px] text-[12.5px] leading-[1.55] text-ink-2">
                Add members and send invites from{" "}
                <Link href={TEAM_ORG_MEMBERS_HREF} className="text-brand-2 underline">
                  organization settings
                </Link>
                , where each new seat is quoted before it is bought.
              </p>
              <OrganizationProfile appearance={clerkOrgProfileAppearance} routing="hash">
                {/* General first, so it (not Members) is the default page. */}
                <OrganizationProfile.Page label="general" />
                <OrganizationProfile.Page label="members" />
              </OrganizationProfile>
            </div>
          )}

          {membersPanel === "selfserve_org" && (
            <div>
              <div className="mb-[12px] font-mono text-[10px] uppercase tracking-[0.13em] text-ink-3">
                {currentGraph?.name ?? "organization"} members
              </div>
              <p className="mb-[12px] text-[12.5px] leading-[1.55] text-ink-2">
                Add members and send invites from{" "}
                <Link href={TEAM_ORG_MEMBERS_HREF} className="text-brand-2 underline">
                  organization settings
                </Link>
                , where each new seat is quoted before it is bought.
              </p>
              {/* No Clerk <OrganizationProfile/> for a self-serve org (#1540
                  SF3, review SF-a): its Members page stays reachable at
                  #/organization-members whatever `appearance` hides, and it
                  invites without our seat quote. A read-only list; members
                  are managed in settings. */}
              <MemberRows
                members={members}
                loading={loading}
                isError={query.isError}
                emptyHint="Invite teammates from organization settings."
                onRetry={() => void query.refetch()}
              />
            </div>
          )}

          {membersPanel === "personal" && (
            <div>
              <MemberRows
                members={members}
                loading={loading}
                isError={query.isError}
                emptyHint="Invite teammates below to share this workspace."
                onRetry={() => void query.refetch()}
              />
              <div className="mt-[16px] flex max-w-[420px] items-center gap-[8px]">
                <Input wrapperClassName="flex-1" placeholder="teammate@company.com" value={invite} onChange={(e) => setInvite(e.target.value)} />
                <Button disabled={!invite} onClick={() => void sendInvite()}>
                  Invite
                </Button>
              </div>
              {msg && <p className="mt-[8px] font-mono text-[11px] text-hue-teal">{msg}</p>}
            </div>
          )}
        </div>

        <div className="rounded-card border border-line bg-bg-2 p-[18px]">
          <PeopleBrief members={members} />
        </div>
      </div>
    </>
  );
}

/* The member rows both panels show. `isError` is checked before the empty
   branch: a rejected `api.teams.members` also resolves `members` to `[]`
   (`const members = query.data ?? []` above), which used to render "Just you
   so far", a confident, wrong claim about the roster on a fetch failure.
   `ErrorNote` + Retry matches the other list pages. */
function MemberRows({
  members,
  loading,
  isError,
  emptyHint,
  onRetry,
}: {
  members: { user_id: string; email: string; role: string }[];
  loading: boolean;
  isError: boolean;
  emptyHint: string;
  onRetry: () => void;
}) {
  return (
    <>
      <ListHead className="mb-[2px]">
        <ListHeadCell width={28}> </ListHeadCell>
        <ListHeadCell>member</ListHeadCell>
        <ListHeadCell width={80}>role</ListHeadCell>
      </ListHead>
      {loading ? (
        <Loading what="loading members" />
      ) : isError ? (
        <div className="flex flex-col items-start gap-[10px]">
          <ErrorNote>Couldn&apos;t load members.</ErrorNote>
          <Button size="sm" onClick={onRetry}>
            Retry
          </Button>
        </div>
      ) : members.length === 0 ? (
        <Empty title="Just you so far" hint={emptyHint} />
      ) : (
        members.map((m) => (
          <PersonRow
            key={m.user_id}
            icon={<Avatar name={m.email} size={26} hue={hueForName(m.email)} />}
            hue={hueForName(m.email)}
            title={m.email}
            trailing={<span className="flex-none font-mono text-[11px] tracking-[0.04em] text-ink-3">{m.role}</span>}
          />
        ))
      )}
    </>
  );
}

/** One member row: avatar, email, role. (The Library's shared list row, which
 *  this page used to borrow, went with Library.) */
function PersonRow({ icon, hue, title, trailing }: { icon: ReactNode; hue: Hue; title: ReactNode; trailing: ReactNode }) {
  return (
    <ListRow className="gap-[11px] py-[10px]">
      {icon}
      <RowTitle>{title}</RowTitle>
      {trailing}
      <Dot hue={hue} size={6} />
    </ListRow>
  );
}
