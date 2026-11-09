"use client";

/* The Outlook email pane's EXISTING Clerk flow, kept as the fallback for when
 * Microsoft SSO is off (404 microsoft_sso_disabled) or unavailable in the host.
 * Behaviour is the old task pane's (extensions/outlook-addin taskpane.js, now
 * removed): sign in with Clerk in the pane, a one-time "which workspace" choice
 * when there are several and none is active, a fresh Clerk JWT per request. Only
 * the look changed — it is the same kit as every other pane. */

import { useEffect, useMemo, useState } from "react";
import { SignIn, useAuth, useOrganization, useOrganizationList, useUser } from "@clerk/nextjs";
import { copy } from "@/lib/addin/copy";
import { bearerEmailApi } from "@/lib/addin/emailTransport";
import type { OfficeApi } from "@/lib/addin/office";
import { apiBase } from "@/lib/apiBase";
import { primaryEmailOf } from "@/lib/addinApprove";
import { Row, Section, Surface, ToastRegion, useToast } from "./kit";
import { MailContent } from "./MailContent";
import { HomeBody } from "./screens";
import type { ItemSnapshot } from "./useOfficeItem";

export function ClerkEmailPane({ office, item, openToday }: { office: OfficeApi | null; item: ItemSnapshot; openToday: () => void }) {
  const { isLoaded, isSignedIn, getToken, orgId, sessionId, signOut } = useAuth();
  const { user } = useUser();
  const { organization } = useOrganization();
  const { isLoaded: orgsLoaded, userMemberships, setActive } = useOrganizationList({ userMemberships: { infinite: true } });
  const [toastMessage, toast] = useToast();
  const [planeChosen, setPlaneChosen] = useState(false);
  const [choosing, setChoosing] = useState(false);
  const memberships = useMemo(() => userMemberships.data ?? [], [userMemberships.data]);
  const { hasNextPage, isFetching, fetchNext } = userMemberships;
  // eslint-disable-next-line no-restricted-syntax -- Clerk SDK pagination of the caller's org memberships for the plane picker, not an app API read
  useEffect(() => { if (hasNextPage && !isFetching) void fetchNext?.(); }, [hasNextPage, isFetching, fetchNext]);

  // Someone with exactly one organization and none active almost certainly means
  // that one; anything more ambiguous is the user's call (guessing wrong files
  // their mail into the wrong ocean).
  const mustChoose = isSignedIn && orgsLoaded && !planeChosen && !orgId && memberships.length > 1;
  useEffect(() => {
    if (!isSignedIn || !orgsLoaded || planeChosen || orgId) return;
    if (memberships.length === 1 && setActive) { setPlaneChosen(true); void setActive({ organization: memberships[0].organization.id }); }
    else if (memberships.length === 0) setPlaneChosen(true); // personal is the only plane
  }, [isSignedIn, orgsLoaded, planeChosen, orgId, memberships, setActive]);

  const api = useMemo(() => bearerEmailApi(() => getToken(), apiBase),
    // getToken rotates per render; the transport mints a fresh token per call.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [sessionId, orgId]);

  async function choose(next: string | null) {
    // Lock the choice in only once Clerk accepted it: a rejected setActive must leave the
    // chooser up, never fall through to a workspace the user did not pick.
    try {
      if (!setActive) throw new Error("Workspace switch unavailable");
      await setActive({ organization: next });
      setPlaneChosen(true); setChoosing(false);
    } catch { toast(copy.workspace.changeFailed); }
  }

  const workspaceName = organization?.name ?? copy.workspace.personal;
  return (
    <Surface label="CoralSwarm email">
      {!isLoaded || !orgsLoaded && isSignedIn ? <p role="status" className="px-1 py-6 text-center text-ink-3">{copy.booting}</p>
        : !isSignedIn ? (
          <Section>
            <div className="px-3 py-3">
              <SignIn routing="hash" oauthFlow="popup" forceRedirectUrl={typeof window === "undefined" ? undefined : window.location.href} signUpForceRedirectUrl={typeof window === "undefined" ? undefined : window.location.href}
                appearance={{ elements: { rootBox: "w-full max-w-full", cardBox: "!w-full !max-w-full !shadow-none", card: "!w-full !max-w-full !bg-bg !border-0 !shadow-none !px-0" } }} />
            </div>
          </Section>
        )
        : mustChoose || choosing ? (
          <>
            <Section header={copy.workspace.title}>
              {memberships.map((member) => <Row key={member.organization.id} icon="label" hue="teal" text={member.organization.name} bottom={copy.workspace.organization} onClick={() => void choose(member.organization.id)} />)}
              <Row icon="account" text={copy.workspace.personal} bottom={copy.workspace.personal} onClick={() => void choose(null)} />
            </Section>
          </>
        ) : (
          <>
            <div key={`${sessionId}|${orgId ?? "personal"}`} className="space-y-3">
              {item.kind === "loading" ? <HomeBody surface="email" /> : (
                <MailContent api={api} item={item} office={office} workspaceName={workspaceName} toast={toast} onUnauthorized={() => { toast(copy.reconnect); void signOut(); }} openToday={openToday} />
              )}
            </div>
            <Section>
              <Row icon="account" text={<span className="break-all">{primaryEmailOf(user) ?? copy.account.connected}</span>} bottom={workspaceName} />
              <Row text={copy.menu.changeWorkspace} onClick={() => setChoosing(true)} />
              <Row text="Sign out" onClick={() => void signOut()} />
            </Section>
          </>
        )}
      <ToastRegion message={toastMessage} />
    </Surface>
  );
}
