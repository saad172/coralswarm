"use client";

/* The screens every add-in pane shares: start card, pairing, home, account row,
 * workspace picker. Layout is native to the iframe; content, order and copy
 * follow the Gmail add-on's cards (Card.gs buildPairingStartCard_,
 * buildPairingWaitingCard_, buildHomepageConnectedCard_, accountRow_). */

import { useEffect, useState } from "react";
import { copy, type Surface as SurfaceKind } from "@/lib/addin/copy";
import type { AddinClient, ConnectedSession, Pairing, Workspace } from "@/lib/addin/microsoftSso";
import { FilledButton, Footer, Illustration, OutlineButton, OverflowMenu, PlusRow, Row, Section, TextButton, type Hue } from "./kit";

export function StartCard({ surface, busy, onConnect }: { surface: SurfaceKind; busy: boolean; onConnect: () => void }) {
  const content = copy.start[surface];
  return (
    <>
      <Section><Illustration image="empty-start" title={content.title} body={content.hint} /></Section>
      <Section>
        {content.rows.map((row) => <Row key={row.text} icon={row.icon} hue={row.hue as Hue} text={row.text} bottom={"bottom" in row ? row.bottom : undefined} />)}
      </Section>
      <Footer><FilledButton full disabled={busy} onClick={onConnect}>{copy.connectButton}</FilledButton></Footer>
    </>
  );
}

/** The Clerk OAuth start state (rollout gate on): one button, no pairing code. A cancelled or failed
 *  attempt says so above the same button, now labelled "Try again". */
export function SignInCard({ surface, busy, notice, onSignIn }: { surface: SurfaceKind; busy: boolean; notice: "cancelled" | "failed" | null; onSignIn: () => void }) {
  const content = copy.start[surface];
  return (
    <>
      <Section><Illustration image="empty-start" title={content.title} body={notice ? content.hint : copy.signIn.hint} /></Section>
      {notice ? <p role="alert" className="rounded-card border border-line bg-bg px-3 py-3 text-ink-2">{notice === "cancelled" ? copy.signIn.cancelled : copy.signIn.failed}</p> : null}
      <Footer><FilledButton full disabled={busy} onClick={onSignIn}>{notice ? copy.retry : copy.signIn.button}</FilledButton></Footer>
    </>
  );
}

export function PairingCard({ pairing, onOpen, onContinue, onRestart }: { pairing: Pairing; onOpen: () => void; onContinue: () => void; onRestart: () => void }) {
  const [first, second, third] = copy.pairing.steps;
  return (
    <>
      <Section>
        <Row icon="key" top={copy.pairing.codeLabel} text={<b data-testid="pairing-code">{pairing.code}</b>} bottom={copy.pairing.codeHint} />
      </Section>
      <Section header={copy.pairing.stepsHeader}>
        <Row icon={first.icon} text={first.text} bottom={first.bottom} />
        <div className="flex px-3 py-2.5"><FilledButton full onClick={onOpen}>{copy.openApproval}</FilledButton></div>
        <Row icon={second.icon} text={second.text} bottom={second.bottom} />
        <Row icon={third.icon} text={third.text} bottom={third.bottom} />
      </Section>
      <Footer>
        <TextButton onClick={onRestart}>{copy.startOver}</TextButton>
        <FilledButton onClick={onContinue}>{copy.pairing.continue}</FilledButton>
      </Footer>
    </>
  );
}

/** No item open: the muted "+" row and the empty illustration. */
export function HomeBody({ surface, message, alert = false }: { surface: SurfaceKind; message?: string; /** The message is an error: announce it, once, in the plus-row. */ alert?: boolean }) {
  const content = copy.home[surface];
  return (
    <Section>
      {alert && message ? <div role="alert"><PlusRow muted text={message} /></div> : <PlusRow muted text={message ?? content.hint} />}
      <Illustration image="empty-home" title={content.title} body={content.body} />
    </Section>
  );
}

/** "<email> · Connected" with the ⋮ menu: Connected as …, Change workspace, Disconnect. */
export function AccountRow({ session, onChangeWorkspace, onDisconnect }: { session: ConnectedSession; onChangeWorkspace: () => void; onDisconnect: () => void }) {
  return (
    <Section>
      <Row icon="account" text={<span className="break-all">{session.account.email}</span>} bottom={copy.account.connected}
        trailing={
          <OverflowMenu label={copy.menu.label} header={copy.account.connectedAs(session.account.email)} subheader={copy.account.workspace(session.workspace.name)}
            items={[
              { label: copy.menu.changeWorkspace, onSelect: onChangeWorkspace },
              { label: copy.menu.disconnect, onSelect: onDisconnect, destructive: true },
            ]} />
        } />
    </Section>
  );
}

/** Change workspace: the picker (from GET link). No dropdown on the main screen. */
export function WorkspaceSheet({ client, current, onPick, onClose }: { client: AddinClient; current: Workspace; onPick: (accountId: string) => Promise<void>; onClose: () => void }) {
  const [workspaces, setWorkspaces] = useState<Workspace[] | null>(null);
  const [failed, setFailed] = useState(false);
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    let live = true;
    client.link().then((info) => { if (live) setWorkspaces(info.workspaces); }).catch(() => { if (live) setFailed(true); });
    return () => { live = false; };
  }, [client]);
  return (
    <>
      <Section header={copy.workspace.title}>
        {failed ? <p role="alert" className="px-3 py-3 text-ink-2">{copy.workspace.failed}</p>
          : !workspaces ? <p role="status" className="px-3 py-3 text-ink-3">{copy.workspace.loading}</p>
          : workspaces.map((workspace) => {
            const isCurrent = workspace.account_id === current.account_id;
            return (
              <Row key={workspace.account_id} icon={isCurrent ? "check" : workspace.kind === "org" ? "label" : "account"} hue={isCurrent ? "ok" : workspace.kind === "org" ? "teal" : "ink"}
                text={workspace.name} bottom={workspace.kind === "org" ? copy.workspace.organization : copy.workspace.personal}
                onClick={busy || isCurrent ? undefined : async () => { setBusy(true); try { await onPick(workspace.account_id); } finally { setBusy(false); } }} />
            );
          })}
      </Section>
      <Footer><OutlineButton onClick={onClose}>{copy.workspace.close}</OutlineButton></Footer>
    </>
  );
}
