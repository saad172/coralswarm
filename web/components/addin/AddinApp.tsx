"use client";

/* The one shell for Teams and Outlook. It owns the connection (SSO → optional
 * one-time pairing → connected), the toast, the account row with its ⋮ menu and
 * the workspace picker, and renders the host-specific connected content through
 * `children`. When Microsoft SSO is off (backend flag) or unavailable in the
 * host, it renders `fallback` — the pre-existing Clerk flow, untouched. */

import { useState, type ReactNode } from "react";
import type { Surface as SurfaceKind } from "@/lib/addin/copy";
import { copy } from "@/lib/addin/copy";
import type { AddinClient, AddinHost, ConnectedSession } from "@/lib/addin/microsoftSso";
import { FilledButton, Footer, Surface, ToastRegion, useToast } from "./kit";
import { AccountRow, PairingCard, SignInCard, StartCard, WorkspaceSheet } from "./screens";
import { useAddinConnection } from "./useAddinConnection";

export interface ConnectedContext {
  session: ConnectedSession;
  client: AddinClient;
  toast: (message: string) => void;
  /** Call when any API answer shows the link is gone: back to the start card. */
  reconnectRequired: () => void;
  openWorkspacePicker: () => void;
}

export function AddinApp({ client, host, surface, label, fallback, children }: {
  client: AddinClient;
  host: Pick<AddinHost, "openLink">;
  surface: SurfaceKind;
  label: string;
  fallback: ReactNode;
  children: (context: ConnectedContext) => ReactNode;
}) {
  const [toastMessage, toast] = useToast();
  const connection = useAddinConnection({ client, host, toast });
  const [picking, setPicking] = useState(false);
  const { state } = connection;
  // Gate on: Change workspace re-runs the sign-in dialog (consent screen) instead of the link's picker.
  const changeWorkspace = () => { if (connection.oauthEnabled) void connection.reselectWorkspace(); else setPicking(true); };

  if (state.status === "fallback") return <>{fallback}</>;

  return (
    <Surface label={label}>
      {state.status === "booting" ? <p role="status" className="px-1 py-6 text-center text-ink-3">{copy.booting}</p> : null}
      {state.status === "error" ? (
        <>
          <p role="alert" className="rounded-card border border-line bg-bg px-3 py-3 text-ink-2">{state.message}</p>
          <Footer><FilledButton full onClick={() => void connection.retry()}>{copy.retry}</FilledButton></Footer>
        </>
      ) : null}
      {state.status === "start" ? <StartCard surface={surface} busy={state.busy} onConnect={() => void connection.connect()} /> : null}
      {state.status === "signin" ? <SignInCard surface={surface} busy={state.busy} notice={state.notice} onSignIn={() => void connection.signIn()} /> : null}
      {state.status === "pairing" ? (
        <PairingCard pairing={state.pairing} onOpen={() => void connection.openApproval()} onContinue={() => void connection.continuePairing()} onRestart={connection.restart} />
      ) : null}
      {state.status === "connected" ? (
        picking ? (
          <WorkspaceSheet client={client} current={state.session.workspace} onClose={() => setPicking(false)}
            onPick={async (accountId) => { if (await connection.changeWorkspace(accountId)) setPicking(false); }} />
        ) : (
          <>
            {/* Keyed by workspace: a workspace change remounts the content, so
                nothing started in the old workspace can finish in the new one. */}
            <div key={state.session.workspace.account_id} className="space-y-3">
              {children({ session: state.session, client, toast, reconnectRequired: connection.reconnectRequired, openWorkspacePicker: changeWorkspace })}
            </div>
            <AccountRow session={state.session} onChangeWorkspace={changeWorkspace} onDisconnect={() => void connection.disconnect()} />
          </>
        )
      ) : null}
      <ToastRegion message={toastMessage} />
    </Surface>
  );
}
