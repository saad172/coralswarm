"use client";

/* The Teams side panel's shell: two user steps, "Log in" and "Add meeting".
 * It reuses the shared connection machine (SSO -> pairing -> connected) but none
 * of the shared chrome: no account row, no overflow menu, no workspace sheet.
 * Outlook keeps using AddinApp untouched. */

import type { ReactNode } from "react";
import { copy } from "@/lib/addin/copy";
import type { AddinClient, AddinHost, Pairing } from "@/lib/addin/microsoftSso";
import { FilledButton, Footer, Icon, Section, Surface, TextButton, ToastRegion, useToast, type Hue } from "./kit";
import { useAddinConnection } from "./useAddinConnection";
import type { ConnectedContext } from "./AddinApp";

/** The one card every Teams step lives in (Log in, Ready, Adding, Added), so moving
 *  between steps changes the content of one screen instead of swapping layouts.
 *  Same language as `Illustration`: rounded card, centered, near the top. */
export function HeroCard({ art, title, subtitle, titleAttr, children }: { art: ReactNode; title: string; subtitle?: string; titleAttr?: string; children?: ReactNode }) {
  return (
    <Section>
      <div className="px-4 pb-4 pt-5 text-center">
        {art}
        <p title={titleAttr} className="mx-auto line-clamp-3 max-w-[280px] break-words font-display text-pane-title text-ink [text-wrap:balance]">{title}</p>
        {subtitle ? <p className="mx-auto mt-1 max-w-[260px] text-pane text-ink-2">{subtitle}</p> : null}
        {children ? <div className="mt-4 space-y-3 text-left">{children}</div> : null}
      </div>
    </Section>
  );
}

/** The 132px brand-soft circle. `image` paints the Log in hero; `glyph` a kit icon for meeting steps. */
export function HeroArt({ image, glyph, hue }: { image?: "empty-start"; glyph?: string; hue?: Hue }) {
  return (
    <div role="img" aria-label="" className="mx-auto mb-3 flex h-[132px] w-[132px] items-center justify-center rounded-full bg-brand-soft bg-no-repeat"
      style={image ? { backgroundImage: `url(/addin/${image}.png)`, backgroundSize: "auto 238px", backgroundPosition: "-45px -14px" } : undefined}>
      {glyph ? <Icon name={glyph} hue={hue} size={56} /> : null}
    </div>
  );
}

/** First-time pairing, still the Log in card: the code, one button to approve in the browser,
 *  and quiet continue / start over. No separate three-step screen. */
export function PairingLogIn({ pairing, onOpen, onContinue, onRestart }: { pairing: Pairing; onOpen: () => void; onContinue: () => void; onRestart: () => void }) {
  return (
    <HeroCard art={<HeroArt image="empty-start" />} title={copy.teams.loginTitle} subtitle={copy.teams.pairingHint}>
      <p className="text-center text-pane-xs text-ink-3">{copy.pairing.codeLabel}: <b data-testid="pairing-code" className="text-ink">{pairing.code}</b></p>
      <FilledButton full onClick={onOpen}>{copy.openApproval}</FilledButton>
      <div data-testid="pairing-secondary" className="flex flex-col items-center gap-1 [&>button]:whitespace-nowrap [&>button:last-child]:text-ink-3">
        <TextButton onClick={onContinue}>{copy.pairing.continue}</TextButton>
        <TextButton onClick={onRestart}>{copy.startOver}</TextButton>
      </div>
    </HeroCard>
  );
}

/** The one "Log in" screen. Shown before CoralSwarm sign-in and again when Microsoft still needs connecting. */
export function LogInCard({ busy, onLogIn, hint }: { busy: boolean; onLogIn: () => void; hint?: string }) {
  return (
    <HeroCard art={<HeroArt image="empty-start" />} title={copy.teams.loginTitle} subtitle={hint ?? copy.teams.loginHint}>
      <FilledButton full disabled={busy} onClick={onLogIn}>{copy.teams.logIn}</FilledButton>
    </HeroCard>
  );
}

/** The Log in card for the Clerk OAuth gate: "Sign in to CoralSwarm", with the cancelled / failed notice and a retry. */
export function SignInLogIn({ busy, notice, onSignIn }: { busy: boolean; notice: "cancelled" | "failed" | null; onSignIn: () => void }) {
  return (
    <HeroCard art={<HeroArt image="empty-start" />} title={copy.teams.loginTitle} subtitle={copy.signIn.hint}>
      {notice ? <p role="alert" className="text-center text-pane-xs text-ink-2">{notice === "cancelled" ? copy.signIn.cancelled : copy.signIn.failed}</p> : null}
      <FilledButton full disabled={busy} onClick={onSignIn}>{notice ? copy.retry : copy.signIn.button}</FilledButton>
    </HeroCard>
  );
}

export function TeamsSimpleApp({ client, host, label, fallback, children }: {
  client: AddinClient;
  host: Pick<AddinHost, "openLink">;
  label: string;
  fallback: ReactNode;
  children: (context: ConnectedContext) => ReactNode;
}) {
  const [toastMessage, toast] = useToast();
  const connection = useAddinConnection({ client, host, toast });
  const { state } = connection;

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
      {state.status === "start" ? <LogInCard busy={state.busy} onLogIn={() => void connection.connect()} /> : null}
      {state.status === "signin" ? <SignInLogIn busy={state.busy} notice={state.notice} onSignIn={() => void connection.signIn()} /> : null}
      {state.status === "pairing" ? (
        <PairingLogIn pairing={state.pairing} onOpen={() => void connection.openApproval()} onContinue={() => void connection.continuePairing()} onRestart={connection.restart} />
      ) : null}
      {state.status === "connected" ? (
        <div key={state.session.workspace.account_id} className="space-y-3">
          {children({ session: state.session, client, toast, reconnectRequired: connection.reconnectRequired, openWorkspacePicker: () => { if (connection.oauthEnabled) void connection.reselectWorkspace(); } })}
        </div>
      ) : null}
      <ToastRegion message={toastMessage} />
    </Surface>
  );
}
