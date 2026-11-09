import { Button, Mono, Notice } from "@/components/ui";
import type { OAuthConnection } from "@/lib/api";
import { credentialRevoked, reconsentNotice, scopeDescription, scopeLabel } from "./reconsentModel";

/** The re-consent banner — CS-431 (org-onboarding-from-the-invite.md §3 item
 *  34). Shared by the Slack tile (`app/(app)/connectors/page.tsx`'s
 *  `ProviderTile`) and `GoogleChatConnector`, so the two connectors this
 *  story covers can never disagree about when to show it or what it says —
 *  the derivation itself lives in `reconsentModel.ts`.
 *
 *  Renders nothing when `reconsentModel.reconsentNotice` says there is
 *  nothing to say (not connected, nothing missing, or the stored scope set
 *  is unknown — never guessed).
 *
 *  A connection whose credential a sync pass recorded as revoked
 *  (`credential_invalid_at`) gets the same banner with a plain "stopped
 *  working" line instead of the scope list — for every provider, since this
 *  is the only member-facing surface for a dead credential.
 *
 *  `Reconnect` calls the SAME handler the card's own `Connect` button uses —
 *  Slack's re-run-OAuth path and Google's `prompt=consent&include_granted_
 *  scopes=true` flow are both already the correct way to pick up a newly
 *  required scope on an already-connected account (see each card's own
 *  `handleConnect`), so this never needs a second code path. `adminOnly`
 *  mirrors `ProviderTile`'s own gate: reconnecting a Slack workspace-wide
 *  bot-token install is the same workspace-level act as the original
 *  Connect, so a plain member sees the notice (so they understand what is
 *  missing) but not a button that would bounce off Slack's own permission
 *  gate — same reasoning `ProviderTile`'s blocked/admins-only states already
 *  apply to Connect and Disconnect. */
export function ReconsentNotice({
  connection,
  onReconnect,
  reconnecting,
  adminOnly = false,
}: {
  connection: OAuthConnection | null;
  onReconnect: () => void;
  reconnecting: boolean;
  adminOnly?: boolean;
}) {
  const revoked = credentialRevoked(connection);
  const notice = reconsentNotice(connection);
  if (!revoked && !notice) return null;

  const action = adminOnly ? (
    <Mono className="text-ink-3">admins only</Mono>
  ) : (
    <Button size="sm" disabled={reconnecting} onClick={onReconnect}>
      {reconnecting ? "Redirecting…" : "Reconnect"}
    </Button>
  );
  if (revoked || !notice) {
    return (
      <Notice tone="warn" action={action}>
        This connection stopped working — access was revoked or expired. Reconnect to resume syncing.
      </Notice>
    );
  }

  return (
    <Notice tone="warn" action={action}>
      {notice.scopes.map((scope) => (
        <span key={scope} className="mr-[4px] inline-block">
          <code className="font-mono text-[11.5px] text-ink">{scopeLabel(scope)}</code>
          {" — "}
          {scopeDescription(scope)}.
        </span>
      ))}
    </Notice>
  );
}
