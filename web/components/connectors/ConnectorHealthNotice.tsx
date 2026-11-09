"use client";

/* D16's connector-health notice — CS-446 (org-onboarding-from-the-invite.md
 * §3 item 16).
 *
 * ── Why this exists at all ──
 * D16's build note: "if the member whose token backs the directory leaves,
 * the whole org silently drops from rung 1 to rung 2 unless that raises a
 * `connector_health` warning." Nothing about that drop is visible: no page
 * errors, no connector shows as broken, no job fails. An auto-add decision
 * is simply made on weaker evidence from then on, forever. A `warn!` would
 * make it visible to an operator tailing logs and to nobody else — which is
 * the "silently" the note objects to — so the condition is recorded
 * (`connector_health`, migration 0243) and shown HERE, to the admin whose
 * setting it changes.
 *
 * ── Shape ──
 * Follows `ReconsentNotice` (CS-431), the closest precedent in the app: a
 * connector-health condition rendered as an actionable `Notice` with the
 * remedy attached, sitting with the connector tile it concerns. The copy
 * derivation lives in `connectorHealthModel.ts` so it is testable without a
 * DOM and can never drift from a second renderer.
 *
 * ── Decides for itself, like `GmailAddonTokens` ──
 * Renders `null` for a non-admin, on the personal plane, while loading, on
 * any error, and when nothing is degraded. The page does not gate it; it
 * gates itself, so `/connectors` grows exactly one line and no conditional.
 * A FAILED fetch renders nothing rather than an error banner: this read is
 * admin-only on the server, and a member who wanders onto the connectors
 * page must not be shown "couldn't load connector health" for a request
 * that was never theirs to make. The condition is durable — it will still
 * be there on the next load — so a swallowed transient costs a delay, not
 * the signal.
 *
 * ── CS-448: a second notice, from the same read ──
 * The response now also carries `directory_attribution` — nobody in the
 * org grants directory access, and these still-present members disconnected
 * Google Chat. It renders here, beside the recorded degradations, because
 * it is the same capability, the same tile and the same admin. It is NOT a
 * degradation and is not shaped like one: no condition, no first-observed
 * date, nothing to resolve. It stops being true the moment someone
 * reconnects, and the backend re-derives it on every read, so there is
 * nothing to dismiss and no state to keep here.
 *
 * Whether BOTH can show at once: yes, and deliberately. A departed backer
 * (item 16's row) and a still-present member who disconnected are two
 * different people with two different remedies — only one of them can be
 * asked to press a button — and collapsing them would lose exactly the
 * distinction migration 0243 was written to preserve. */

import { useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { connectorHealthQueryOptions } from "@/lib/appQueries";
import { Button, Mono, Notice } from "@/components/ui";
import { useOrgAdmin } from "@/components/org/useOrgAdmin";
import {
  BACKING_MEMBER_DEPARTED,
  connectorHealthCopy,
  degradationKey,
  directoryAttributionCopy,
  visibleDegradations,
  GOOGLE_CHAT,
} from "./connectorHealthModel";

export function ConnectorHealthNotice({
  /** Start the Google Chat connect flow — the SAME handler the Google Chat
   *  tile's own Connect button uses, threaded in rather than duplicated, so
   *  the notice's remedy and the tile's button can never diverge about what
   *  "connect" means (the `?surface=chat` mint-state + authorize dance).
   *  Omitted ⇒ the notice still renders and still says what to do, just
   *  without a button. */
  onConnectGoogleChat,
  connecting = false,
}: {
  onConnectGoogleChat?: () => void;
  connecting?: boolean;
}) {
  const { authReady, getAuthToken, queryScope } = useApp();
  const { loaded, hasOrg, isAdmin } = useOrgAdmin();
  const eligible = loaded && hasOrg && isAdmin && authReady;
  // Deliberately silent on failure — see this file's own doc comment.
  const payload = useQuery({ ...connectorHealthQueryOptions({ getAuthToken, scope: queryScope }), enabled: eligible }).data ?? null;

  if (!eligible) return null;
  const degradations = visibleDegradations(payload);
  const attribution = directoryAttributionCopy(payload?.directory_attribution);
  if (degradations.length === 0 && !attribution) return null;

  return (
    // No outer margin: this renders INSIDE the Google Chat card's own
    // `flex flex-col gap-[11px]`, which already spaces it from its
    // neighbours. A `mb-` here would stack on top of that gap.
    <div className="flex flex-col gap-[10px]">
      {degradations.map((item) => {
        const copy = connectorHealthCopy(item);
        // The action is offered only for a condition this build actually
        // recognises AND whose connector is the one this button connects.
        // Pointing a "Connect Google Chat" button at a degradation we
        // cannot name would be a guess dressed as a remedy.
        const canAct = copy.recognised && item.provider === GOOGLE_CHAT && Boolean(onConnectGoogleChat);
        return (
          <Notice
            key={degradationKey(item)}
            tone="warn"
            action={
              canAct ? (
                <Button size="sm" disabled={connecting} onClick={onConnectGoogleChat}>
                  {connecting ? "Redirecting…" : "Connect Google Chat"}
                </Button>
              ) : undefined
            }
          >
            <strong className="text-ink">{copy.what}</strong>{" "}
            {copy.backingEmail && item.condition === BACKING_MEMBER_DEPARTED && (
              <>
                It was connected through{" "}
                <code className="font-mono text-[11.5px] text-ink">{copy.backingEmail}</code>, who
                is no longer in this organization.{" "}
              </>
            )}
            {copy.means} {copy.remedy}
            <Mono className="mt-[6px] block text-ink-4">
              organization admins only · noticed {new Date(item.first_observed_at).toLocaleDateString()}
            </Mono>
          </Notice>
        );
      })}

      {/* CS-448. Rendered AFTER the recorded degradations, never instead of
          them: a durable condition an admin can watch resolve outranks a
          view of right now. The remedy button is the same handler the tile
          and the degradation notice above use — threaded in once, so all
          three can never disagree about what "connect" means. */}
      {attribution && (
        <Notice
          tone="warn"
          action={
            onConnectGoogleChat ? (
              <Button size="sm" disabled={connecting} onClick={onConnectGoogleChat}>
                {connecting ? "Redirecting…" : "Connect Google Chat"}
              </Button>
            ) : undefined
          }
        >
          <strong className="text-ink">{attribution.what}</strong>{" "}
          {attribution.means} {attribution.remedy}
          {/* `span role="list"`, not `ul`. `Notice` puts its children
              inside a `span`, and a `ul` nested in a `span` is invalid
              HTML: React builds it fine through the DOM API, but the
              browser's own parser hoists it out of the span when it reads
              the server-rendered markup, which would break the layout on
              first paint and only on first paint. The ARIA roles keep the
              list semantics a screen reader needs. */}
          <span role="list" className="mt-[6px] flex flex-col gap-[3px]">
            {attribution.members.map((m) => (
              <span role="listitem" key={m.email}>
                <code className="font-mono text-[11.5px] text-ink">{m.email}</code>
                <span className="text-ink-4">
                  {" "}
                  · disconnected {new Date(m.disconnectedAt).toLocaleDateString()}
                </span>
              </span>
            ))}
          </span>
          <Mono className="mt-[6px] block text-ink-4">
            organization admins only · {attribution.scopeLabel} — {attribution.scopeDescription}
          </Mono>
        </Notice>
      )}
    </div>
  );
}
