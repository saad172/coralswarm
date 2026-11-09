"use client";

import { useEffect, useState, type ReactNode } from "react";
import Link from "next/link";
import { useQuery } from "@tanstack/react-query";
import { Button, Card, Eyebrow, Mono } from "@/components/ui";
import { useOrgAdmin } from "@/components/org/useOrgAdmin";
import { useApp } from "@/lib/providers";
import { chatEmptyQueryOptions } from "@/lib/appQueries";
import {
  deriveFirstRunChecklist,
  FIRST_RUN_SNOOZE_MS,
  firstRunSnoozeKey,
  type FirstRunInput,
  type ItemState,
} from "@/lib/firstRunChecklist";

/* An org's first-run checklist on the empty Ask: connect your own sources
   (D21: each member connects with their own grant), invite teammates (admins
   only), then ask a first question. Every item comes from the server; see
   lib/firstRunChecklist.ts for the derivation. The three reads are the shared
   React Query entries the sidebar history, Connectors and the Members page
   already use, so the checklist adds no request a warm cache can answer.
   Renders nothing for a personal account, while loading, and once done. */

function readSnooze(key: string | null): number | null {
  if (!key || typeof window === "undefined") return null;
  const raw = window.localStorage.getItem(key);
  const n = raw === null ? NaN : Number(raw);
  return Number.isFinite(n) ? n : null;
}

function joinLabels(labels: string[]): string {
  if (labels.length <= 1) return labels.join("");
  return `${labels.slice(0, -1).join(", ")} and ${labels[labels.length - 1]}`;
}

function StepMark({ state, n }: { state: ItemState; n: number }) {
  const done = state === "done";
  return (
    <span
      aria-hidden
      className={`grid h-[22px] w-[22px] flex-none place-items-center rounded-pill border font-mono text-[11px] ${
        done ? "border-ok bg-ok/[12%] text-ok" : "border-line bg-bg-2 text-ink-3"
      }`}
    >
      {done ? "✓" : n}
    </span>
  );
}

function Step({ n, state, title, children }: { n: number; state: ItemState; title: string; children: ReactNode }) {
  const status =
    state === "done" ? "done" : state === "unknown" ? "couldn't check" : state === "unavailable" ? "not on yet" : "to do";
  return (
    <li className="flex gap-[12px] py-[12px]">
      <StepMark state={state} n={n} />
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline gap-[8px]">
          <span className={`text-[13.5px] font-medium ${state === "done" ? "text-ink-3 line-through" : "text-ink"}`}>
            {title}
          </span>
          <Mono className={state === "done" ? "text-ok" : "text-ink-4"}>{status}</Mono>
        </div>
        <div className="mt-[3px] text-[12.5px] leading-[1.55] text-ink-2">{children}</div>
      </div>
    </li>
  );
}

const LINK = "text-[12.5px] font-medium text-brand-2 hover:underline";

export function FirstRunChecklist() {
  const { authReady, getAuthToken, queryScope, accountId, internalUserId } = useApp();
  const org = useOrgAdmin();
  const active = org.loaded && org.hasOrg && authReady;
  const ctx = { getAuthToken, scope: queryScope };
  const snoozeKey = accountId && internalUserId ? firstRunSnoozeKey(accountId, internalUserId) : null;

  const query = useQuery({ ...chatEmptyQueryOptions(ctx), enabled: active });

  const [snoozedUntil, setSnoozedUntil] = useState<number | null>(null);
  useEffect(() => setSnoozedUntil(readSnooze(snoozeKey)), [snoozeKey]);

  if (!active) return null;

  const data = query.data;
  const connections: FirstRunInput["connections"] = query.isError ? "error" : !data ? null :
    data.connections_status === "unavailable" ? "unavailable" : data.connections_status === "pending" ? null : data.connections;
  const teammates: FirstRunInput["teammates"] = !org.isAdmin ? "unavailable" : query.isError ? "error" :
    data?.teammate_count ?? (data ? "unavailable" : null);
  const conversationCount: FirstRunInput["conversationCount"] = query.isError ? "error" : data?.own_conversation_count ?? null;

  const checklist = deriveFirstRunChecklist({
    hasOrg: org.hasOrg,
    connections,
    teammates,
    conversationCount,
    snoozedUntil,
    now: Date.now(),
  });
  if (!checklist.visible) return null;
  const [connect, invite, ask] = checklist.items;
  const showInvite = invite.state !== "unavailable";

  function snooze() {
    const until = Date.now() + FIRST_RUN_SNOOZE_MS;
    if (snoozeKey) window.localStorage.setItem(snoozeKey, String(until));
    setSnoozedUntil(until);
  }

  return (
    <Card role="region" aria-label="Getting started" className="px-[18px] py-[14px]">
      <Eyebrow>getting started{org.orgName ? ` in ${org.orgName}` : ""}</Eyebrow>
      <ol className="mt-[4px] divide-y divide-line-3">
        <Step n={1} state={connect.state} title="Connect your sources">
          {connect.state === "done" ? (
            <>You&rsquo;ve connected {joinLabels(connect.connected)}. Add more any time from Connectors.</>
          ) : connect.state === "unavailable" ? (
            <>
              Connectors aren&rsquo;t switched on for this workspace yet, so there&rsquo;s nothing to connect here for
              now. Once they are, each member connects Slack, Google or Microsoft with their own account from
              Connectors.
            </>
          ) : (
            <>
              Each member connects Slack, Google or Microsoft with their own account, and captures what they can
              see. Nobody else&rsquo;s connection captures for you.
              {connect.shared.length > 0 && (
                <>
                  {" "}
                  {joinLabels(connect.shared)} {connect.shared.length === 1 ? "is" : "are"} connected for the workspace,
                  but not with your account yet.
                </>
              )}
              {connect.needsReconnect.length > 0 && <> {joinLabels(connect.needsReconnect)} needs reconnecting.</>}
              {connect.state === "unknown" && <> Couldn&rsquo;t check your connections just now.</>}
            </>
          )}
          {connect.state !== "done" && connect.state !== "unavailable" && (
            <div className="mt-[6px]">
              <Link href="/connectors" className={LINK}>
                Open Connectors →
              </Link>
            </div>
          )}
        </Step>
        {showInvite && (
          <Step n={2} state={invite.state} title="Invite your teammates">
            {invite.state === "done" ? (
              <>
                {invite.teammates === 1 ? "1 other person is" : `${invite.teammates} other people are`} in this
                organization or invited to it.
              </>
            ) : invite.state === "unknown" ? (
              <>Couldn&rsquo;t check who&rsquo;s been invited just now.</>
            ) : (
              <>
                Answers get better with everyone&rsquo;s sources. Each person gets a sign-in email and a private
                workspace of their own.
              </>
            )}
            {invite.state !== "done" && (
              <div className="mt-[6px]">
                <Link href="/settings/organization/members" className={LINK}>
                  Invite people →
                </Link>
              </div>
            )}
          </Step>
        )}
        <Step n={showInvite ? 3 : 2} state={ask.state} title="Ask your first question">
          {ask.state === "done"
            ? "You've asked your first question."
            : ask.state === "unknown"
              ? "Couldn't check your chats just now."
              : "Type it in the box below. Answers cite the captured sources they came from."}
        </Step>
      </ol>
      <div className="mt-[6px] flex items-center gap-[10px] border-t border-line-3 pt-[10px]">
        <Button variant="ghost" size="sm" onClick={snooze}>
          Hide for now
        </Button>
        <Mono className="text-ink-4">comes back in 3 days if it&rsquo;s still not done</Mono>
      </div>
    </Card>
  );
}
