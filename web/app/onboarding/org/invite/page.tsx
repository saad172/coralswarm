"use client";

// 6e — Step 5 · Invite. Sends real invites through `POST /v1/orgs/invites`
// (the same call the members page makes), one address at a time. Each invite
// takes a free seat. A batch bigger than the free seats first goes through the
// same seat quote → confirm step as Billing (`useInviteSeats`), so no seat is
// bought without its price shown. Every invite is sent with
// `allow_purchase: false`, so the server refuses (409 `no_free_seat`) rather
// than buying when the pool moved or could not be read; that reopens the quote
// for the invites still unsent. What an invited member sees on the other end
// is 6g (app/onboarding/join), not this page.

import { useState, type KeyboardEvent } from "react";
import { useRouter } from "next/navigation";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Button, Loading, Mono, Notice, Select } from "@/components/ui";
import { StepRail, OnboardingChrome, HeaderPill } from "@/components/onboarding/OnboardingChrome";
import { StepCard, WizardActions } from "@/components/onboarding/StepCard";
import { ORG_STEPS } from "@/components/onboarding/orgSteps";
import { useOrgAdmin } from "@/components/org/useOrgAdmin";
import { useInviteSeats } from "@/components/org/useInviteSeats";
import { useApp } from "@/lib/providers";
import { api, type SlotPoolResult } from "@/lib/api";
import { appInvalidation, orgSlotsQueryOptions } from "@/lib/appQueries";
import {
  inviteStopsBatch,
  isNoFreeSeat,
  mapAddMemberError,
  seatsAddedNote,
  seatsAfterRefusal,
  slotPlan,
  validateSingleEmail,
  type SlotPlan,
} from "@/lib/orgInvites";

type RowStatus = "queued" | "sending" | "sent" | "failed" | "skipped";
interface InviteRow {
  email: string;
  status: RowStatus;
  error?: string;
  emailSent?: boolean;
}

const plural = (n: number, one: string, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

function SlotNote({ plan, count, poolFailed }: { plan: SlotPlan; count: number; poolFailed: boolean }) {
  if (plan.kind === "unknown") {
    return (
      <Notice tone="warn" className="mb-[14px]">
        {poolFailed
          ? "Couldn't read your seats. Each invite uses a free seat; if one needs a new seat, you'll see its price before anything is bought."
          : "Checking your free seats…"}
      </Notice>
    );
  }
  if (plan.kind === "unbilled") {
    return (
      <p className="mb-[14px] text-[12.5px] leading-[1.55] text-ink-2">
        No seats have been bought for this organization yet, so sending these invites doesn&rsquo;t charge anything
        today.
      </p>
    );
  }
  const { free, fromPool, toBuy } = plan;
  if (toBuy === 0) {
    return (
      <p className="mb-[14px] text-[12.5px] leading-[1.55] text-ink-2">
        {count === 0
          ? `You have ${plural(free, "free seat")}. Each invite uses one; past that, you'll see the price of more seats before anything is sent.`
          : `${plural(count, "invite")} ${count === 1 ? "uses" : "use"} ${fromPool} of your ${plural(free, "free seat")}. Nothing new is bought.`}
      </p>
    );
  }
  return (
    <Notice tone="warn" className="mb-[14px]">
      {free === 0
        ? `Every seat is taken, so ${count === 1 ? "this invite needs" : `these ${plural(count, "invite")} need`} ${plural(toBuy, "more seat")}.`
        : `You have ${plural(free, "free seat")}. ${plural(fromPool, "invite")} ${fromPool === 1 ? "uses" : "use"} ${free === 1 ? "it" : "them"}; the other ${toBuy} need ${toBuy === 1 ? "one more seat" : "more seats"}.`}{" "}
      You&rsquo;ll see the price before anything is sent. Revoking an invite frees its seat for the next person.
    </Notice>
  );
}

function rowNote(row: InviteRow): { text: string; tone: "ok" | "bad" | "muted" } | null {
  switch (row.status) {
    case "sending":
      return { text: "sending…", tone: "muted" };
    case "sent":
      return row.emailSent
        ? { text: "sent", tone: "ok" }
        : { text: "seat held, but the email didn't go out. Resend it from Members.", tone: "bad" };
    case "failed":
      return { text: row.error ?? "Couldn't send this invite.", tone: "bad" };
    case "skipped":
      return { text: "not sent — fix the problem above, then send again", tone: "muted" };
    default:
      return null;
  }
}

const NOTE_TONE = { ok: "text-ok", bad: "text-bad", muted: "text-ink-3" } as const;

export default function OrgInvitePage() {
  const router = useRouter();
  const { ready, authReady, getAuthToken, queryScope } = useApp();
  const qc = useQueryClient();
  const org = useOrgAdmin();
  const canInvite = org.loaded && ready && authReady && org.hasOrg && org.isAdmin;

  const [rows, setRows] = useState<InviteRow[]>([]);
  const [draft, setDraft] = useState("");
  const [draftErr, setDraftErr] = useState<string | null>(null);
  const [role, setRole] = useState<"member" | "admin">("member");
  const [sending, setSending] = useState(false);
  const inviteSeats = useInviteSeats();

  // The same cached seat pool the Members page reads.
  const slotsOptions = orgSlotsQueryOptions({ getAuthToken, scope: queryScope });
  const poolQuery = useQuery({ ...slotsOptions, enabled: canInvite });
  const pool = poolQuery.data ?? null;
  const poolFailed = poolQuery.isError;
  // Sending before the count arrives would skip the quote and lean on the
  // server's 409; wait for it instead. A failed read is not "loading": that
  // case sends, and the 409 path prices any seat it needs.
  const poolLoading = poolQuery.isLoading;

  const unsent = rows.filter((r) => r.status === "queued" || r.status === "failed" || r.status === "skipped");
  const sent = rows.filter((r) => r.status === "sent");
  const plan = slotPlan(unsent.length, pool);
  const toBuy = plan.kind === "known" ? plan.toBuy : 0;

  function addDraft() {
    const email = draft.trim().toLowerCase();
    if (!email) return;
    const err = validateSingleEmail(email);
    if (err) {
      setDraftErr(err);
      return;
    }
    setDraftErr(null);
    setRows((prev) => (prev.some((r) => r.email === email) ? prev : [...prev, { email, status: "queued" }]));
    setDraft("");
  }
  function onKeyDown(e: KeyboardEvent<HTMLInputElement>) {
    if (e.key === "Enter" || e.key === ",") {
      e.preventDefault();
      addDraft();
    }
  }
  const patch = (email: string, next: Partial<InviteRow>) =>
    setRows((prev) => prev.map((r) => (r.email === email ? { ...r, ...next } : r)));

  async function sendAll() {
    if (sending || poolLoading) return;
    const batch = unsent.map((r) => r.email);
    inviteSeats.clearError();
    if (plan.kind === "known" && plan.toBuy > 0) {
      await inviteSeats.requestSeats(plan.toBuy, plan.free, batch.length, () => void sendBatch(batch));
      return;
    }
    await sendBatch(batch);
  }

  async function sendBatch(batch: string[]) {
    const token = getAuthToken();
    if (!token) return;
    setSending(true);
    let anyFailed = false;
    let refused: string[] | null = null;
    try {
      // Sequential: a failure that would repeat (payment, permission, no free
      // seat) must stop the rest rather than be retried once per address.
      for (let i = 0; i < batch.length; i++) {
        const email = batch[i];
        patch(email, { status: "sending", error: undefined });
        try {
          const res = await api.orgs.invites.create(token, { email, role, allow_purchase: false });
          patch(email, { status: "sent", emailSent: res.email_sent });
        } catch (e) {
          if (isNoFreeSeat(e)) {
            // No seat was bought for this address. Put it and the rest back
            // in the queue and price the seats they need.
            refused = batch.slice(i);
            const back = new Set(refused);
            setRows((prev) => prev.map((r) => (back.has(r.email) ? { ...r, status: "queued", error: undefined } : r)));
            break;
          }
          anyFailed = true;
          patch(email, { status: "failed", error: mapAddMemberError(e) });
          if (inviteStopsBatch(e)) {
            const rest = new Set(batch.slice(i + 1));
            setRows((prev) => prev.map((r) => (rest.has(r.email) ? { ...r, status: "skipped" } : r)));
            break;
          }
        }
      }
    } finally {
      setSending(false);
      // Invites take seats; the Members page and this note read the pool.
      void qc.invalidateQueries({ queryKey: appInvalidation.orgSlots() });
      void qc.invalidateQueries({ queryKey: appInvalidation.orgDirectory() });
    }
    // Everything went out: any seats bought for it are now in use.
    if (!anyFailed && !refused) inviteSeats.clearAdded();
    if (refused) await priceRefused(refused);
  }

  /** The server had no free seat for `remaining`. Re-read the pool (the
   *  cached count was stale or missing) and open the quote for them. */
  async function priceRefused(remaining: string[]) {
    let fresh: SlotPoolResult | null = null;
    try {
      fresh = await qc.fetchQuery({ ...slotsOptions, staleTime: 0 });
    } catch {
      fresh = null;
    }
    const { toBuy, free } = seatsAfterRefusal(remaining.length, fresh);
    await inviteSeats.requestSeats(toBuy, free, remaining.length, () => void sendBatch(remaining));
  }

  const finish = () => router.push("/chats");

  let body;
  if (!org.loaded || !ready) {
    body = <Loading what="checking your organization" />;
  } else if (!authReady || !org.hasOrg) {
    body = (
      <Notice tone="warn" className="mb-4">
        Switch to your organization to invite people. Invites are sent from inside the organization they join.
      </Notice>
    );
  } else if (!org.isAdmin) {
    body = (
      <Notice tone="warn" className="mb-4">
        Only an org admin can invite people. Ask an admin of {org.orgName ?? "your organization"} to send them, or
        skip this step.
      </Notice>
    );
  } else {
    body = (
      <>
        <label
          htmlFor="invite-email"
          className="mb-[6px] block font-mono text-[9.5px] uppercase tracking-[0.13em] text-ink-3"
        >
          invite by email
        </label>
        <div
          className="mb-[6px] rounded-field border-[1.5px] border-ink p-[11px_12px]"
          style={{ boxShadow: "0 0 0 3px color-mix(in srgb, var(--ink) 8%, transparent)" }}
        >
          {rows.length > 0 && (
            <ul className="mb-[8px] flex flex-col gap-[6px]">
              {rows.map((row) => {
                const note = rowNote(row);
                const removable = !sending && row.status !== "sent" && row.status !== "sending";
                return (
                  <li key={row.email} className="flex flex-wrap items-center gap-[8px] text-[12.5px]">
                    <span className="inline-flex items-center gap-[6px] rounded-row bg-bg-2 px-[8px] py-[3px] text-ink">
                      {row.email}
                      {removable && (
                        <button
                          type="button"
                          className="text-ink-4 hover:text-ink"
                          onClick={() => setRows((prev) => prev.filter((r) => r.email !== row.email))}
                          aria-label={`Remove ${row.email}`}
                        >
                          ×
                        </button>
                      )}
                    </span>
                    {note && <span className={`leading-[1.5] ${NOTE_TONE[note.tone]}`}>{note.text}</span>}
                  </li>
                );
              })}
            </ul>
          )}
          <input
            id="invite-email"
            value={draft}
            onChange={(e) => {
              setDraft(e.target.value);
              if (draftErr) setDraftErr(null);
            }}
            onKeyDown={onKeyDown}
            onBlur={addDraft}
            disabled={sending}
            placeholder={rows.length ? "add another…" : "name@company.com"}
            className="w-full bg-transparent p-[3px_2px] text-[12.5px] text-ink outline-none placeholder:text-ink-4"
          />
        </div>
        {draftErr ? (
          <p className="mb-[12px] text-[12.5px] leading-[1.5] text-bad">{draftErr}</p>
        ) : (
          <Mono className="mb-[12px] block text-ink-4">press enter after each address</Mono>
        )}

        <div className="mb-[14px] flex items-end gap-[9px]">
          <Select
            label="role"
            value={role}
            disabled={sending}
            onChange={(e) => setRole(e.target.value === "admin" ? "admin" : "member")}
            wrapperClassName="w-[160px]"
          >
            <option value="member">Member</option>
            <option value="admin">Admin</option>
          </Select>
          <span className="pb-[10px] font-mono text-[10.5px] text-ink-4">applies to everyone in this batch</span>
        </div>

        <SlotNote plan={plan} count={unsent.length} poolFailed={poolFailed} />
        {inviteSeats.error && (
          <Notice tone="warn" className="mb-[14px]">
            {inviteSeats.error}
          </Notice>
        )}
        {inviteSeats.dialog}

        {inviteSeats.added > 0 && unsent.length > 0 && !sending && (
          <Notice tone="warn" className="mb-[14px]">
            {seatsAddedNote(inviteSeats.added)}
          </Notice>
        )}

        {sent.length > 0 && (
          <Notice className="mb-[14px]">
            {sent.length === rows.length
              ? `${plural(sent.length, "invite")} sent.`
              : `${sent.length} of ${plural(rows.length, "invite")} sent.`}{" "}
            Each person gets a sign-in email and lands in the organization&rsquo;s shared workspace.
          </Notice>
        )}
      </>
    );
  }

  const allSent = rows.length > 0 && unsent.length === 0 && !sending;

  return (
    <OnboardingChrome
      right={
        <>
          <StepRail steps={ORG_STEPS} currentIndex={4} />
          {org.hasOrg && org.orgName && <HeaderPill tone="ok">{org.orgName}</HeaderPill>}
        </>
      }
    >
      <StepCard
        eyebrow="step 5 of 5"
        title="Bring your team in"
        description="Everyone lands in one shared workspace, and everyone also gets a workspace of their own. You can't read theirs."
        width={560}
      >
        {body}
        <WizardActions>
          {allSent ? (
            <Button variant="brand" onClick={finish}>
              Continue →
            </Button>
          ) : (
            <>
              {canInvite && (
                <Button
                  variant="brand"
                  disabled={unsent.length === 0 || sending || inviteSeats.pricing || poolLoading}
                  onClick={() => void sendAll()}
                >
                  {sending
                    ? "Sending…"
                    : inviteSeats.pricing
                      ? "Pricing…"
                      : poolLoading && unsent.length > 0
                        ? "Checking seats…"
                        : `Send ${plural(unsent.length, "invite")}${toBuy > 0 ? ` · adds ${plural(toBuy, "seat")}` : ""}`}
                </Button>
              )}
              <Button variant="secondary" disabled={sending} onClick={finish}>
                {sent.length > 0 ? "Done for now" : "Skip — I’ll invite later"}
              </Button>
            </>
          )}
        </WizardActions>
      </StepCard>
    </OnboardingChrome>
  );
}
