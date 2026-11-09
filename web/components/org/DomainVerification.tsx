"use client";

// CS-450 (org-onboarding plan §3 item 53) — the admin surface for CS-443's
// domain-verification backend (`backend/src/routes/org_domains.rs`), which
// shipped with zero web UI: claim a domain, publish (and copy) the DNS TXT
// record it hands back, check that record now, or ask for an email
// confirmation instead — plus remove a domain.
//
// Lives on the members page, directly ABOVE `AutoAddSetting`: a verified
// domain is gate 1 of auto-add (CS-449), so an admin reads this section,
// claims and proves a domain, and sees the toggle right below react to it
// without leaving the page. `AutoAddSetting`'s own gate copy links up here
// (`#domains`) when nothing arms it yet, instead of the dead end it was
// before this story.
//
// FIX (code review on PR #1048): the domain list used to be fetched HERE,
// with `AutoAddSetting` running an independent, identically-shaped fetch of
// the same admin-only `GET /v1/orgs/domains` — two requests for one list,
// and (the actual bug) two separate `useState`s that could disagree: an
// admin who proved a domain via this component watched the toggle directly
// below keep reading "none is verified yet" until a full reload. The read
// now lives on `members/page.tsx`, passed down as `domains`/`loadErr`
// (`AutoAddSetting` receives the same two props) — one fetch, one piece of
// state, both components reacting to the same generation of it. Mutations
// here (`claim`/`verify`/`removeDomain`) patch that shared state directly
// through `onDomainsChange`, the page's own `setDomains`, instead of a
// round trip through a refetch.
//
// Every "what state is this domain in, what do the buttons do, how do we
// read a failure" decision lives in `domainVerificationModel.ts`, tested
// there directly — this file is wiring, state and markup only, the same
// split `AutoAddSetting.tsx`/`autoAddModel.ts` and
// `GmailAddonTokens.tsx`/`addinTokensModel.ts` keep.
//
// Destructive action (Remove) is the house's two-step inline pattern
// (`Remove` → `Confirm`/`Cancel`) — NO `confirm()` anywhere in this app, see
// `GmailAddonTokens.tsx`'s own header comment for why.

import { useEffect, useRef, useState, type Dispatch, type SetStateAction } from "react";
import { Badge, Button, Empty, ErrorNote, Eyebrow, Input, Loading, Mono, Notice, cn } from "@/components/ui";
import { IconCopy } from "@/components/icons";
import { SettingList } from "@/components/settings/SettingRow";
import { useApp } from "@/lib/providers";
import { api, ApiError, type OrgDomain } from "@/lib/api";
import { timeAgo } from "@/lib/teamEvents";
import { domainArmsAutoAdd } from "./autoAddModel";
import {
  claimFailure,
  confirmationFailure,
  dnsCheckFailure,
  domainStatus,
  removeFailure,
  type ConfirmationOutcome,
  type DnsCheckOutcome,
} from "./domainVerificationModel";

/** Click-to-copy field for the TXT record name/value — same shape
 *  `SlackOnboardingChecklist.tsx`'s `CopyInviteCommand` already uses for the
 *  Slack invite command, so a copyable one-liner looks the same everywhere
 *  in the app. Fails soft on a denied clipboard permission or non-secure
 *  context: the value stays printed and selectable either way.
 *
 *  This is a fifth hand-rolled copy-to-clipboard control (code review on PR
 *  #1048) — `CopyInviteCommand` above duplicates the same "Copied" flash,
 *  including the same uncleaned `setTimeout`. A shared control belongs in
 *  `components/ui`, but that touches four other files mid fix-round on an
 *  open PR, so this is scoped to fixing this copy's own leak only — the
 *  extraction is still owed. */
function CopyField({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false);
  const resetTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(
    () => () => {
      if (resetTimer.current) clearTimeout(resetTimer.current);
    },
    [],
  );

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      if (resetTimer.current) clearTimeout(resetTimer.current);
      resetTimer.current = setTimeout(() => setCopied(false), 1800);
    } catch {
      // Clipboard permission denied, or a non-secure context — see this
      // function's own doc comment.
    }
  }

  return (
    <div>
      <Eyebrow className="mb-[3px]">{label}</Eyebrow>
      <div className="flex items-center gap-[8px] rounded-row border border-line bg-bg px-[10px] py-[7px]">
        <code className="min-w-0 flex-1 truncate font-mono text-[11.5px] text-ink">{value}</code>
        <Button
          type="button"
          size="sm"
          icon={<IconCopy width={12} height={12} />}
          onClick={() => void handleCopy()}
        >
          {copied ? "Copied" : "Copy"}
        </Button>
      </div>
    </div>
  );
}

function DnsCheckNote({ outcome }: { outcome: DnsCheckOutcome }) {
  return (
    <p
      className={cn(
        "mt-[8px] text-[11.5px] leading-[1.5]",
        // A resolver we couldn't reach (502) reads as a quiet infrastructure
        // note, not a warning about the admin's own DNS record — see
        // `domainVerificationModel.ts`'s own header comment, point 1, for
        // why these two are never allowed to look the same.
        outcome.kind === "resolver_unreachable" ? "text-ink-3" : "text-warn",
      )}
    >
      {outcome.message}
    </p>
  );
}

interface DomainRowProps {
  d: OrgDomain;
  verifying: boolean;
  verifyOutcome: DnsCheckOutcome | undefined;
  onVerify: (d: OrgDomain) => void;
  confirmEmail: string;
  onConfirmEmailChange: (v: string) => void;
  sending: boolean;
  confirmOutcome: ConfirmationOutcome | undefined;
  onSendConfirmation: (d: OrgDomain) => void;
  onResetConfirm: (id: string) => void;
  removeConfirming: boolean;
  removing: boolean;
  onRemoveClick: (id: string) => void;
  onRemoveCancel: () => void;
  onRemoveConfirm: (d: OrgDomain) => void;
}

function DomainRow({
  d,
  verifying,
  verifyOutcome,
  onVerify,
  confirmEmail,
  onConfirmEmailChange,
  sending,
  confirmOutcome,
  onSendConfirmation,
  onResetConfirm,
  removeConfirming,
  removing,
  onRemoveClick,
  onRemoveCancel,
  onRemoveConfirm,
}: DomainRowProps) {
  const status = domainStatus(d);
  const armed = domainArmsAutoAdd(d);
  // BLOCKER 2 fix (code review on PR #1048): gated on the SAME reading the
  // auto-add toggle itself uses (`domainArmsAutoAdd`, not `d.verified`) —
  // an email-verified domain still has somewhere stronger to go. The
  // backend's own `verify_dns` short-circuit (`backend/src/routes/
  // org_domains.rs`) only skips a row already `dns_txt`-verified; a row
  // verified by email falls through and CAN be upgraded by publishing (and
  // checking) this same TXT record. Hiding this panel on `d.verified` alone
  // — the bug — stranded that path behind "remove and re-claim", with
  // nothing on screen saying so.
  const showTxtPanel = !armed;
  // The email panel, unlike the TXT one, stays hidden once ANY proof
  // exists: the backend still refuses a second `confirmation` with 409
  // "already verified" (`request_confirmation`) once `verified_at` is set,
  // so offering it again would be exactly the dead click the original
  // `!d.verified` guard was written to avoid — that reasoning still holds
  // for this panel specifically.
  const showEmailPanel = !d.verified;
  const proofPending = confirmOutcome && confirmOutcome.kind !== "sent" && confirmOutcome.kind !== "not_configured";

  return (
    // No `border-b`/`last:border-b-0` here (code review, item 8) — the
    // parent `SettingList` is a `Card` with `divide-y divide-line-3`
    // already; this doubled the hairline, and `last:` never even fired
    // since the claim-domain row and its hint footer are always later
    // siblings inside the same list.
    <div className="px-[15px] py-[13px]">
      <div className="flex items-center gap-[10px]">
        <div className="min-w-0 flex-1">
          <span className="text-[13.5px] font-medium text-ink">{d.domain}</span>
          <p className="mt-[3px] text-[12px] leading-[1.5] text-ink-3">
            {status.label}
            {d.verified_at && <span className="ml-[8px] text-ink-4">· verified {timeAgo(d.verified_at)}</span>}
          </p>
        </div>
        <Badge tone={status.kind === "unverified" ? "warn" : "ok"} outline={status.kind !== "arms_auto_add"}>
          {status.tag}
        </Badge>
        {removeConfirming ? (
          <span className="flex flex-none items-center gap-[6px]">
            <Button variant="danger" size="sm" disabled={removing} onClick={() => onRemoveConfirm(d)}>
              {removing ? "Removing…" : "Confirm"}
            </Button>
            <Button size="sm" disabled={removing} onClick={onRemoveCancel}>
              Cancel
            </Button>
          </span>
        ) : (
          <Button variant="danger" size="sm" className="flex-none" onClick={() => onRemoveClick(d.id)}>
            Remove
          </Button>
        )}
      </div>

      {(showTxtPanel || showEmailPanel) && (
        <div
          className={cn(
            "mt-[12px] grid grid-cols-1 gap-[12px]",
            // Two columns only when both panels are actually showing — a
            // lone panel (the email-verified upgrade case) stays full width
            // rather than sitting half-empty.
            showTxtPanel && showEmailPanel && "sm:grid-cols-2",
          )}
        >
          {showTxtPanel && (
            <div className="rounded-control border border-line-2 bg-bg-2 p-[11px]">
              <Eyebrow className="mb-[7px]">dns txt record — arms auto-add</Eyebrow>
              {d.verified && (
                <p className="mb-[7px] text-[11.5px] leading-[1.5] text-ink-3">
                  {d.domain} is already proven by email — publishing this record is what arms auto-add.
                </p>
              )}
              <div className="flex flex-col gap-[7px]">
                <CopyField label="name" value={d.dns_record_name} />
                <CopyField label="value" value={d.dns_record_value} />
              </div>
              <Button
                variant="secondary"
                size="sm"
                className="mt-[10px]"
                disabled={verifying}
                onClick={() => onVerify(d)}
              >
                {verifying ? "Checking…" : "Check DNS now"}
              </Button>
              {verifyOutcome && <DnsCheckNote outcome={verifyOutcome} />}
            </div>
          )}

          {showEmailPanel && (
            <div className="rounded-control border border-line-2 bg-bg-2 p-[11px]">
              <Eyebrow className="mb-[7px]">email confirmation — proves control only</Eyebrow>
              <p className="mb-[8px] text-[11.5px] leading-[1.5] text-ink-3">
                Proves you control {d.domain}, same as DNS — but auto-add only arms from a DNS-verified domain.
              </p>
              {confirmOutcome?.kind === "sent" ? (
                <Notice
                  action={
                    <Button variant="ghost" size="sm" onClick={() => onResetConfirm(d.id)}>
                      Send another
                    </Button>
                  }
                >
                  Sent — check the inbox at that address.
                </Notice>
              ) : confirmOutcome?.kind === "not_configured" ? (
                <Notice tone="warn">{confirmOutcome.message}</Notice>
              ) : (
                <>
                  {/* item 4, code review on PR #1048: a reload used to erase
                      that a link was already outstanding — an admin would
                      retype the address and walk straight into the cooldown
                      409 with nothing on screen explaining which earlier
                      send it meant. `d.confirmation_sent_at` survives a
                      reload; this session's own `confirmOutcome` (above)
                      still wins once one exists. */}
                  {!confirmOutcome && d.confirmation_sent_at && (
                    <p className="mb-[7px] text-[11.5px] leading-[1.5] text-ink-4">
                      Last sent to {d.confirmation_email ?? "that address"} {timeAgo(d.confirmation_sent_at)}.
                    </p>
                  )}
                  <div className="flex items-center gap-[6px]">
                    <Input
                      type="email"
                      placeholder={`name@${d.domain}`}
                      value={confirmEmail}
                      onChange={(e) => onConfirmEmailChange(e.target.value)}
                      onKeyDown={(e) => {
                        // item 7, code review on PR #1048 — same
                        // input+button Enter-submit every other add row in
                        // the app already binds (`WorkspaceMembers.tsx`,
                        // `NewWorkspace.tsx`,
                        // `onboarding/org/invite/page.tsx`).
                        if (e.key === "Enter" && !sending && confirmEmail.trim()) {
                          e.preventDefault();
                          onSendConfirmation(d);
                        }
                      }}
                      wrapperClassName="flex-1"
                      aria-label={`Confirmation address at ${d.domain}`}
                    />
                    <Button
                      variant="secondary"
                      size="sm"
                      disabled={sending || !confirmEmail.trim()}
                      onClick={() => onSendConfirmation(d)}
                    >
                      {sending ? "Sending…" : "Send link"}
                    </Button>
                  </div>
                  {proofPending && confirmOutcome && (
                    <p className="mt-[7px] text-[11.5px] leading-[1.5] text-warn">{confirmOutcome.message}</p>
                  )}
                </>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

interface DomainVerificationProps {
  /** Lifted to `members/page.tsx` (BLOCKER 1, code review on PR #1048) — see
   *  this file's own header comment for why. `null` = not loaded yet (or the
   *  read failed); the page's own effect is the only place this is fetched. */
  domains: OrgDomain[] | null;
  loadErr: string | null;
  /** `true` on a 403 (`domainsListFailure`) — a dead end, so the retry
   *  action is withheld rather than offered as a click that can never
   *  succeed (item 1, code review on PR #1048). */
  loadForbidden: boolean;
  onRetry: () => void;
  /** Every mutation here (`claim`/`verify`/`removeDomain`) patches the
   *  page's own `domains` state directly through this — the same setter the
   *  page hands its `useState`, not a second copy of the list. */
  onDomainsChange: Dispatch<SetStateAction<OrgDomain[] | null>>;
}

export function DomainVerification({ domains, loadErr, loadForbidden, onRetry, onDomainsChange }: DomainVerificationProps) {
  const { getAuthToken } = useApp();

  const [claimDomain, setClaimDomain] = useState("");
  const [claiming, setClaiming] = useState(false);
  const [claimErr, setClaimErr] = useState<string | null>(null);

  // item 3, code review on PR #1048: keyed per row, not a single value —
  // every row's buttons are independently clickable, so a single shared id
  // let one row's completing request clear ANOTHER row's in-flight state
  // and re-enable its button mid-request (duplicate verifies, duplicate
  // confirmation emails that then tripped the cooldown 409, a double
  // DELETE). Same shape `verifyOutcome`/`confirmOutcome` already use.
  const [verifyingIds, setVerifyingIds] = useState<Record<string, boolean>>({});
  const [verifyOutcome, setVerifyOutcome] = useState<Record<string, DnsCheckOutcome | undefined>>({});

  const [confirmEmail, setConfirmEmail] = useState<Record<string, string>>({});
  const [sendingIds, setSendingIds] = useState<Record<string, boolean>>({});
  const [confirmOutcome, setConfirmOutcome] = useState<Record<string, ConfirmationOutcome | undefined>>({});

  const [removeConfirmId, setRemoveConfirmId] = useState<string | null>(null);
  const [removingIds, setRemovingIds] = useState<Record<string, boolean>>({});
  const [removeErr, setRemoveErr] = useState<string | null>(null);

  async function claim() {
    const value = claimDomain.trim();
    if (!value) return;
    const token = getAuthToken();
    if (!token) return;
    setClaiming(true);
    setClaimErr(null);
    try {
      const claimed = await api.orgs.domains.claim(token, value);
      setClaimDomain("");
      onDomainsChange((prev) => {
        if (!prev) return [claimed];
        // Claiming the same domain twice returns the SAME row (200,
        // idempotent) rather than a new one — replace by id instead of
        // appending a duplicate.
        const exists = prev.some((x) => x.id === claimed.id);
        return exists ? prev.map((x) => (x.id === claimed.id ? claimed : x)) : [...prev, claimed];
      });
    } catch (e) {
      setClaimErr(claimFailure(e).message);
    } finally {
      setClaiming(false);
    }
  }

  async function verify(d: OrgDomain) {
    const token = getAuthToken();
    if (!token) return;
    setVerifyingIds((prev) => ({ ...prev, [d.id]: true }));
    try {
      const updated = await api.orgs.domains.verify(token, d.id);
      onDomainsChange((prev) => (prev ? prev.map((x) => (x.id === updated.id ? updated : x)) : prev));
      setVerifyOutcome((prev) => ({ ...prev, [d.id]: undefined }));
    } catch (e) {
      // item 2, code review on PR #1048: a 404 means the client's OWN list
      // is stale (this row is already gone) — same "refetch, don't just
      // show calmer copy" precedent `members/page.tsx`'s own invite
      // handlers hold for `not_pending`.
      if (e instanceof ApiError && e.status === 404) onRetry();
      setVerifyOutcome((prev) => ({ ...prev, [d.id]: dnsCheckFailure(e) }));
    } finally {
      setVerifyingIds((prev) => ({ ...prev, [d.id]: false }));
    }
  }

  async function sendConfirmation(d: OrgDomain) {
    // Prefills from `d.confirmation_email` (item 4) whenever this session
    // hasn't typed its own override yet — see `DomainRow`'s own value prop.
    const email = (confirmEmail[d.id] ?? d.confirmation_email ?? "").trim();
    if (!email) return;
    const token = getAuthToken();
    if (!token) return;
    setSendingIds((prev) => ({ ...prev, [d.id]: true }));
    try {
      await api.orgs.domains.requestConfirmation(token, d.id, email);
      setConfirmOutcome((prev) => ({ ...prev, [d.id]: { kind: "sent" } }));
    } catch (e) {
      // item 2: a 404 ("gone from the list") or a 409 "already verified"
      // both mean the client's list is stale — the cooldown 409 ("sent
      // very recently") is NOT stale, so it must not trigger a refetch.
      const stale = e instanceof ApiError && (e.status === 404 || (e.status === 409 && e.message === "that domain is already verified"));
      if (stale) onRetry();
      setConfirmOutcome((prev) => ({ ...prev, [d.id]: confirmationFailure(e) }));
    } finally {
      setSendingIds((prev) => ({ ...prev, [d.id]: false }));
    }
  }

  function resetConfirm(id: string) {
    setConfirmOutcome((prev) => ({ ...prev, [id]: undefined }));
    setConfirmEmail((prev) => ({ ...prev, [id]: "" }));
  }

  async function removeDomain(d: OrgDomain) {
    const token = getAuthToken();
    if (!token) return;
    setRemovingIds((prev) => ({ ...prev, [d.id]: true }));
    setRemoveErr(null);
    try {
      await api.orgs.domains.remove(token, d.id);
      onDomainsChange((prev) => (prev ? prev.filter((x) => x.id !== d.id) : prev));
      setRemoveConfirmId(null);
    } catch (e) {
      // item 2: already gone server-side — the client's list is stale.
      if (e instanceof ApiError && e.status === 404) onRetry();
      setRemoveErr(removeFailure(e));
    } finally {
      setRemovingIds((prev) => ({ ...prev, [d.id]: false }));
    }
  }

  return (
    <div id="domains" className="scroll-mt-[24px]">
      <div className="mb-[9px] flex items-center gap-[9px]">
        <Eyebrow>verified domains</Eyebrow>
        {domains && domains.length > 0 && (
          <Badge tone="ink" className="px-[6px] py-[1px] text-[9px]">
            {domains.length}
          </Badge>
        )}
      </div>
      <p className="mb-[10px] max-w-[68ch] text-[12.5px] leading-[1.55] text-ink-2">
        Prove your organization controls a domain, two ways. Publishing a DNS TXT record is the only
        proof that arms auto-add; confirming by email proves control too, but never arms it.
      </p>

      {loadErr ? (
        <ErrorNote
          className="mb-[16px]"
          action={
            // item 1, code review on PR #1048: a 403 (`domainsListFailure`,
            // not an org admin or a personal account) is a dead end — no
            // retry changes the answer — same precedent this page's own
            // roster read already holds for its 403. Every other list-read
            // failure is worth retrying.
            loadForbidden ? undefined : (
              <Button variant="secondary" size="sm" onClick={onRetry}>
                Try again
              </Button>
            )
          }
        >
          {loadErr}
        </ErrorNote>
      ) : domains === null ? (
        <Loading what="verified domains" />
      ) : (
        <>
          <SettingList className="mb-[16px]">
            {domains.length === 0 ? (
              <div className="px-[15px] py-[16px]">
                <Empty
                  title="No domains claimed yet"
                  hint="Claim a domain your organization controls below, then prove it — auto-add stays off until one is verified by DNS."
                />
              </div>
            ) : (
              domains.map((d) => (
                <DomainRow
                  key={d.id}
                  d={d}
                  verifying={!!verifyingIds[d.id]}
                  verifyOutcome={verifyOutcome[d.id]}
                  onVerify={(dom) => void verify(dom)}
                  confirmEmail={confirmEmail[d.id] ?? d.confirmation_email ?? ""}
                  onConfirmEmailChange={(v) => setConfirmEmail((prev) => ({ ...prev, [d.id]: v }))}
                  sending={!!sendingIds[d.id]}
                  confirmOutcome={confirmOutcome[d.id]}
                  onSendConfirmation={(dom) => void sendConfirmation(dom)}
                  onResetConfirm={resetConfirm}
                  removeConfirming={removeConfirmId === d.id}
                  removing={!!removingIds[d.id]}
                  onRemoveClick={(id) => {
                    setRemoveErr(null);
                    setRemoveConfirmId(id);
                  }}
                  onRemoveCancel={() => setRemoveConfirmId(null)}
                  onRemoveConfirm={(dom) => void removeDomain(dom)}
                />
              ))
            )}
            <div className="flex items-center gap-[8px] border-t border-line-3 bg-bg-subtle px-[15px] py-[12px]">
              <Input
                placeholder="acme.com"
                value={claimDomain}
                onChange={(e) => setClaimDomain(e.target.value)}
                onKeyDown={(e) => {
                  // item 7, code review on PR #1048 — same Enter-submit
                  // every other add row in the app binds on this exact
                  // input+button pairing.
                  if (e.key === "Enter" && !claiming && claimDomain.trim()) {
                    e.preventDefault();
                    void claim();
                  }
                }}
                wrapperClassName="flex-1"
                aria-label="Domain to claim"
              />
              <Button variant="brand" disabled={claiming || !claimDomain.trim()} onClick={() => void claim()}>
                {claiming ? "Claiming…" : "Claim domain"}
              </Button>
            </div>
            <div className="border-t border-line-3 bg-bg-subtle px-[15px] pb-[11px]">
              <Mono>an org-owned domain, not a public email provider</Mono>
            </div>
          </SettingList>

          {claimErr && (
            <div className="mb-[16px] -mt-[8px]">
              <ErrorNote>{claimErr}</ErrorNote>
            </div>
          )}
          {removeErr && (
            <div className="mb-[16px] -mt-[8px]">
              <ErrorNote>{removeErr}</ErrorNote>
            </div>
          )}
        </>
      )}
    </div>
  );
}
