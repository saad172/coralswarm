"use client";

// CS-449 (org-onboarding plan §3 item 38, D11 "ask once") — the settings row
// that flips CS-447's `org_auto_add_settings` on, plus the "ask once" dialog
// D11 requires on that exact flip. Neither had ANY web UI before this story
// (CS-447/#1037 shipped `GET`/`PUT /v1/orgs/auto-add` backend-only) — this is
// the first surface either setting gets.
//
// Lives on the members page, directly above `JoinRequestQueue`: the two are
// the closest possible neighbours — flipping this on is exactly what can
// make requests already sitting in that queue admissible, and D11's dialog
// is the bridge between the two so an admin never has to notice that on
// their own.
//
// The "should we ask? what do we say?" logic all lives in `autoAddModel.ts`,
// tested there directly — this component only wires fetches to that model
// and renders what it returns, the same split `ConnectorHealthNotice.tsx`
// keeps against `connectorHealthModel.ts`.
//
// FIX (code review on PR #1048, BLOCKER 1): `domains`/`domainsErr` used to
// be fetched HERE too — an independent, identically-shaped read of the same
// admin-only `GET /v1/orgs/domains` `DomainVerification.tsx` was already
// making, in its own `useState` that could disagree with that component's.
// Both now receive the SAME `domains`/`domainsErr` as props from
// `members/page.tsx`, which owns the one fetch — see that file's own
// comment on the two components, and `DomainVerification.tsx`'s own header
// comment for the full story.

import { useCallback, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { Avatar, Button, ErrorNote, Modal, Mono, Notice, Select, Toggle, hueForName } from "@/components/ui";
import { SettingList, SettingRow } from "@/components/settings/SettingRow";
import { useApp } from "@/lib/providers";
import { api, ApiError, type AutoAddSettings, type OrgDomain } from "@/lib/api";
import { useMembersAdmin, membersAdminKey } from "@/lib/useMembersAdmin";
import { refreshJoinRequestCount } from "./useJoinRequestCount";
import { timeAgo } from "@/lib/teamEvents";
import {
  type AdmitOutcomeSummary,
  type AskOnceDialogPlan,
  admitOutcomeReasonClause,
  askOnceCopy,
  askOnceMaxAdmit,
  askOnceRequestIds,
  autoAddDomainState,
  autoAddGateCopy,
  planAskOnceDialog,
  summarizeAdmitOutcome,
} from "./autoAddModel";
import { FALLBACK_TIERS, resolveTierCode, tierOptionLabel, type TierOption } from "./seatTierOptions";

function mapAutoAddSaveError(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.status === 400) return "That slot tier isn't available.";
    if (e.status === 403) return "Only an admin can change this.";
    if (e.status === 503) return "Briefly unavailable — try again.";
  }
  return "Couldn't save that — try again.";
}

function mapAdmitError(e: unknown): string {
  if (e instanceof ApiError) {
    if (e.status === 403) return "Only an admin can do this.";
    if (e.status === 503) return "Briefly unavailable — try again.";
  }
  return "Couldn't admit them — try again.";
}

export function AutoAddSetting({
  /** Lifted to `members/page.tsx` (BLOCKER 1, code review on PR #1048) —
   *  see this file's own header comment. `null` = not loaded yet, or the
   *  read failed (`domainsErr` names which). */
  domains,
  domainsErr,
  /** The page's own retry for the SAME read — `DomainVerification.tsx`'s
   *  "Try again" calls the identical function, so a fix from either side of
   *  the page is visible on both immediately, no reload required. */
  onRetryDomains,
  /** Fired after a successful admit-queued click — the roster and slot pool
   *  both change (an admit is a membership plus a spent slot, same as an
   *  approval), so the parent's own `refetch` belongs here too, same
   *  contract `JoinRequestQueue`'s `onDecided` already keeps. */
  onAdmitted,
}: {
  domains: OrgDomain[] | null;
  domainsErr: string | null;
  onRetryDomains: () => void;
  onAdmitted?: () => void;
}) {
  const { getAuthToken, queryScope } = useApp();
  const qc = useQueryClient();
  const admin = useMembersAdmin();
  const certificate = JSON.stringify([queryScope, admin.computedAt?.toISOString(), admin.authorityRevision]);
  const [receipt, setReceipt] = useState<{ certificate: string; value: AutoAddSettings } | null>(null);
  const settings = admin.data ? receipt?.certificate === certificate ? receipt.value : admin.data.auto_add : null;
  const settingsErr = admin.error ? (admin.error instanceof ApiError ? admin.error.message : "Couldn't load the auto-add setting.") : null;
  const setSettings = (next: AutoAddSettings) => {
    setReceipt({ certificate, value: next });
    void qc.invalidateQueries({ queryKey: membersAdminKey(queryScope) }, { cancelRefetch: false });
  };
  const tiers = admin.data?.seat_tiers;
  const tierOptions: TierOption[] = tiers && tiers.length > 0 ? tiers : FALLBACK_TIERS;

  const [saving, setSaving] = useState(false);
  const [saveErr, setSaveErr] = useState<string | null>(null);

  const [askOncePlan, setAskOncePlan] = useState<AskOnceDialogPlan | null>(null);
  const [admitting, setAdmitting] = useState(false);
  const [admitErr, setAdmitErr] = useState<string | null>(null);
  const [admitSummary, setAdmitSummary] = useState<AdmitOutcomeSummary | null>(null);
  // FIX (code review on PR #1041): the flip used to be the ONLY way to open
  // the "ask once" dialog. "Not now", a failed `queuedPreview` read, or
  // simply navigating away before it rendered all left the backlog
  // permanently unreachable — reloading with auto-add already on called
  // `queuedPreview` nowhere at all. `checkingQueue`/`queueCheckErr` back the
  // standing "Review who's queued" control below (rendered whenever
  // auto-add is on), which re-runs the exact same check on demand, as many
  // times as needed, instead of the one-shot, silent-on-failure version the
  // flip alone used to run.
  const [checkingQueue, setCheckingQueue] = useState(false);
  const [queueCheckErr, setQueueCheckErr] = useState<string | null>(null);

  function refetch() {
    void admin.refresh();
  }

  const checkAskOnce = useCallback(async (token: string) => {
    setCheckingQueue(true);
    setQueueCheckErr(null);
    try {
      const preview = await api.orgs.autoAdd.queuedPreview(token);
      const plan = planAskOnceDialog(preview);
      if (plan.kind !== "hidden") setAskOncePlan(plan);
    } catch (e) {
      // NOT silent (fixed — code review on PR #1041). The toggle's own save
      // already succeeded and this must not roll it back, so it still
      // leaves `settings` alone — but a failed read used to vanish with no
      // trace, which was one of the ways the backlog went unreachable. The
      // "Review who's queued" control below shows this error with a retry,
      // on demand, instead.
      setQueueCheckErr(e instanceof ApiError ? e.message : "Couldn't check the queued requests.");
    } finally {
      setCheckingQueue(false);
    }
  }, []);

  /** The standing way back into the dialog — see the `checkingQueue`/
   *  `queueCheckErr` state's own doc comment. Available whenever auto-add
   *  is on (mirrors exactly when the toggle's own tier `<Select>` shows),
   *  independent of whether the flip that turned it on ever ran this
   *  successfully. */
  function reviewQueue() {
    const token = getAuthToken();
    if (!token) return;
    void checkAskOnce(token);
  }

  async function saveSettings(next: AutoAddSettings, isFlipOn: boolean) {
    const token = getAuthToken();
    if (!token) return;
    setSaving(true);
    setSaveErr(null);
    try {
      const updated = await api.orgs.autoAdd.put(token, next);
      setSettings(updated);
      if (isFlipOn) void checkAskOnce(token);
    } catch (e) {
      setSaveErr(mapAutoAddSaveError(e));
    } finally {
      setSaving(false);
    }
  }

  function handleToggle(next: boolean) {
    if (!settings) return;
    const flippingOn = next && !settings.enabled;
    void saveSettings({ enabled: next, tier: settings.tier }, flippingOn);
  }

  function handleTierChange(tier: string) {
    if (!settings) return;
    void saveSettings({ enabled: settings.enabled, tier }, false);
  }

  async function confirmAskOnce() {
    if (!askOncePlan) return;
    // FIX (code review on PR #1041): every passer shown is sent now, not
    // just the promise-sized slice — see `askOnceRequestIds`'s own doc
    // comment for why the earlier, narrower send broke the drain entirely.
    // `maxAdmit` is the actual promise (the button's own words), so it —
    // not the full `request_ids.length` — is what `summarizeAdmitOutcome`
    // measures the outcome against.
    const requestIds = askOnceRequestIds(askOncePlan);
    const maxAdmit = askOnceMaxAdmit(askOncePlan);
    if (!requestIds || maxAdmit === null) return;
    const token = getAuthToken();
    if (!token) return;
    setAdmitting(true);
    setAdmitErr(null);
    try {
      const result = await api.orgs.autoAdd.admitQueued(token, requestIds, maxAdmit);
      setAdmitSummary(summarizeAdmitOutcome(maxAdmit, result));
      setAskOncePlan(null);
      // An admit is exactly what an approval is — a new membership and a
      // spent slot — so both refresh signals that follow one fire here too.
      refreshJoinRequestCount(qc);
      onAdmitted?.();
    } catch (e) {
      setAdmitErr(mapAdmitError(e));
    } finally {
      setAdmitting(false);
    }
  }

  // Computed unconditionally (rather than inside `renderRow` alone) so the
  // standing "Review who's queued" control below can gate on the exact same
  // "armed" reading the toggle itself uses, without a second `domains` scan
  // that could drift from it.
  const domainState = autoAddDomainState(domains);

  // A plain if-chain rather than a nested ternary: TypeScript can narrow
  // `settings`/`domains` out of `null` through an early `return` in a way it
  // cannot through an intermediate `loading` boolean, and the row the
  // armed+loaded case renders is the one that actually reads `settings.*`.
  function renderRow() {
    if (settingsErr) {
      return (
        <SettingRow title="Auto-add" desc={settingsErr}>
          <Button variant="secondary" size="sm" onClick={refetch}>
            Try again
          </Button>
        </SettingRow>
      );
    }
    if (domainsErr) {
      return (
        <SettingRow title="Auto-add" desc={domainsErr}>
          <Button variant="secondary" size="sm" onClick={onRetryDomains}>
            Try again
          </Button>
        </SettingRow>
      );
    }
    if (settings === null || domains === null) {
      return <SettingRow title="Auto-add" desc="Checking this organization's verified domains…" />;
    }
    if (!domainState.armed) {
      // CS-450: this used to be a dead end (the copy named the missing
      // state with nowhere to send the admin — CS-443 shipped the backend
      // with zero web UI). `#domains` is `DomainVerification`'s own section
      // id, rendered directly above this component on the same page — see
      // that component's own `id="domains"` and the test in
      // `AutoAddSetting.test.tsx` covering the two stay in sync.
      return (
        <SettingRow title="Auto-add" desc={autoAddGateCopy(domainState)}>
          <a
            href="#domains"
            // item 9, code review on PR #1048: matches its house twin
            // (`members/page.tsx`'s "Resend it" link) — the single control
            // this whole escape hatch depends on had dropped the
            // `focus-visible` ring every other inline link in the file
            // carries.
            className="whitespace-nowrap rounded-control text-[12.5px] text-brand-2 transition-colors hover:text-brand hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand/40"
          >
            Manage domains <span className="font-mono text-[10px] text-ink-4">→</span>
          </a>
        </SettingRow>
      );
    }
    return (
      <SettingRow title="Auto-add" desc={autoAddGateCopy(domainState)}>
        <div className="flex flex-col items-end gap-[8px]">
          <Toggle checked={settings.enabled} onChange={handleToggle} disabled={saving} ariaLabel="Auto-add" />
          {settings.enabled && (
            <Select
              // FIX (code review on PR #1041): `settings.tier` is a SERVER
              // value — a previously saved setting — so unlike
              // `JoinRequestQueue.tsx`'s own per-row default it can name a
              // tier `tierOptions` genuinely doesn't contain (e.g. since
              // deactivated). Passed straight through, the visible
              // selection and the saved tier would silently disagree; see
              // `seatTierOptions.ts`'s own doc comment for the guard.
              value={resolveTierCode(tierOptions, settings.tier)}
              onChange={(e) => handleTierChange(e.target.value)}
              disabled={saving}
              wrapperClassName="w-auto"
              className="w-auto"
              aria-label="Auto-add slot tier"
            >
              {tierOptions.map((t) => (
                <option key={t.code} value={t.code}>
                  {tierOptionLabel(t)}
                </option>
              ))}
            </Select>
          )}
        </div>
      </SettingRow>
    );
  }

  const dialogCopy = askOnceCopy(askOncePlan ?? { kind: "hidden" });
  // FIX (code review on PR #1041): the "— ." bug. `admit_queued` drops an
  // id from BOTH `admitted` and `not_admitted` when it no longer names an
  // open request for this account (already decided by hand since the
  // preview, or simply gone) — so `shortfall` can be positive while
  // `admitOutcomeReasonClause` is empty. Computed once and only rendered
  // when non-empty, rather than trusting `shortfall > 0` alone to mean
  // there is a clause to show after the em dash.
  const admitReasonClause = admitSummary ? admitOutcomeReasonClause(admitSummary) : "";

  return (
    <>
      <SettingList className="mb-[16px]">{renderRow()}</SettingList>

      {saveErr && (
        <div className="mb-[16px] -mt-[8px]">
          <ErrorNote>{saveErr}</ErrorNote>
        </div>
      )}

      {/* FIX (code review on PR #1041): the backlog is otherwise reachable
          exactly once — the moment the toggle flips on. "Not now", a failed
          `queuedPreview` read, or just coming back later all left no way
          back in. This standing control re-runs the same check on demand,
          any time auto-add is on. */}
      {settings?.enabled &&
        domainState.armed &&
        (queueCheckErr ? (
          <div className="mb-[16px] -mt-[8px]">
            <ErrorNote
              action={
                <Button variant="secondary" size="sm" onClick={reviewQueue} disabled={checkingQueue}>
                  Try again
                </Button>
              }
            >
              {queueCheckErr}
            </ErrorNote>
          </div>
        ) : (
          <div className="mb-[16px] -mt-[8px]">
            <Button variant="ghost" size="sm" onClick={reviewQueue} disabled={checkingQueue}>
              {checkingQueue ? "Checking…" : "Review who's queued"}
            </Button>
          </div>
        ))}

      {admitSummary && (
        <div className="mb-[16px] -mt-[8px]">
          <Notice tone={admitSummary.shortfall > 0 ? "warn" : "ok"}>
            {admitSummary.headline}
            {admitSummary.shortfall > 0 && admitReasonClause && ` — ${admitReasonClause}.`}
          </Notice>
        </div>
      )}

      {admitErr && (
        <div className="mb-[16px] -mt-[8px]">
          <ErrorNote>{admitErr}</ErrorNote>
        </div>
      )}

      {/* D11 "ask once" — every queued person who would pass, listed (not
          just counted), admitted oldest-first in one click. Hidden entirely
          when nothing is queued: flipping auto-add on with an empty queue
          just turns it on. Opened by a successful flip-on, OR by the
          standing "Review who's queued" control above — see that
          control's own doc comment for why the flip alone wasn't enough. */}
      <Modal
        open={askOncePlan !== null}
        onClose={() => setAskOncePlan(null)}
        title="Auto-add is on."
        description={
          dialogCopy && (
            <>
              {/* Two `span`s, not a text node + `<br/>` — so each line is
                  its own element a screen reader (and a test) can address,
                  rather than one run-together text blob. */}
              <span className="block">{dialogCopy.headline}</span>
              {dialogCopy.subline && <span className="mt-[2px] block">{dialogCopy.subline}</span>}
            </>
          )
        }
        footer={
          <>
            <Button variant="secondary" onClick={() => setAskOncePlan(null)} disabled={admitting}>
              {dialogCopy?.dismissLabel ?? "Not now"}
            </Button>
            {dialogCopy?.admitLabel && (
              <Button variant="brand" onClick={() => void confirmAskOnce()} disabled={admitting}>
                {/* "none-fit"'s own click admits nobody — "Admitting…" would
                    describe something this particular click never does. */}
                {admitting ? (askOncePlan?.kind === "none-fit" ? "Queuing…" : "Admitting…") : dialogCopy.admitLabel}
              </Button>
            )}
          </>
        }
      >
        {askOncePlan && askOncePlan.kind !== "hidden" && (
          <div className="flex flex-col gap-[8px]">
            {askOncePlan.passers.map((p) => (
              <div key={p.request_id} className="flex items-center gap-[10px]">
                <Avatar name={p.name ?? p.email} hue={hueForName(p.name ?? p.email)} size={22} />
                <div className="min-w-0 flex-1">
                  <span className="text-[13px] text-ink">{p.name ?? p.email}</span>
                  {p.name && (
                    <span className="ml-[6px] text-[11.5px] text-ink-3">
                      <Mono>{p.email}</Mono>
                    </span>
                  )}
                </div>
                <Mono className="text-ink-4">{timeAgo(p.requested_at)}</Mono>
              </div>
            ))}
            {dialogCopy?.footnote && (
              <p className="mt-[6px] text-[12px] leading-[1.5] text-ink-3">{dialogCopy.footnote}</p>
            )}
          </div>
        )}
      </Modal>
    </>
  );
}
