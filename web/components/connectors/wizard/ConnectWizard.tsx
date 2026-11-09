"use client";
import { connectorCallbackDeadline } from "@/lib/connectorActionPolicy";
import { refreshPageSection } from "@/lib/pageSectionRefresh";
import type { ConnectorDetail } from "@/lib/connectorsApi";
import { ApiError } from "@/lib/api";
import Link from "next/link";
import { useCallback, useEffect, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { invalidate, invalidationFor } from "@/lib/appQueries";
import { useApp } from "@/lib/providers";
import { connectorsApi, type ConnectorSummary, type ScopeSelection } from "@/lib/connectorsApi";
import { Button, Checkbox, Modal, OptionTile, StepRail } from "@/components/ui";
import { ScopePicker } from "../ScopePicker";

const steps = ["Connect", "Choose sources", "Choose scope", "Import history", "Done"];
export function historyReason(reason: string) {
  return ({ starts_at_connect: "Captures from the moment you connect.", per_meeting: "Meeting capture is chosen per meeting.", automatic_window: "History is captured automatically for this source.", followed_threads_only: "Only followed threads are captured." } as Record<string, string>)[reason] ?? "This source does not support importing history.";
}
export function ConnectWizard({ connector, onClose, returnTo = "wizard", reconnect = false }: { connector: ConnectorSummary; onClose: () => void; reconnect?: boolean; returnTo?: "wizard" | "onboarding" }) {
  const { getAuthToken, queryScope } = useApp();
  const client = useQueryClient();
  const [step, setStep] = useState(reconnect || connector.status === "not_connected" || connector.status === "reconnect_needed" ? 0 : connector.resume_step === "history" ? 3 : connector.resume_step === "scope" ? 2 : 1);
  const [sources, setSources] = useState(connector.sources);
  const [index, setIndex] = useState(0);
  const [selection, setSelection] = useState<ScopeSelection>({ mode: connector.sources.find(s => s.enabled)?.scope?.mode ?? "pick", item_ids: [] });
  const drafts = useRef<Record<string, ScopeSelection>>({});
  const [ready, setReady] = useState(false);
  const [hosted, setHosted] = useState<string | null>(() => typeof window === "undefined" ? null : sessionStorage.getItem(`connector-pending:${connector.key}`));
  const [hostedUrl, setHostedUrl] = useState<string | null>(() => typeof window === "undefined" ? null : sessionStorage.getItem(`connector-url:${connector.key}`));
  const [expired, setExpired] = useState(false);
  const [busy, setBusy] = useState(false);
  const [automaticAttempt, setAutomaticAttempt] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [days, setDays] = useState<Record<string, 0 | 7 | 30 | 90>>({});
  const running = useRef(false);
  const explicitStep = useRef(false);
  const completed = sources.filter(s => s.history_requested_at != null).map(s => s.key);
  // Durable claims acknowledge requests, not completed ingestion.
  const reconcile = useCallback((saved: ConnectorSummary) => {
    setSources(prev => prev.map(s => {
      const authoritative = saved.sources.find(v => v.key === s.key);
      return authoritative ? { ...s, history_days: authoritative.history_days, history_requested_at: authoritative.history_requested_at, setup_completed_at: authoritative.setup_completed_at } : s;
    }));
  }, []);
  useEffect(() => { reconcile(connector); }, [connector, reconcile]);
  const authRef = useRef(getAuthToken);
  authRef.current = getAuthToken;
  const single = sources.length === 1;
  const toolkit = single && sources[0].scope_shape === "everything_only" && ["jira", "linear", "confluence", "hubspot", "zendesk", "dart"].includes(connector.key);
  const visibleSteps = steps.filter((_, i) => !(single && i === 1) && !(toolkit && i === 2));
  useEffect(() => {
    if (step !== 1 || !single) return;
    let active = true;
    setBusy(true); setError(null);
    connectorsApi.patchSource(authRef.current() ?? "", connector.key, sources[0].key, { enabled: true }).then(saved => {
      if (!active) return;
      setSources([saved]); setSelection({ mode: saved.scope?.mode ?? "pick", item_ids: [] }); setReady(false); setStep(2);
    }).catch(() => { if (active) setError("Couldn’t enable this source. Please try again."); }).finally(() => { if (active) setBusy(false); });
    return () => { active = false; };
  // Sources are updated only after the durable enable succeeds.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [step, single, connector.key, automaticAttempt]);
  useEffect(() => {
    if (step !== 2 || !toolkit) return;
    let active = true;
    setBusy(true); setError(null);
    connectorsApi.setScope(authRef.current() ?? "", connector.key, sources[0].key, { mode: "everything" }).then(saved => {
      if (!active) return;
      setSources([saved]); setStep(saved.history.supported ? 3 : 4);
    }).catch(() => { if (active) setError("Couldn’t save workspace scope. Please try again."); }).finally(() => { if (active) setBusy(false); });
    return () => { active = false; };
  // Persist workspace-wide scope before skipping the redundant choice.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [step, toolkit, connector.key, automaticAttempt]);
  const enabled = sources.filter(s => s.enabled);
  const source = enabled[index];
  const refresh = () => invalidate(client, invalidationFor.connectorChanged());
  const close = () => { if (!busy) { void refresh(); onClose(); } };
  async function advance() {
    if (running.current) return;
    running.current = true;
    explicitStep.current = true;
    setBusy(true); setError(null);
    try {
      const token = getAuthToken();
      if (!token) throw new Error("auth");
      if (step === 0 && hosted) {
        const expires = sessionStorage.getItem(`connector-expiry:${connector.key}`);
        if (expires && Date.parse(expires) <= Date.now()) { setExpired(true); return; }
        const connected = await connectorsApi.complete(token, connector.key, hosted);
        sessionStorage.removeItem(`connector-pending:${connector.key}`);
        setHosted(null); setSources(connected.sources); setStep(1); await refresh(); return;
      }
      if (step === 0) {
        const result = await connectorsApi.connect(token, connector.key, returnTo);
        sessionStorage.setItem(`connector-return:${connector.key}`, returnTo);
        const url = new URL(result.authorize_url);
        if (!["https:", "http:"].includes(url.protocol)) throw new Error("url");
        if (result.pending_id) {
          sessionStorage.setItem(`connector-pending:${connector.key}`, result.pending_id);
          setHosted(result.pending_id); setHostedUrl(url.href);
          sessionStorage.setItem(`connector-url:${connector.key}`, url.href);
          sessionStorage.setItem(`connector-expiry:${connector.key}`, result.expires_at);
          window.open(url.href, "_blank", "noopener,noreferrer");
          return;
        }
        window.location.assign(url.href); return;
      }
      if (step === 1) {
        for (const s of sources) await connectorsApi.patchSource(token, connector.key, s.key, { enabled: s.enabled });
        setSelection(drafts.current[enabled[0]?.key] ?? { mode: enabled[0]?.scope?.mode ?? "pick", item_ids: [] }); setReady(false); setIndex(0); setStep(2);
      } else if (step === 2 && source) {
        const updated = await connectorsApi.setScope(token, connector.key, source.key, source.scope_shape === "everything_only" ? { mode: "everything" } : selection);
        setSources(prev => prev.map(s => s.key === updated.key ? updated : s));
        if (index + 1 < enabled.length) { setSelection(drafts.current[enabled[index + 1].key] ?? { mode: enabled[index + 1].scope?.mode ?? "pick", item_ids: [] }); setReady(false); setIndex(index + 1); }
        else if (enabled.some(s => s.history.supported)) setStep(3);
        else { await refresh(); setStep(4); }
      } else if (step === 3) {
        const saved = await refreshPageSection<ConnectorDetail>(token, "connector_detail", { provider: connector.key });
        reconcile(saved);
        // A stale link or older client may resume at history before confirming
        // an unsupported source. Return to scope; do not claim Done without its
        // durable completion or silently replace an existing selection.
        if (saved.sources.some(s => s.enabled && !s.history.supported && !s.setup_completed_at)) {
          const first = saved.sources.find(s => s.enabled);
          setSources(saved.sources); setIndex(0);
          setSelection({ mode: first?.scope?.mode ?? "pick", item_ids: [] });
          setReady(false); setStep(2); return;
        }
        for (const s of enabled.filter(s => s.history.supported)) {
          const current = saved.sources.find(v => v.key === s.key);
          if (!current) throw new Error("missing source");
          if (current.history_requested_at != null) continue;
          try {
            const accepted = await connectorsApi.backfill(token, connector.key, s.key, (current.history_days ?? days[s.key] ?? 0) as 0 | 7 | 30 | 90);
            reconcile({ ...saved, sources: [accepted] });
          } catch (failure) {
            // A lost response (or provider start error) may follow a durable claim.
            const recovered = await refreshPageSection<ConnectorDetail>(token, "connector_detail", { provider: connector.key });
            reconcile(recovered);
            if (!recovered.sources.find(v => v.key === s.key)?.history_requested_at) throw failure;
          }
        }
        await refresh(); setStep(4);
      }
      await refresh();
    } catch (failure) { if (step === 0 && failure instanceof ApiError && failure.code === "sign_in_expired") setExpired(true); else setError("Couldn’t save this step. Your saved choices are kept. Please try again."); }
    finally { running.current = false; setBusy(false); }
  }
  return <Modal open onClose={close} display title={step === 0 ? `Connect ${connector.name}` : step === 2 ? `Choose scope: ${source?.label ?? "sources"}` : steps[step]} header={<StepRail compact steps={visibleSteps} currentIndex={Math.max(0, visibleSteps.indexOf(steps[step]))} />} footer={<>
    {step > (toolkit ? 3 : single ? 2 : 1) && step < 4 && <Button disabled={busy || completed.length > 0} onClick={() => {
      if (step === 2 && index > 0) { setIndex(index - 1); setSelection(drafts.current[enabled[index - 1].key] ?? { mode: enabled[index - 1]?.scope?.mode ?? "pick", item_ids: [] }); setReady(false); return; }
      if (step === 3) { setIndex(0); setSelection(drafts.current[enabled[0]?.key] ?? { mode: enabled[0]?.scope?.mode ?? "pick", item_ids: [] }); setReady(false); }
      setStep(step - 1);
    }}>Back</Button>}
    <Button disabled={busy} onClick={close}>Close</Button>
    {step < 4 && !expired && !(single && step === 1) && !(toolkit && step === 2) && <Button disabled={busy || (step === 1 && !enabled.length) || (step === 2 && !ready)} onClick={() => void advance()}>{busy ? "Saving…" : step === 0 ? hosted ? "I’ve finished signing in" : `Sign in to ${connector.name}` : step === 3 ? "Finish" : "Continue"}</Button>}
    {step === 4 && <Link href={`/connectors/${connector.key}`}>View {connector.name}</Link>}
  </>}>
    {error && <p role="alert" className="mb-4 text-bad">{error}{((single && step === 1) || (toolkit && step === 2)) && <Button disabled={busy} onClick={() => setAutomaticAttempt(v => v + 1)}>Try again</Button>}</p>}
    {step === 0 && <p>Sign in to choose what CoralSwarm captures. If a sign-in tab opens, return here when you finish.</p>}
    {expired && <p role="alert">Sign-in expired. Start a new sign-in to try again.</p>}
    {step === 4 && <div className="space-y-4"><p>Setup saved. History imports continue in the background.</p>{enabled.map(s => <div key={s.key}><strong>{s.label}</strong><p>{s.scope?.mode === "everything" ? "Everything I can see" : `${s.scope?.count ?? 0} ${s.scope?.noun ?? "items"} selected`}</p><p>{s.history.supported ? s.history_days ? `${s.history_days} days requested` : "No history requested" : historyReason(s.history.reason)}</p></div>)}</div>}
    {step === 0 && hosted && <div className="mt-4 space-y-3">{hostedUrl && <a className="underline" href={hostedUrl} target="_blank" rel="noopener noreferrer">Open sign-in again</a>}<Button disabled={busy} onClick={() => { sessionStorage.removeItem(`connector-pending:${connector.key}`); setHosted(null); setHostedUrl(null); setExpired(false); sessionStorage.removeItem(`connector-url:${connector.key}`); sessionStorage.removeItem(`connector-expiry:${connector.key}`); }}>Start a new sign-in</Button></div>}
    {step === 1 && single && busy && <p role="status">Enabling source…</p>}
    {step === 1 && !single && <div className="space-y-4">{sources.map(s => <div key={s.key}><Checkbox disabled={busy} label={s.label} checked={s.enabled} onChange={enabled => setSources(prev => prev.map(v => v.key === s.key ? { ...v, enabled } : v))} /></div>)}</div>}
    {step === 2 && source && !toolkit && <ScopePicker actionDeadline={explicitStep.current ? undefined : connectorCallbackDeadline(client, queryScope, connector.key)} key={source.key} getAuthToken={getAuthToken} provider={connector.key} source={source} value={source.scope_shape === "everything_only" ? { mode: "everything" } : selection} preserveSelection={Boolean(drafts.current[source.key])} onReady={setReady} onChange={value => { drafts.current[source.key] = value; setSelection(value); }} />}
    {step >= 2 && enabled.filter(s => !s.history.supported).map(s => <p className="mt-4 text-sm text-ink-3" key={s.key}>{s.label}: {!s.history.supported && historyReason(s.history.reason)}</p>)}
    {step === 3 && enabled.filter(s => s.history.supported).map(s => <fieldset className="mb-4" key={s.key}><legend>{s.label}</legend><div className="grid grid-cols-2 gap-2">{([7, 30, 90, 0] as const).map(d => <OptionTile key={d} name={s.key} value={String(d)} title={d === 0 ? "None" : d === 90 ? "Up to 90 days" : `Last ${d} days`} checked={(s.history_days ?? days[s.key] ?? 0) === d} disabled={busy || completed.includes(s.key) || (s.history.supported && d > s.history.max_days)} onChange={() => setDays(prev => ({ ...prev, [s.key]: d }))} />)}</div></fieldset>)}
  </Modal>;
}
