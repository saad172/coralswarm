"use client";
import { apiRequest } from "@/lib/api";
import { usePageSection } from "@/lib/pageSections";
import type { ConnectorDetail as ConnectorDetailPayload } from "@/lib/connectorsApi";
import type { ConnectorSummary } from "@/lib/connectorsApi";
import Link from "next/link";
import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { connectorQueryOptions, invalidate, invalidationFor } from "@/lib/appQueries";
import { connectorsApi } from "@/lib/connectorsApi";
import { Button, Modal, Verdict, Notice } from "@/components/ui";
import { ProviderMark } from "@/components/settings/ProviderIcon";
import { STATUS_VIEW, problemLine } from "./statusModel";
import { ConnectorSourceCard } from "./ConnectorSourceCard";
import { CaptureFeed } from "./CaptureFeed";
import { TopicStrip } from "./TopicStrip";
import { ConnectWizard } from "./wizard/ConnectWizard";
export function ConnectorDetail({ provider }: { provider: string }) {
 const { getAuthToken, queryScope, authReady } = useApp(); const client = useQueryClient();
 const q = usePageSection<ConnectorDetailPayload>("connector_detail", { provider }, authReady, connectorQueryOptions({ getAuthToken, scope: queryScope }, provider).queryKey);
 const [recovering, setRecovering] = useState(false); const [recoveryMessage, setRecoveryMessage] = useState("");
 async function recover() { setRecovering(true); setRecoveryMessage(""); try { const token = getAuthToken(); if (!token) throw new Error("auth"); const result = await apiRequest<ConnectorSummary>(`/v1/connectors/${provider}/recover`, token, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ problem: "capture_unavailable" }) }); /* A repair changes the connector list and Home's attention row. Mark them stale only: `q.refresh` re-reads this detail, so an active refetch here would read it twice. */ await invalidate(client, invalidationFor.connectorChanged(), { refetchType: "none" }); await q.refresh(); setRecoveryMessage(result.problem ? "Capture is still unavailable. The recovery check did not confirm a repair." : "Recovery check completed."); } catch { setRecoveryMessage("Couldn’t complete recovery. Please try again."); } finally { setRecovering(false); } }
 const [confirm, setConfirm] = useState(false); const [busy, setBusy] = useState(false); const [error, setError] = useState(false); const [wizard, setWizard] = useState(false);
 async function disconnect() { setBusy(true); setError(false); try { const t = getAuthToken(); if (!t) throw new Error("auth"); await connectorsApi.disconnect(t, provider); await invalidate(client, invalidationFor.connectorChanged()); setConfirm(false); } catch { setError(true); } finally { setBusy(false); } }
 if ((q.isPending || (!q.error && q.data === undefined))) return <p role="status">Loading connector…</p>;
 if (q.error || !q.data) return <p role="alert">Couldn’t load connector. <Button onClick={() => void q.refresh()}>Retry</Button></p>;
 const c = q.data; const reconnect = c.status === "reconnect_needed" || ["missing_scopes", "app_not_installed", "credential_invalid", "sign_in_expired", "access_revoked"].includes(c.problem?.code ?? ""); const status = STATUS_VIEW[c.status];
 return <div className="space-y-6"><Link href="/connectors">← Connectors</Link><header className="flex flex-wrap items-center justify-between gap-4"><div className="flex min-w-0 items-center gap-3"><ProviderMark provider={provider} letter={c.name[0]} box={44} /><div><h1 className="font-display text-3xl">{c.name}</h1><p className="text-ink-3">{c.account_label}{c.connected_at && ` · Connected ${new Date(c.connected_at).toLocaleDateString()}`}</p></div></div><Verdict tone={status.tone} glyph={status.glyph}>{status.label}</Verdict></header>
 {c.problem && <Notice tone="warn" action={reconnect ? <Button onClick={() => setWizard(true)}>Reconnect</Button> : c.problem.code === "capture_unavailable" && ["jira", "linear", "confluence", "hubspot", "zendesk", "dart"].includes(provider) ? <Button disabled={recovering} onClick={() => void recover()}>{recovering ? "Checking capture…" : "Retry capture check"}</Button> : <Button onClick={() => void q.refresh()}>Check again</Button>}>{problemLine(c)}</Notice>}
 {recoveryMessage && <p role="status">{recoveryMessage}</p>}
 {!c.problem && status.action.kind !== "manage" && <Button onClick={() => setWizard(true)}>{status.action.label}</Button>}
 {c.status !== "not_connected" && <><TopicStrip topics={c.topics} /><div className="grid items-start gap-5 lg:grid-cols-[340px_minmax(0,1fr)]"><div className="space-y-4">{c.sources.map(s => <ConnectorSourceCard key={`${queryScope}:${provider}:${s.key}`} provider={provider} source={s} detail={c} computedAt={q.computedAt?.toISOString() ?? ""} />)}</div><CaptureFeed key={`${queryScope}:${provider}`} provider={provider} sources={c.sources} initialPage={c.captures} computedAt={q.computedAt?.toISOString() ?? ""} /></div><Button variant="danger" onClick={() => setConfirm(true)}>Disconnect</Button></>}
 <Modal open={confirm} onClose={() => { if (!busy) setConfirm(false); }} title={`Stop capturing from ${c.name}?`}><p>What’s already captured stays in your ocean.</p>{error && <p role="alert">Couldn’t disconnect. Please try again.</p>}<div className="mt-4 flex gap-3"><Button disabled={busy} onClick={() => setConfirm(false)}>Cancel</Button><Button variant="danger" disabled={busy} onClick={() => void disconnect()}>Disconnect</Button></div></Modal>
 {wizard && <ConnectWizard reconnect={reconnect} connector={c} onClose={() => setWizard(false)} />}</div>;
}
