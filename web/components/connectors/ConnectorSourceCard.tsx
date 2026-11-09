"use client";
import { useState } from "react";
import Link from "next/link";
import { useQueryClient, useQuery } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { apiRequest } from "@/lib/api";
import { invalidate, invalidationFor, oauthConnectionsQueryOptions } from "@/lib/appQueries";
import { connectorsApi, type ConnectorDetail, type SourceSummary, type ScopeSelection } from "@/lib/connectorsApi";
import { Button, Card, Meter, Sparkline, Verdict } from "@/components/ui";
import { sourceStatusView } from "./statusModel";
import { ScopePicker } from "./ScopePicker";
import { FollowedThreads } from "./FollowedThreads";
import { GmailAddonTokens } from "./GmailAddonTokens";
import { historyReason } from "./wizard/ConnectWizard";
export function ConnectorSourceCard({ provider, source, detail, computedAt }: { provider: string; source: SourceSummary; detail?: ConnectorDetail; computedAt?: string }) {
 const { getAuthToken, queryScope, authReady } = useApp(); const client = useQueryClient();
 const [edit, setEdit] = useState(false); const [ready, setReady] = useState(false); const [busy, setBusy] = useState(false); const [error, setError] = useState(false);
 const [selection, setSelection] = useState<ScopeSelection>({ mode: source.scope?.mode ?? (source.scope_shape === "everything_only" ? "everything" : "pick") });
 const [meeting, setMeeting] = useState("");
 const [pickerVersion, setPickerVersion] = useState(0);
 const [meetingSaved, setMeetingSaved] = useState(false);
 const [days, setDays] = useState<0 | 7 | 30 | 90>(7);
 const status = sourceStatusView(source.status);
 const mail = /gmail|outlook|mail/.test(source.key);
 const connections = useQuery({ ...oauthConnectionsQueryOptions({ getAuthToken, scope: queryScope }), enabled: authReady && edit && mail });
 async function run(action: (token: string) => Promise<unknown>, close = false) { if (busy) return; setBusy(true); setError(false); try { const t = getAuthToken(); if (!t) throw new Error("auth"); await action(t); await Promise.all([invalidate(client, invalidationFor.connectorChanged()), client.invalidateQueries({ queryKey: ["connector-captures"] })]); if (close) setEdit(false); } catch { setError(true); } finally { setBusy(false); } }
 return <Card className="space-y-4"><div className="flex flex-wrap items-center justify-between gap-3"><h2 className="text-lg font-semibold">{source.label}</h2><Verdict tone={status.tone} glyph={status.glyph}>{status.label}</Verdict><Sparkline values={source.week} /></div>
 <p className="text-sm text-ink-3">{source.scope?.mode === "everything" ? "Everything I can see" : `${source.scope?.count ?? 0} ${source.scope?.noun ?? "items"} selected`} · {source.last_captured_at ? `Last captured ${new Date(source.last_captured_at).toLocaleString()}` : "Nothing captured yet"}</p>
 {source.backfill && <Meter value={source.backfill.percent ?? 0} indeterminate={source.backfill.percent === null} label="Importing history" />}
 <div className="flex flex-wrap gap-2"><Button disabled={busy} onClick={() => void run(t => connectorsApi.patchSource(t, provider, source.key, { enabled: !source.enabled }))}>{source.enabled ? "Turn off" : "Turn on"}</Button>{source.enabled && <Button disabled={busy} onClick={() => void run(t => connectorsApi.patchSource(t, provider, source.key, { paused: !source.paused }))}>{source.paused ? "Resume" : "Pause"}</Button>}<Button disabled={busy} aria-expanded={edit} onClick={() => { setReady(false); if (!edit) setSelection({ mode: source.scope?.mode ?? (source.scope_shape === "everything_only" ? "everything" : "pick") }); setEdit(!edit); }}> {edit ? "Close" : "Edit"}</Button></div>
 {error && <p role="alert">Couldn’t save this change. Please try again.</p>}
 {edit && <div className="space-y-4"><ScopePicker key={pickerVersion} getAuthToken={getAuthToken} provider={provider} source={source} value={selection} onChange={setSelection} onReady={setReady} /><Button disabled={busy || !ready} onClick={() => void run(t => connectorsApi.setScope(t, provider, source.key, selection), true)}>Save scope</Button>
 {source.history.supported ? <div><label>Import history <select value={days} onChange={e => setDays(Number(e.target.value) as typeof days)}>{[0,7,30,90].filter(d => d <= (source.history.supported ? source.history.max_days : 0)).map(d => <option key={d} value={d}>{d ? `${d} days` : "None"}</option>)}</select></label><Button disabled={busy || !source.enabled || source.paused || !!source.backfill} onClick={() => void run(t => connectorsApi.backfill(t, provider, source.key, days))}>Request history</Button></div> : <p>{historyReason(source.history.reason)}</p>}
 {provider === "google" && source.key === "meet" && <div className="space-y-2"><label>Meet link or meeting code <input className="w-full border border-line bg-bg p-2" value={meeting} onChange={e => { setMeeting(e.target.value); setMeetingSaved(false); }} /></label><p className="text-sm text-ink-3">Allow the next meeting only. Requires a transcript; unused approval expires after 30 days.</p><Button disabled={busy || !meeting.trim()} onClick={() => void run(async t => { await apiRequest("/v1/connectors/google/meet/captures", t, { method: "POST", body: JSON.stringify({ meeting_code: meeting.trim() }) }); setMeetingSaved(true); setReady(false); setPickerVersion(v => v + 1); })}>Allow meeting capture</Button>{meetingSaved && <p role="status">Meeting capture allowed. Waiting for an available transcript.</p>}</div>}
 {provider === "microsoft" && /meet/.test(source.key) && <Link href="/connectors/microsoft-teams/meeting-capture">Choose a meeting to capture</Link>}
 {mail && <><FollowedThreads connections={connections.data ?? []} mailboxProvider={provider === "google" ? "google" : "microsoft"} initialPage={detail?.watched} computedAt={computedAt} />{provider === "google" && <GmailAddonTokens initialTokens={detail?.addin_tokens.tokens} computedAt={computedAt} />}</>}
 </div>}</Card>;
}
