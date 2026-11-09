"use client";

/* 2j Add sources — paste, upload a transcript, or connect Slack. Zoom,
   Teams, Gmail, Meet, and Slack meeting ingest also arrive through
   Connectors. Everything is user-initiated; nothing enters a workspace
   without an explicit action. Native on-device recording is gone. */

import { useState, type ChangeEvent } from "react";
import Link from "next/link";
import { useQueryClient } from "@tanstack/react-query";
import { useApp } from "@/lib/providers";
import { useEngine } from "@/lib/engine/provider";
import { api, ApiError } from "@/lib/api";
import { invalidate, invalidationFor } from "@/lib/appQueries";
import { PageHead, ErrorNote, Card, Input, Textarea, Button, Dot, Mono } from "@/components/ui";
import { AddRecentActivity } from "@/components/library/AddRecentActivity";

type Result = { atom_ids: string[]; jobs_enqueued: number; chunks_created: number };

const SOURCES = [
  { id: "paste", nm: "Paste text", d: "notes, docs, decisions" },
  { id: "transcript", nm: "Add meeting transcript", d: "paste or upload a transcript" },
  { id: "slack", nm: "Connect Slack", d: "your public channel history" },
] as const;

export default function AddPage() {
  const { token, currentGraph, graphs } = useApp();
  const qc = useQueryClient();
  // Ingest only lands in the caller's own ocean (`routes/ingest.rs`
  // `is_own_writable_ocean`), but the open ocean in an org is the org's, so
  // pasting there 400s. On the org plane the only member ocean listed is the
  // caller's own.
  const destination =
    currentGraph && (currentGraph.kind === "personal" || currentGraph.kind === "member")
      ? currentGraph
      : (graphs.find((g) => g.kind === "member") ?? graphs.find((g) => g.kind === "personal") ?? currentGraph);
  const eng = useEngine();
  const [source, setSource] = useState<"paste" | "transcript">("paste");
  const [text, setText] = useState("");
  const [label, setLabel] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [result, setResult] = useState<Result | null>(null);

  // Both paste and transcript modes use the same text box; transcript ingests
  // with source_type "transcript" so the backend chunks it by speaker turns.

  async function ingest() {
    if (!token || !destination || !text.trim()) return;
    const content = text.trim();
    const defaultLabel = source === "transcript" ? "Meeting transcript" : "Pasted note";
    const sourceId = label.trim() || `${defaultLabel} · ${new Date().toLocaleDateString()}`;
    setBusy(true);
    setErr(null);
    setResult(null);
    try {
      const r = await api.corals.ingest(token, {
        graphId: destination.id,
        content,
        sourceType: source === "transcript" ? "transcript" : undefined,
        sourceId,
      });
      setResult(r);
      // The "added recently" rail below (and the workspace, Today and stats
      // reads) are cached; refetch them so the new coral shows up here.
      void invalidate(qc, invalidationFor.ingest(destination.id));
      if (eng?.status === "ready" && eng.engine) {
        eng.engine
          .ingest([{ id: r.atom_ids[0] ?? crypto.randomUUID(), sourceType: source === "transcript" ? "transcript" : "note", sourceId, content }])
          .catch(() => {});
      }
      if (r.chunks_created > 0) setText("");
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "Ingest failed");
    } finally {
      setBusy(false);
    }
  }

  async function loadFile(e: ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;
    setErr(null);
    try {
      setText(await file.text());
      if (!label.trim()) setLabel(file.name.replace(/\.[^.]+$/, ""));
    } catch {
      setErr("Could not read that file.");
    }
  }

  const pipeline = [
    { label: "chunk" },
    { label: "embed 384-d" },
    { label: "encrypt" },
    { label: "grow topics" },
  ];

  return (
    <>
      <PageHead title="Add sources">
        Paste a note or transcript, or connect Slack. Zoom, Teams, Gmail, and Meet land via Connectors.
      </PageHead>

      <div className="grid grid-cols-1 items-start gap-[24px] xl:grid-cols-[minmax(0,1fr)_320px]">
        <div className="max-w-[680px]">
          <div className="mb-[20px] grid grid-cols-3 gap-[14px]">
            {SOURCES.map((s) => {
              const on = source === s.id;
              const card = (
                <Card
                  className={`text-center transition-colors ${on ? "border-brand" : "hover:border-ink-3"}`}
                  style={on ? { background: "var(--brand-soft)" } : undefined}
                >
                  <div className="text-[13.5px] font-medium text-ink">{s.nm}</div>
                  <div className="mt-[3px] font-mono text-[11px] text-ink-3">{s.d}</div>
                </Card>
              );
              return s.id === "slack" ? (
                <Link key={s.id} href="/connectors">{card}</Link>
              ) : (
                <button key={s.id} type="button" onClick={() => setSource(s.id)}>
                  {card}
                </button>
              );
            })}
          </div>

          <div className="mb-[14px] flex items-center gap-[10px]">
            <span className="font-mono text-[9.5px] uppercase tracking-[0.13em] text-ink-3">destination</span>
            <span className="inline-flex items-center gap-[9px] rounded-field border border-line bg-bg px-[12px] py-[7px]">
              <Dot hue="teal" size={8} />
              <span className="text-[13px] font-medium text-ink">{destination?.name ?? "workspace"}</span>
              {destination && <Mono className="text-ink-3">{destination.kind}</Mono>}
            </span>
          </div>

          <Input
            wrapperClassName="mb-[12px]"
            className="font-mono text-[13px]"
            placeholder="source label (optional) — e.g. standup-2026-07-27"
            value={label}
            onChange={(e) => setLabel(e.target.value)}
          />
          {source === "transcript" && (
            <label className="mb-[12px] flex cursor-pointer flex-wrap items-center gap-[10px] font-mono text-[12px] text-ink-3">
              <input type="file" accept=".txt,.vtt,.srt,.md,text/plain" className="hidden" onChange={loadFile} />
              <span className="rounded-row border border-line bg-bg px-[13px] py-[7px] text-[13px] text-ink-2">Upload transcript file</span>
              <span>or paste below — speaker turns (&ldquo;Name: …&rdquo;) chunk best. .txt · .vtt · .srt · .md</span>
            </label>
          )}
          <Textarea
            className="min-h-[140px] resize-y"
            placeholder={source === "transcript" ? "Paste a meeting transcript…" : "Paste notes, a doc, or a decision…"}
            value={text}
            onChange={(e) => setText(e.target.value)}
          />

          <div className="mt-[16px] flex flex-wrap items-center gap-[7px] font-mono text-[11px] text-ink-3">
            {pipeline.map((p, i) => {
              const done = result && i < 3;
              const queued = result && i === 3 && result.jobs_enqueued > 0;
              return (
                <span key={p.label} className="contents">
                  <span
                    className="rounded-pill border px-[11px] py-[6px]"
                    style={
                      done
                        ? { borderColor: "var(--ok)", color: "var(--ok)", background: "color-mix(in srgb, var(--ok) 10%, var(--bg))" }
                        : queued
                          ? { borderColor: "var(--hue-amber)", color: "var(--hue-amber)", background: "color-mix(in srgb, var(--hue-amber) 10%, var(--bg))" }
                          : undefined
                    }
                  >
                    {queued ? `${p.label} (queued)` : p.label}
                  </span>
                  {i < pipeline.length - 1 && <span>→</span>}
                </span>
              );
            })}
          </div>

          {err && (
            <div className="mt-[16px]">
              <ErrorNote>{err}</ErrorNote>
            </div>
          )}

          {result && (
            <div
              className="mt-[16px] rounded-card border px-[16px] py-[12px] text-[13px] text-ink [&_b]:font-medium"
              style={{ borderColor: "var(--ok)", background: "color-mix(in srgb, var(--ok) 8%, var(--bg))" }}
            >
              {result.chunks_created > 0 ? (
                <>
                  Spawned <b>{result.chunks_created} item{result.chunks_created === 1 ? "" : "s"}</b> · {result.jobs_enqueued} extraction job(s) queued.{" "}
                  <Link href="/today" className="text-brand-2 underline">
                    See what&apos;s flowing →
                  </Link>
                </>
              ) : (
                <>No items spawned — the content didn&apos;t chunk. Try longer prose.</>
              )}
            </div>
          )}

          <Button variant="brand" size="lg" className="mt-[20px]" disabled={busy || !text.trim()} onClick={() => void ingest()}>
            {busy ? "Releasing…" : `Release into ${destination?.name ?? "workspace"} ▸`}
          </Button>
        </div>

        <div className="rounded-card border border-line-2 bg-bg-subtle-2 p-[16px]">
          <AddRecentActivity />
        </div>
      </div>
    </>
  );
}
