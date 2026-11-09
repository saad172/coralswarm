"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { type MeetingEvidencePassage } from "@/lib/api";
import { readP6Evidence, retryP6Evidence } from "@/lib/p6EvidenceApi";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { useLibraryRead, libraryReadFailure } from "@/lib/useLibraryRead";
import { useCaptureDetail } from "@/lib/useCaptureDetail";
import { meetingEvidenceHref, sourceEvidenceHref } from "@/lib/provenance";
import { scopedChatHref } from "@/lib/scopedChatScope";
import { SourceSnapshotMetadata } from "./SourceSnapshotMetadata";
import { Button, Empty, ErrorNote, Loading } from "@/components/ui";

const LINK_STYLE = "rounded-row text-brand-2 underline underline-offset-4 hover:text-ink focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand";

/** Never relocate evidence by searching for the first matching quote. Repeated
 * words and astral characters must resolve to the recorded occurrence. */
export function exactPassageParts(text: string, passage: MeetingEvidencePassage) {
  const chars = Array.from(text);
  const { start_char: start, end_char: end } = passage;
  if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || end <= start || end > chars.length) return null;
  const quote = chars.slice(start, end).join("");
  if (quote !== passage.quote) return null;
  return { before: chars.slice(0, start).join(""), quote, after: chars.slice(end).join("") };
}

export function PreservedTranscript({ text, passage, label = "Preserved transcript" }: { text: string; passage?: MeetingEvidencePassage; label?: string }) {
  const mark = useRef<HTMLElement>(null);
  const parts = passage ? exactPassageParts(text, passage) : null;
  const matched = parts !== null;
  useEffect(() => {
    if (matched) mark.current?.scrollIntoView?.({ block: "nearest", behavior: "auto" });
  }, [passage?.passage_id, text, matched]);

  return (
    <>
      {passage && !parts && <p role="status" className="mb-3 text-sm text-ink-2">This passage could not be matched to its recorded position. The saved text is shown without a highlight.</p>}
      <div tabIndex={0} aria-label={label} className="max-h-[36rem] overflow-auto whitespace-pre-wrap break-words rounded-row border border-line bg-bg-2 p-4 text-sm leading-7 text-ink selection:bg-brand-soft focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand">
        {parts ? <>{parts.before}<mark ref={mark} className="rounded-badge bg-brand-soft text-ink">{parts.quote}</mark>{parts.after}</> : text}
      </div>
    </>
  );
}

function capturedLabel(value: string) {
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "medium" }) : "Capture time unavailable";
}

const STATUS_COPY = {
  not_started: "Atoms have not been extracted from this version yet.",
  pending: "Atom extraction is queued. The preserved transcript is ready to read.",
  running: "Finding facts, decisions, actions, and observations in this version…",
  done: "Each atom is linked to the passage that supports it.",
  failed: "Atom extraction failed. The preserved transcript is still available.",
  blocked: "Atom extraction is paused by this workspace’s processing settings.",
} as const;

/** Independent of the mutable meeting summary and its correction overlay. */
type SourceEvidenceProps = { token: string | null; graphId: string } & (
  { sourceId: string; sourceItemId?: never } | { sourceItemId: string; sourceId?: never }
);

export function SourceEvidence({ token: _token, graphId, sourceId = "", sourceItemId }: SourceEvidenceProps) {
  const isMeeting = !sourceItemId;
  const href = (version: string, atom?: string, passage?: string) => sourceItemId ? sourceEvidenceHref(sourceItemId, graphId, version, atom, passage) : meetingEvidenceHref(sourceId, graphId, version, atom, passage);
  const params = useSearchParams();
  const router = useRouter();
  const versionId = params.get("version_id") || undefined;
  const atomId = params.get("semantic_atom_id");
  const passageId = params.get("passage_id");
  const admission = useEvidenceAdmission();
  const scopeKey = JSON.stringify([admission.key, graphId, sourceId, sourceItemId, versionId, atomId]);
  const [paging, setPaging] = useState<{ scopeKey: string; versionId?: string; before?: string; atomsAfter?: string } | null>(null);
  const activePaging = paging?.scopeKey === scopeKey ? paging : null;
  const initial = useCaptureDetail({ graphId, ...(sourceItemId ? { sourceItemId } : { sourceId }), versionId, atomId: atomId ?? undefined });
  const key = JSON.stringify([scopeKey, activePaging, initial.dataUpdatedAt]);
  const activeKey = useRef(key);
  activeKey.current = key;
  const [retryKey, setRetryKey] = useState<string | null>(null);
  const [failure, setFailure] = useState<{ key: string; text: string } | null>(null);
  const paged = useLibraryRead(key, admission.ready && !!activePaging && !!initial.data, (signal) => readP6Evidence(admission.getToken(), {
    graphId, ...(sourceItemId ? { sourceItemId } : { sourceId }),
    ...(activePaging?.versionId || versionId ? { versionId: activePaging?.versionId || versionId } : {}),
    ...(atomId ? { atomId } : {}), ...(activePaging?.before ? { before: activePaging.before } : {}),
    ...(activePaging?.atomsAfter ? { atomsAfter: activePaging.atomsAfter } : {}),
  }, signal));
  const activeRead = activePaging ? paged : null;
  const read = {
    data: initial.data ? activeRead ? activeRead.data : initial.data.evidence : undefined,
    error: initial.data === undefined ? (initial.error ? libraryReadFailure(initial.error) : undefined) : activeRead?.error,
    refreshError: initial.data !== undefined ? (initial.error ? libraryReadFailure(initial.error) : undefined) ?? activeRead?.refreshError : undefined,
    loading: initial.data === undefined && !initial.error || !!activeRead?.loading,
    refreshing: initial.isFetching || !!activeRead?.refreshing,
    refresh: () => { if (activeRead) activeRead.refresh(); else void initial.refetch(); },
  };
  const error = failure?.key === key ? failure.text : read.error;
  const page = !error ? read.data : undefined;
  const version = page?.selected_version;
  const olderKey = read.loading ? key : null;
  const moreAtomsKey = read.loading ? key : null;
  const retryController = useRef<AbortController | null>(null);
  useEffect(() => () => retryController.current?.abort(), [key]);
  function refresh() { setFailure(null); read.refresh(); }
  async function retry() {
    if (!version || retryKey === key) return;
    const requestKey = key;
    setRetryKey(key);
    setFailure(null);
    try {
      const controller = new AbortController();
      retryController.current = controller;
      const response = await retryP6Evidence(admission.getToken(), { graphId, ...(sourceItemId ? { sourceItemId } : { sourceId }), versionId: version.version_id }, controller.signal);
      if (activeKey.current !== requestKey) return;
      admission.getToken();
      if (!response.accepted) throw new Error("Extraction could not be queued. Refresh the status and try again.");
      void initial.refetch();
      if (activeRead) activeRead.refresh();
    } catch (e) {
      if (activeKey.current === requestKey) setFailure({ key, text: e instanceof Error ? e.message : "Couldn’t retry atom extraction." });
    } finally {
      if (activeKey.current === requestKey) setRetryKey(null);
    }
  }
  function loadOlder() {
    if (page?.next_cursor) setPaging({ scopeKey, versionId: version?.version_id, before: page.next_cursor });
  }
  function loadMoreAtoms() {
    if (version?.atoms_next_cursor) setPaging({ scopeKey, versionId: version.version_id, atomsAfter: version.atoms_next_cursor, before: activePaging?.before });
  }

  const selectedAtom = version?.atoms.find((atom) => atom.atom_id === atomId);
  const selectedPassage = selectedAtom?.passages.find((passage) => passage.passage_id === passageId) ?? (!passageId ? selectedAtom?.passages[0] : undefined);
  const versions = page ? [...page.versions] : [];
  if (version && !versions.some((v) => v.version_id === version.version_id)) versions.unshift({ version_id: version.version_id, captured_at: version.captured_at, is_legacy_baseline: version.is_legacy_baseline, is_current: false });

  return (
    <section id={isMeeting ? "meeting-evidence" : "source-evidence"} aria-labelledby="source-evidence-heading" className="min-w-0 scroll-mt-6 space-y-4">
      <div>
        <h2 id="source-evidence-heading" className="font-display text-3xl font-normal text-ink">{isMeeting ? "Preserved transcript and atoms" : "Preserved source and atoms"}</h2>
        <p className="mt-2 max-w-prose text-sm leading-6 text-ink-2">Explore what was captured, one version at a time. Select an atom’s evidence to see its exact passage.</p>
      </div>

      {error && <ErrorNote action={<Button size="sm" onClick={() => refresh()}>Refresh evidence</Button>}>{error}</ErrorNote>}
      {!error && read.refreshError && <ErrorNote action={<Button size="sm" onClick={() => refresh()}>Refresh evidence</Button>}>{read.refreshError}</ErrorNote>}
      {read.loading && !error && <Loading what="loading preserved evidence" />}
      {!read.loading && !error && !page && <Empty title="No preserved version yet" hint="No permitted retained evidence is available for this capture." />}
      {page && !version && <Empty title="No preserved version yet" hint={isMeeting ? "Older meetings may not have versioned evidence. Verified retained source evidence may be unavailable for older captures." : "This source has no preserved version yet."} />}

      {page && version && <>
        {version.origin?.transport === "synthetic" && <p className="text-sm font-medium text-ink-2">Synthetic demo evidence</p>}
        <div className="flex flex-wrap items-end gap-3 border-b border-line pb-4">
          <label className="flex min-w-0 flex-col gap-2 text-sm font-medium text-ink">
            {isMeeting ? "Preserved transcript version" : "Preserved source version"}
            <select value={version.version_id} onChange={(event) => router.replace(href(event.target.value), { scroll: false })} className="field max-w-full rounded-field border border-line bg-bg px-3 py-2 text-sm text-ink">
              {versions.map((v) => <option key={v.version_id} value={v.version_id}>{v.is_legacy_baseline ? "Preserved " : "Captured "}{capturedLabel(v.captured_at)}{v.is_current ? " · current capture" : ""}</option>)}
            </select>
          </label>
          {page.next_cursor && <Button disabled={olderKey === key} onClick={() => void loadOlder()}>{olderKey === key ? "Loading versions…" : "Older versions"}</Button>}
          <p className="text-xs leading-5 text-ink-2">{version.is_legacy_baseline ? "This date records when older source content was preserved. Its original capture time is unknown." : "Capture time records when CoralSwarm saved this version."}</p>
        </div>

        <SourceSnapshotMetadata version={version} />
        {sourceItemId && <Button onClick={() => {
          if (activeKey.current !== key) return;
          try { admission.getToken(); router.push(scopedChatHref({ graph_ids: [graphId], source_item_id: sourceItemId, source_version_id: version.version_id })); }
          catch { setFailure({ key, text: "The account changed. Verify this exact capture before asking." }); }
        }}>Ask about this exact capture</Button>}

        <div className="flex flex-wrap items-start justify-between gap-3" role="status">
          <div className="max-w-prose text-sm leading-6 text-ink-2">
            <p>{isMeeting ? STATUS_COPY[version.extraction_status] : STATUS_COPY[version.extraction_status].replaceAll("transcript", "source text")}</p>
            {version.extraction_error && <p>{version.extraction_error}</p>}
          </div>
          <div className="flex flex-wrap gap-2">
            {version.can_retry && <Button disabled={retryKey === key} onClick={() => void retry()}>{retryKey === key ? "Queuing extraction…" : version.extraction_status === "not_started" ? "Extract atoms" : "Retry extraction"}</Button>}
          </div>
        </div>

        {atomId && !selectedAtom && version.extraction_status === "done" && <p role="status" className="text-sm text-ink-2">The linked atom is not available in this version.</p>}
        {passageId && selectedAtom && !selectedPassage && <p role="status" className="text-sm text-ink-2">The linked passage is not available in this atom.</p>}

        <div className="grid min-w-0 gap-6 lg:grid-cols-2">
          <div className="min-w-0">
            <h3 className="mb-3 text-base font-semibold text-ink">Atoms <span className="font-normal text-ink-2">({version.atoms.length}{version.atoms_next_cursor || atomId || activePaging?.atomsAfter ? " shown" : ""})</span></h3>
            {atomId && <p className="mb-3 text-sm text-ink-2">Showing atoms from the selected passage onward. <Link scroll={false} href={href(version.version_id)} className={LINK_STYLE}>Show from the beginning</Link></p>}
            {version.atoms.length === 0 ? <p className="text-sm leading-6 text-ink-2">{version.extraction_status === "done" ? "No supported atoms were found in this version." : "Atoms will appear here when extraction finishes."}</p> : (
              <ul className="divide-y divide-line border-y border-line">
                {version.atoms.map((atom) => <li key={atom.atom_id} className={`min-w-0 space-y-3 px-3 py-4 ${atom.atom_id === atomId ? "bg-brand-soft" : "bg-bg"}`}>
                  <div>
                    <span className="font-mono text-xs text-ink-2">{atom.kind}</span>
                    <p className="mt-1 break-words text-base leading-6 text-ink">{atom.statement}</p>
                  </div>
                  {atom.passages.map((passage, index) => <div key={passage.passage_id} className="space-y-1">
                    <blockquote className="break-words text-sm leading-6 text-ink-2">“{passage.quote}”</blockquote>
                    <Link scroll={false} href={href(version.version_id, atom.atom_id, passage.passage_id)} className={`inline-block text-sm ${LINK_STYLE}`} aria-current={selectedPassage?.passage_id === passage.passage_id ? "location" : undefined}>Show exact passage{atom.passages.length > 1 ? ` ${index + 1}` : ""}</Link>
                  </div>)}
                  <div className="flex flex-wrap items-baseline gap-x-3 gap-y-2 text-sm">
                    <span className="text-ink-2">Connected nodes</span>
                    {atom.nodes.length ? atom.nodes.map((node) => <Link key={node.node_id} href={`/topics?${new URLSearchParams({ id: node.node_id, graph_id: node.graph_id, ...((sourceItemId || version.source_item_id) ? { source_item_id: sourceItemId || version.source_item_id || "", version_id: version.version_id } : {}) })}`} className={`break-words ${LINK_STYLE}`}>{node.title || "Unnamed node"}</Link>) : <span className="text-ink-2">None linked yet</span>}
                  </div>
                </li>)}
              </ul>
            )}
            {version.atoms_next_cursor && <Button className="mt-3" disabled={moreAtomsKey === key} onClick={() => void loadMoreAtoms()}>{moreAtomsKey === key ? "Loading atoms…" : "More atoms"}</Button>}
          </div>
          <div className="min-w-0">
            <h3 className="mb-3 text-base font-semibold text-ink">Saved passage in context</h3>
            {version.text_status === "available" && version.raw_text !== null ? <PreservedTranscript text={version.raw_text} passage={selectedPassage} label={isMeeting ? "Preserved transcript" : "Preserved source text"} /> : <p className="text-sm leading-6 text-ink-2">{isMeeting ? "The preserved transcript text is temporarily unavailable. Refresh evidence to try loading it again." : "The preserved source text is temporarily unavailable. Refresh evidence to try loading it again."}</p>}
            <p className="mt-3 text-xs leading-5 text-ink-2">{isMeeting ? "This saved representation is separate from any later summary or correction. It does not establish current validity." : "Passages refer to this saved representation. The upstream source may have changed since capture."}</p>
          </div>
        </div>
      </>}
    </section>
  );
}
