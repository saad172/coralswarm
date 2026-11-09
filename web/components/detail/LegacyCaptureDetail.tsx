"use client";

import Link from "next/link";
import type { MouseEvent } from "react";
import { useSearchParams } from "next/navigation";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { libraryReadFailure } from "@/lib/useLibraryRead";
import { useCaptureDetail } from "@/lib/useCaptureDetail";
import { captureSummary } from "@/lib/pageSectionTypes/captureDetail";
import { validateLegacyCaptureRoute } from "@/lib/libraryRouteState";
import { sourceEvidenceHref } from "@/lib/provenance";
import { DetailBreadcrumb } from "./DetailBreadcrumb";
import { Button, Empty, ErrorNote, Loading } from "@/components/ui";

function eventTime(value: string | null | undefined) {
  return value && Number.isFinite(new Date(value).getTime()) ? new Date(value).toLocaleString() : "Unknown";
}

/** The legacy container is an address, not proof of a summary, hierarchy or
 * project association. Only server-verified exact source pointers are links. */
export function LegacyCaptureDetail() {
  const params = useSearchParams();
  const admission = useEvidenceAdmission();
  let routeError: string | null = null;
  try { validateLegacyCaptureRoute(new URLSearchParams(params.toString())); }
  catch (error) { routeError = error instanceof Error ? error.message : "This capture link is invalid."; }
  const id = params.get("source_id") || "";
  const returnLibrary = params.get("returnLibrary");
  const safeReturn = returnLibrary && /^\/home(?:[/?]|$)/.test(returnLibrary) && returnLibrary.length <= 12000 ? returnLibrary : "/home";
  const graphId = params.get("graph_id") || admission.currentGraphId;
  const paramsText = params.toString();
  const getToken = () => {
    if (`${window.location.pathname}${window.location.search}` !== `/meeting${paramsText ? `?${paramsText}` : ""}`) throw new Error("The selected capture address changed. Verify the current source.");
    return admission.getToken();
  };
  const query = useCaptureDetail({ graphId: graphId || "", sourceId: id,
    versionId: params.get("version_id") || undefined, atomId: params.get("semantic_atom_id") || undefined }, !routeError && !!graphId && !!id);
  const read = { data: query.data ? captureSummary(query.data, graphId || "", id) : undefined,
    error: query.error ? libraryReadFailure(query.error) : undefined, refreshError: undefined, loading: !query.data && !query.error,
    refresh: () => { void query.refetch(); } };
  const verifyNavigation = (event: MouseEvent<HTMLAnchorElement>) => {
    try { getToken(); } catch { event.preventDefault(); read.refresh(); }
  };
  if (routeError) return <ErrorNote>{routeError}</ErrorNote>;
  if (!id) return <Empty title="No meeting selected" hint="Open a captured source from Home or a chat citation." />;
  if (!graphId) return <Empty title="Choose a workspace" hint="Select the workspace where this source was captured." />;
  return <div className="min-w-0 space-y-6">
    <DetailBreadcrumb crumbs={[{ label: "Home", href: safeReturn }, { label: "Meeting" }]} />
    <h1 className="font-display text-4xl font-normal text-ink">Meeting retained evidence</h1>
    {(read.error ?? read.refreshError) && <ErrorNote action={<Button onClick={read.refresh}>Retry verified sources</Button>}>{read.error ?? read.refreshError}</ErrorNote>}
    {read.loading && <Loading what="verifying retained source links" />}
    {read.data && <>
      <p role="status" className="max-w-prose text-sm leading-6 text-ink-2">The unattributed stored summary and correction overlay cannot yet be attributed to permitted retained evidence. They remain unavailable as a mutable container projection. Reported context and outcomes are available only on verified retained versions below.</p>
      <dl className="flex flex-wrap gap-x-8 gap-y-3 text-sm text-ink-2">
        <div><dt>Source event time</dt><dd>{eventTime(read.data.occurred_at)}</dd></div>
      </dl>
      <section className="space-y-3" aria-label="Verified retained sources">
        <h2 className="font-display text-2xl font-normal text-ink">Verified retained sources <span className="text-ink-2">({read.data.source_items.length} shown)</span></h2>
        <p className="text-sm leading-6 text-ink-2">Source items, semantic outcomes, and raw chunks are distinct counts. Capture time does not establish current validity.</p>
        {read.data.source_items.length === 0 ? <Empty title="No verified source mapping" hint="This container has no permitted exact source version to inspect. This does not establish an empty history." /> : <ul className="divide-y divide-line border-y border-line">
          {read.data.source_items.map((source, index) => <li className="py-3" key={`${source.source_item_id}:${source.source_version_id}`}><Link onClick={verifyNavigation} className="block break-words rounded-row text-sm text-brand-2 underline underline-offset-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand" href={sourceEvidenceHref(source.source_item_id, source.graph_id, source.source_version_id)}>Open retained source {index + 1}</Link><Link onClick={verifyNavigation} className="mt-2 inline-block rounded-row text-sm text-brand-2 underline underline-offset-4 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-brand" href={`/library?${new URLSearchParams({ g: source.graph_id, item: source.source_item_id, version: source.source_version_id, history: "1", view: "outcomes" })}`}>Inspect exact captured context and outcomes {index + 1}</Link></li>)}
        </ul>}
      </section>
    </>}
  </div>;
}
