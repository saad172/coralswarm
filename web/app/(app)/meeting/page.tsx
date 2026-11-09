"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { EvidenceIdentityBoundary } from "@/components/detail/EvidenceIdentityBoundary";
import { LegacyCaptureDetail } from "@/components/detail/LegacyCaptureDetail";
import { MeetingEvidence } from "@/components/meetings/MeetingEvidence";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { validateLegacyCaptureRoute } from "@/lib/libraryRouteState";
import { Loading } from "@/components/ui";

function MeetingDetail() {
  const params = useSearchParams();
  const admission = useEvidenceAdmission();
  const graphId = params.get("graph_id") || admission.currentGraphId;
  const sourceId = params.get("source_id");
  let validRoute = true;
  try { validateLegacyCaptureRoute(new URLSearchParams(params.toString())); } catch { validRoute = false; }
  return <div className="min-w-0 space-y-8">
    <LegacyCaptureDetail />
    {validRoute && graphId && sourceId && <MeetingEvidence token={null} graphId={graphId} sourceId={sourceId} />}
  </div>;
}
export default function MeetingSummaryPage() {
  return <Suspense fallback={<Loading what="verifying meeting evidence" />}><EvidenceIdentityBoundary><MeetingDetail /></EvidenceIdentityBoundary></Suspense>;
}
