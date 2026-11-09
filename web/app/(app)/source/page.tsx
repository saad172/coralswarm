"use client";

import { EvidenceIdentityBoundary } from "@/components/detail/EvidenceIdentityBoundary";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { useApp } from "@/lib/providers";
import { DetailBreadcrumb } from "@/components/detail/DetailBreadcrumb";
import { Empty, Loading } from "@/components/ui";
import { SourceEvidence } from "@/components/sources/SourceEvidence";

function SourcePage() {
  const params = useSearchParams();
  const { token } = useApp();
  const graphId = params.get("graph_id");
  const sourceItemId = params.get("source_item_id");
  return <>
    <DetailBreadcrumb crumbs={[{ label: "Home", href: "/home" }, { label: "Preserved evidence" }]} />
    {graphId && sourceItemId ? <SourceEvidence token={token} graphId={graphId} sourceItemId={sourceItemId} /> : <Empty title="No source selected" hint="Open a preserved source from Home, a chat citation or a node’s supporting passages." />}
  </>;
}

export default function PreservedSourcePage() {
  return <Suspense fallback={<Loading what="loading source" />}><EvidenceIdentityBoundary><SourcePage /></EvidenceIdentityBoundary></Suspense>;
}
