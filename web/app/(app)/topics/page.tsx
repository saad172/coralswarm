"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import RouteRedirect from "@/components/RouteRedirect";
import { EvidenceIdentityBoundary } from "@/components/detail/EvidenceIdentityBoundary";
import { TopicPage } from "@/components/topic/TopicPage";
import { Loading } from "@/components/ui";

/* `/topics?id=<node_id>[&graph_id=]`: the standalone topic (reef) page, reached
   from chat citations, Today's side panel and the REEFS TOUCHED chips. It is
   not listed anywhere and there is no index of topics: without an id there is
   nothing to show, so it goes Home. */

function Topic() {
  const params = useSearchParams();
  const id = params.get("id");
  if (!id) return <RouteRedirect to="/home" preserveQuery={false} />;
  return <TopicPage nodeId={id} graphId={params.get("graph_id") || undefined} />;
}

export default function TopicDetailPage() {
  return (
    <Suspense fallback={<Loading what="verifying topic" />}>
      <EvidenceIdentityBoundary>
        <Topic />
      </EvidenceIdentityBoundary>
    </Suspense>
  );
}
