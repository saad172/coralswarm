"use client";

import { Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { OrgDeletionPage } from "@/components/org/OrgDeletionPage";

/* In-app org deletion (backend control 0387). Outside the (app) layout on
   purpose: while an org is being deleted its token is refused by every route
   the app shell boots from. */
function WithParams() {
  const params = useSearchParams();
  return <OrgDeletionPage orgParam={params.get("org")} />;
}

export default function OrgDeletionRoute() {
  return (
    <Suspense fallback={null}>
      <WithParams />
    </Suspense>
  );
}
