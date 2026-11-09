"use client";

import { useEffect } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Loading } from "@/components/ui";

/* Client-side redirect for a route the redesign retired.

   Client-side because the app was once built as a static export, where
   `redirects()` in next.config is silently dropped; the stubs remain.

   `preserveQuery` carries the search string across, so a bookmarked
   `/reef?id=<uuid>` lands on `/workspaces?id=<uuid>` rather than losing which
   thing the link was pointing at. */

export default function RouteRedirect({
  to,
  preserveQuery = true,
}: {
  to: string;
  preserveQuery?: boolean;
}) {
  const router = useRouter();
  const params = useSearchParams();

  useEffect(() => {
    const qs = preserveQuery ? params.toString() : "";
    router.replace(qs ? `${to}?${qs}` : to);
  }, [router, params, to, preserveQuery]);

  return <Loading what="redirecting" />;
}
