"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { Loading } from "@/components/ui";

/* Client-side stub for a retired Settings route. The web app is a static
   export (no server redirects), so old links and bookmarks land on a tiny
   page that replaces itself with the new route. `replace`, not `push`, so
   Back doesn't bounce through the dead URL. Query string and hash carry
   over. */
export function SettingsRedirect({ to }: { to: string }) {
  const router = useRouter();
  useEffect(() => {
    router.replace(`${to}${window.location.search}${window.location.hash}`);
  }, [router, to]);
  return <Loading what="settings" />;
}
