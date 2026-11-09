"use client";

import { Suspense, useCallback, useEffect, useLayoutEffect, useMemo, useState, useSyncExternalStore, type ReactNode } from "react";
import { notifyManager, useIsFetching, useQueryClient } from "@tanstack/react-query";
import { usePathname, useSearchParams } from "next/navigation";
import { useApp } from "@/lib/providers";
import type { OceansIndex } from "@/lib/oceansIndex";
import { hasCriticalPageObserver } from "@/lib/pageReadPriority";
import { pageRefreshInProgress } from "@/lib/pageRefreshPolicy";
import { createPageReadRegistration, PageReadRegistrationContext } from "@/lib/pageReadRegistration";
import { criticalReadCount, subscribeCriticalReads, setPageLoadGate } from "@/lib/transport";

/** The committed page tree is the registration barrier. A pending Stored
 * answer is still loading even between its 2s polls. A verified stored answer
 * is loaded while its refresh is owed, so background prefetch can proceed. */
function Boundary({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const search = useSearchParams();
  const { ready, authReady, queryScope, bootError } = useApp();
  const qc = useQueryClient();
  const key = `${queryScope}|${pathname}?${search}`;
  const registration = useMemo(createPageReadRegistration, [key]);
  const subscribeCritical = useCallback((listener: () => void) => subscribeCriticalReads(notifyManager.batchCalls(listener)), []);
  const critical = useSyncExternalStore(subscribeCritical, criticalReadCount, () => 0);
  const deferred = useSyncExternalStore(registration.subscribe, registration.snapshot, () => 0);
  useLayoutEffect(() => () => registration.dispose(), [registration]);
  const [committed, setCommitted] = useState<string | null>(null);
  // Subscribe to data, observer and status transitions, not only HTTP count:
  // a successful withheld envelope has fetchStatus idle but is not loaded.
  const [revision, setRevision] = useState(0);
  useEffect(() => qc.getQueryCache().subscribe(notifyManager.batchCalls(() => setRevision((n) => n + 1))), [qc]);
  const fetching = useIsFetching({ predicate: (q) => hasCriticalPageObserver(q) });
  const pending = qc.getQueryCache().findAll().some((q) => {
    if (!hasCriticalPageObserver(q)) return false;
    if (q.state.status === "error") return pageRefreshInProgress(q);
    const data = q.state.data as { computed_at?: string | null; refresh_eta?: string | null } | undefined;
    return (q.meta?.model === "oceans_index" && (q.state.data as OceansIndex | undefined)?.components.some((c) => c.stored.computed_at === null)) || q.state.status === "pending" || data?.computed_at === null;
  });
  const failed = !!bootError || qc.getQueryCache().findAll().some((q) => hasCriticalPageObserver(q) && q.state.status === "error" && !pageRefreshInProgress(q));
  const settled = committed === key && (ready || !!bootError) && fetching === 0 && critical === 0 && !pending && deferred === 0;
  const loaded = settled && !failed;
  useLayoutEffect(() => {
    setPageLoadGate(key, false);
    return () => {
      setPageLoadGate(key, false);
      // Cancel outgoing speculative reads. A foreground observer that joined
      // the same query owns it now and must retain its request.
      void qc.cancelQueries({ predicate: (query) => query.meta?.pageRead === true && query.meta.priority === "background" && !hasCriticalPageObserver(query) }, { revert: false });
    };
  }, [key, qc]);
  useEffect(() => { setCommitted(key); }, [key, authReady]);
  useEffect(() => { setPageLoadGate(key, settled); }, [key, settled, revision]);
  return <PageReadRegistrationContext.Provider value={registration}><div className="contents" data-page-loaded={loaded ? "true" : "false"} data-page-settled={settled ? "true" : "false"} data-page-failed={failed ? "true" : "false"} data-page-required={deferred}>{children}</div></PageReadRegistrationContext.Provider>;
}

export function PageLoadBoundary({ children }: { children: ReactNode }) {
  return <Suspense fallback={children}><Boundary>{children}</Boundary></Suspense>;
}
