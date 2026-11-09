"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import { usePathname, useSearchParams } from "next/navigation";
import { useEvidenceAdmission } from "./useEvidenceAdmission";
import { prefetchConversation } from "./appQueries";
import { browserIdleEnv, IDLE_WARM_COUNT } from "./idleWarm";
import { openIntent } from "./openIntent";

/** Row handlers that warm a chat's entry once the pointer or focus settles on
 *  its row (lib/openIntent.ts). The open chat is never warmed. Spread onto
 *  each row link. */
export function useConversationPrefetch(activeId: string | null) {
  const { ready, getToken, queryScope } = useEvidenceAdmission();
  const qc = useQueryClient();
  const pathname = usePathname();
  const search = useSearchParams().toString();
  const controller = useMemo(() => ({ current: new AbortController() }), [queryScope, pathname, search]);
  const live = useRef({ ready, getToken, queryScope, activeId });
  live.current = { ready, getToken, queryScope, activeId };
  const intent = useMemo(
    () =>
      openIntent((id) => {
        const { ready, getToken, queryScope, activeId } = live.current;
        if (id !== activeId) void prefetchConversation(qc, { getAuthToken: getToken, scope: queryScope, authReady: ready }, id, controller.current.signal);
      }),
    [qc, controller],
  );
  useEffect(() => {
    if (controller.current.signal.aborted) controller.current = new AbortController();
    return () => { intent.disarm(); controller.current.abort(); };
  }, [intent, controller]);
  return useCallback(
    (id: string) => ({
      onPointerEnter: () => intent.arm(id),
      onPointerLeave: intent.disarm,
      onFocus: () => intent.arm(id),
      onBlur: intent.disarm,
    }),
    [intent],
  );
}

/** Per page load: which of the current scope's newest chats were warmed. */
const idleWarmed = new WeakMap<QueryClient, { key: string; done: Set<string> }>();

/** Queue the three newest Stored details together. Transport opens their
 * slots only after the page's critical registrations settle, and preempts
 * them for later critical work. Visibility is eligibility, never a timer. */
export function useIdleConversationPrefetch(rankedIds: readonly string[], activeId: string | null) {
  const { ready, getToken, queryScope } = useEvidenceAdmission();
  const qc = useQueryClient();
  const pathname = usePathname();
  const search = useSearchParams().toString();
  const activeRef = useRef(activeId);
  activeRef.current = activeId;
  const newest = rankedIds.slice(0, IDLE_WARM_COUNT).join(" ");
  const [wake, setWake] = useState(0);
  useEffect(() => {
    const resume = () => setWake((n) => n + 1);
    document.addEventListener("visibilitychange", resume);
    window.addEventListener("online", resume);
    return () => {
      document.removeEventListener("visibilitychange", resume);
      window.removeEventListener("online", resume);
    };
  }, []);
  useEffect(() => {
    if (!ready || !newest || !browserIdleEnv.eligible()) return;
    const key = JSON.stringify([queryScope, pathname, search, newest]);
    let run = idleWarmed.get(qc);
    if (run?.key !== key) idleWarmed.set(qc, (run = { key, done: new Set() }));
    const { done } = run;
    const ctx = { getAuthToken: getToken, scope: queryScope, authReady: ready };
    const controller = new AbortController();
    for (const id of newest.split(" ")) {
      if (done.has(id) || id === activeRef.current) continue;
      void prefetchConversation(qc, ctx, id, controller.signal).then(() => {
        if (!controller.signal.aborted) done.add(id);
      });
    }
    return () => controller.abort();
  }, [qc, ready, getToken, queryScope, pathname, search, newest, wake]);
}
