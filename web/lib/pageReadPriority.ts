import type { Query } from "@tanstack/react-query";
import { beginCriticalRead, type RequestPriority, type RequestPrioritySource } from "./transport";
import type { PageReadContext } from "./pageReadAuthority";

/** Inspect actual observer registrations rather than Query.meta, which is
 * the last writer's metadata when foreground and warmer share a cache key. */
export function hasCriticalPageObserver(query: Query): boolean {
  return query.observers.some((observer) => {
    const { meta, enabled } = observer.options;
    return meta?.pageRead === true && meta.priority !== "background" && meta.priority !== "normal" &&
      (typeof enabled === "function" ? enabled(query) : enabled) !== false;
  });
}

/** Query deduplication may join an already queued warmer without another
 * queryFn/GET. Observe foreground registration and promote that same request.
 * Promotion is monotonic, and its critical lease includes body verification. */
export function pageReadPriority(context: PageReadContext, declared: RequestPriority) {
  let priority = declared;
  let closed = false;
  let releaseCritical = priority === "critical" ? beginCriticalRead(context.signal) : undefined;
  const listeners = new Set<(priority: RequestPriority) => void>();
  const rank = { critical: 0, normal: 1, background: 2 };
  const promote = (next: unknown) => {
    if (closed || (next !== "critical" && next !== "normal" && next !== "background") || rank[next] >= rank[priority]) return;
    priority = next;
    if (next === "critical") releaseCritical ??= beginCriticalRead(context.signal);
    listeners.forEach((listener) => listener(next));
  };
  const query = context.queryKey && context.client?.getQueryCache().find({ queryKey: context.queryKey, exact: true });
  if (query) promote(hasCriticalPageObserver(query) ? "critical" : query.meta?.priority);
  const stop = query && context.client?.getQueryCache().subscribe((event) => {
    if (event.query === query) promote(hasCriticalPageObserver(query) ? "critical" : query.meta?.priority);
  });
  const source: RequestPrioritySource = {
    current: () => priority,
    subscribe: (listener) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
  };
  const release = () => {
    if (closed) return;
    closed = true;
    stop?.(); releaseCritical?.(); listeners.clear();
    context.signal.removeEventListener("abort", release);
  };
  context.signal.addEventListener("abort", release, { once: true });
  if (context.signal.aborted) release();
  return { source, release };
}
