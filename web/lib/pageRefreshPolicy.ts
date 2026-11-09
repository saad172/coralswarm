import type { Query, QueryClient } from "@tanstack/react-query";
import { hasCriticalPageObserver } from "./pageReadPriority";
import manifest from "./pageSections.generated.json" with { type: "json" };
import { refreshOwed, type PageModel, type Stored } from "./pageSectionWire";

export const STORED_POLL_MS = 2_000;
/** Usable answers do not need the cold-page cadence, including when a
 * worker's ETA has passed. Keep retries finite and back off failures. */
export const STALE_POLL_MS = 30_000;
/** Two additional scheduler cadences are a finite client recovery allowance,
 * not a promise that a contended/dormant worker will dispatch by then. */
export const RECOVERY_POLL_ALLOWANCE = 2;
export interface RefreshLimits { max_delay_seconds: number; compute_timeout_ms: number; compute_total_timeout_ms: number; worker_base_poll_ms: number }
export function refreshLimits(model: PageModel): RefreshLimits {
  const limits = manifest[model] as unknown as RefreshLimits;
  if (![limits.max_delay_seconds, limits.compute_timeout_ms, limits.compute_total_timeout_ms, limits.worker_base_poll_ms].every((n) => Number.isFinite(n) && n > 0)) {
    throw new Error("Stored refresh limits are unavailable. Retry after updating the page registry.");
  }
  return limits;
}
export function refreshRecoveryMs(limits: RefreshLimits): number {
  return limits.max_delay_seconds * 1_000 + limits.compute_total_timeout_ms +
    limits.worker_base_poll_ms * (1 + RECOVERY_POLL_ALLOWANCE);
}
export class PageRefreshExpiredError extends Error {
  constructor() { super("This section is still being prepared. Automatic checks have stopped; retry to check again."); }
}

/** ETA changes do not extend a cycle. Only a later verified computed_at,
 * resolved owed state, or an explicit read after a stopped cycle can do so. */
export class StoredRefreshCycle {
  deadline: number | null = null;
  computedAt: number | null = null;
  eta: number | null = null;
  failures = 0;
  stopped = false;
  constructor(readonly limits: RefreshLimits, readonly awaitingFresh = false) {}
  beforeRead(now: number, restartStopped = true) {
    // A fan-in read may be polling a different constituent. It must not
    // replenish this constituent's exhausted recovery allowance.
    if (this.stopped) {
      if (!restartStopped) return;
      this.deadline = null; this.failures = 0; this.stopped = false;
    }
    if (this.deadline !== null && now >= this.deadline) {
      this.stopped = true;
      // Exhausting background recovery must not turn a usable answer into
      // a load error. The final read still verifies current authority.
      if (this.computedAt === null) throw new PageRefreshExpiredError();
    }
  }
  observe(stored: Stored<unknown>, now: number) {
    if (!refreshOwed(stored)) { this.deadline = null; this.failures = 0; this.stopped = false; this.computedAt = null; this.eta = null; return; }
    const computedAt = stored.computed_at === null ? null : Date.parse(stored.computed_at);
    if (this.deadline === null || computedAt === null && this.computedAt !== null ||
        computedAt !== null && (this.computedAt === null || computedAt > this.computedAt)) {
      this.deadline = now + refreshRecoveryMs(this.limits);
      this.stopped = false;
    }
    // A withheld answer is a new authority boundary, never a usable cache hit.
    this.computedAt = computedAt === null ? null : Math.max(this.computedAt ?? computedAt, computedAt);
    this.eta = stored.refresh_eta === null ? null : Date.parse(stored.refresh_eta);
    this.failures = 0;
  }
  failure() { this.failures++; }
  stop() { this.stopped = true; }
  interval(now: number): number | false {
    if (this.deadline === null || this.stopped) return false;
    // A deadline check may wake the query without another network start.
    const passive = this.computedAt !== null && !this.awaitingFresh;
    const cadence = (passive ? STALE_POLL_MS : STORED_POLL_MS) * 2 ** Math.min(this.failures, 2);
    const delay = passive && this.eta !== null ? Math.max(cadence, this.eta - now) : cadence;
    return Math.max(1, Math.min(delay, this.deadline - now));
  }
}
const cycles = new WeakMap<object, Map<string, StoredRefreshCycle>>();
const watchedClients = new WeakSet<QueryClient>();
/** Recovery belongs to required observers. A dormant prefetch must not age
 * into a failed first open, or keep automatic work alive after navigation. */
export function watchPageRefreshObservers(client: QueryClient | undefined) {
  if (!client || watchedClients.has(client)) return;
  watchedClients.add(client);
  client.getQueryCache().subscribe((event) => {
    if (!hasCriticalPageObserver(event.query) && (event.type === "observerRemoved" || event.query.state.fetchStatus === "idle"))
      for (const cycle of existingPageRefreshCycles(event.query)) cycle.stop();
  });
}
export function pageRefreshCycle(query: object, model: PageModel, identity: string = model): StoredRefreshCycle {
  let group = cycles.get(query);
  if (!group) cycles.set(query, group = new Map());
  let cycle = group.get(identity);
  // These views supply current connector/meeting controls. A usable old
  // certificate can render immediately, but cannot delay reconciliation of
  // a saved choice or followed thread by the passive grid's 30s floor/ETA.
  if (!cycle) group.set(identity, cycle = new StoredRefreshCycle(refreshLimits(model),
    model === "connector_detail" || model === "teams_transcript_captures"));
  return cycle;
}
export function existingPageRefreshCycles(query: object): StoredRefreshCycle[] { return [...(cycles.get(query)?.values() ?? [])]; }
export function retainPageRefreshCycles(query: object, identities: Set<string>) {
  const group = cycles.get(query);
  if (!group) return;
  for (const [identity, cycle] of group) if (!identities.has(identity)) { cycle.stop(); group.delete(identity); }
}
export function pageRefreshInProgress(query: Query | undefined): boolean {
  if (!query) return false;
  return existingPageRefreshCycles(query).some((cycle) => cycle.deadline !== null && !cycle.stopped);
}
