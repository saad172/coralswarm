// W1 — the client transport chokepoint.
//
// Every call out of `lib/api.ts`'s `req()` / `reqPaged()` passes through
// `guardedFetch` here. This module exists because CoralSwarm's web client had
// no shared request infrastructure at all: any screen could issue an unbounded
// number of concurrent fetches, and one did — the Reefs workbench fanned out
// one request per reef (3,440 of them) and the browser answered with
// `net::ERR_INSUFFICIENT_RESOURCES`. Nothing anywhere clamped it, because there
// was no layer whose job that was.
//
// The design principle (plan §4.2, docs/plans/request-reliability-and-scale.md):
// ONE chokepoint per concern. `req()` was already the single client egress, so
// policy is enforced here rather than asked of 22 call sites.
//
// WHAT IS DELIBERATELY NOT HERE:
//   - The SSE/ask stream (`lib/api.ts:198`, `/v1/queries/stream`) keeps its own
//     fetch. A streaming response holds its slot for the whole answer, so
//     putting it under a 6-slot semaphore would let two open ask tabs consume a
//     third of the budget indefinitely. It has its own longer budget upstream.
//   - Caching. That is W4 (TanStack Query). This layer is transport only; the
//     in-flight coalescing below is not a cache — nothing is retained after the
//     response settles.
//
// THE FAIRNESS PROBLEM, WHICH IS THE WHOLE POINT OF THE QUOTA:
// A plain global FIFO semaphore would make the SITEWIDE symptom worse, not
// better. Load Reefs, navigate away mid-load, and every subsequent screen's
// requests queue behind ~3,440 orphaned ones. So a bare semaphore is not
// shipped here: the scheduler below is fair-share (see `pickNextWaiter`), the
// queue has a wait cap distinct from the request timeout, and aborting really
// removes work rather than merely ignoring its result.

import { dispatchPageReadTiming, responsePageReadTiming, joinPageReadTiming, clonePageReadTiming, type PageReadTimingRun } from "./pageReadTimingReceipt";
import { apiBase } from "./apiBase";

// ── Configuration ───────────────────────────────────────────────────────────

export interface TransportConfig {
  /** Max requests in flight to the API origin at once, across all callers. 6
   *  matches what browsers themselves allow per HTTP/1.1 origin, so this is
   *  not an arbitrary throttle — it is the real ceiling, made explicit and
   *  enforced before the browser's own queue (which is what overflowed). */
  maxInFlight: number;
  /** Hard ceiling on how many of `maxInFlight` any one caller may hold, even
   *  when it is the only caller. Keeps one route from owning every slot. */
  maxPerCallerInFlight: number;
  /** Max queued entries per caller. Beyond this, enqueue REJECTS rather than
   *  growing an unbounded queue — a 3,440-deep queue is not backpressure, it
   *  is the same bug with a different shape. */
  maxPerCallerQueued: number;
  /** Max time a request may sit on the queue before being rejected. Distinct
   *  from `requestTimeoutMs` on purpose: a 15 s request timeout on a request
   *  that waited four minutes for a slot is not a timeout, it is a hang. */
  queueWaitCapMs: number;
  /** Per-attempt request timeout. Starts when the SLOT IS ACQUIRED, not at
   *  enqueue, so queue time is never charged against it. */
  requestTimeoutMs: number;
  /** Total attempts including the first. */
  maxAttempts: number;
  /** Base for exponential backoff, milliseconds. */
  retryBaseMs: number;
  /** Ceiling on a single backoff sleep, milliseconds. */
  retryMaxMs: number;
  /** Consecutive failures to an origin before the breaker opens. */
  breakerThreshold: number;
  /** How long the breaker stays open before allowing a half-open probe. */
  breakerCooldownMs: number;
}

export const DEFAULT_TRANSPORT_CONFIG: TransportConfig = {
  maxInFlight: 6,
  maxPerCallerInFlight: 3,
  maxPerCallerQueued: 4096,
  queueWaitCapMs: 10_000,
  requestTimeoutMs: 15_000,
  maxAttempts: 3,
  retryBaseMs: 250,
  retryMaxMs: 5_000,
  breakerThreshold: 5,
  breakerCooldownMs: 10_000,
};

let config: TransportConfig = { ...DEFAULT_TRANSPORT_CONFIG };

/** Rollback lever (plan §9): `NEXT_PUBLIC_TRANSPORT_GUARDS=0` restores the
 *  pre-W1 behaviour of calling `fetch` directly, with no redeploy of logic
 *  needed to relax it. Read per call rather than captured at module load so a
 *  test can flip it. */
function guardsEnabled(): boolean {
  return process.env.NEXT_PUBLIC_TRANSPORT_GUARDS !== "0";
}

// ── Typed failures ──────────────────────────────────────────────────────────
//
// Each of these is a DISTINCT product outcome, not an internal detail, which is
// why they are separate classes rather than one error with a string. "We queued
// your request for 10 s and gave up" and "the server took too long" call for
// different UI and different alerts (plan §6.6's client failure taxonomy).

export type TransportFailureKind =
  | "queue-timeout"
  | "queue-full"
  | "timeout"
  | "breaker-open"
  | "aborted"
  | "network";

export class TransportError extends Error {
  readonly kind: TransportFailureKind;
  readonly caller: string;
  constructor(kind: TransportFailureKind, caller: string, message: string) {
    super(message);
    this.name = "TransportError";
    this.kind = kind;
    this.caller = caller;
  }
}

// ── Instrumentation sink ────────────────────────────────────────────────────
//
// W6 ships this to OTLP. Until then it is a no-op by default — but the shape is
// fixed NOW so that W6 is a wiring change, not a re-instrumentation. Note what
// is carried: `caller` is a route TEMPLATE, never a concrete path, so this is
// a low-cardinality dimension and cannot leak reef/atom ids (plan §10).

export interface TransportEvent {
  /** Route template, e.g. `/v1/nodes/{id}/atoms`. Never a concrete path. */
  caller: string;
  method: string;
  /** HTTP status, or null when the request never produced a response. */
  status: number | null;
  /** Time on the queue before a slot was acquired. */
  queuedMs: number;
  /** Time from slot acquisition to settle, summed across attempts. */
  durationMs: number;
  /** 1-based attempt number this event describes. */
  attempt: number;
  outcome: "ok" | "http-error" | TransportFailureKind;
  /** The response's `Server-Timing` phases in ms, when the server sent it. */
  serverTiming?: ServerTiming;
}

export interface ServerTiming {
  totalMs?: number;
  authMs?: number;
  appMs?: number;
  txOpenMs?: number;
}

const SERVER_TIMING_KEYS: Record<string, keyof ServerTiming> = {
  total: "totalMs",
  auth: "authMs",
  app: "appMs",
  txopen: "txOpenMs",
};

/** Parse the backend's `Server-Timing` (`auth;dur=1.2, app;dur=40.1, …`).
 *  Unknown metrics and malformed entries are skipped, never thrown on. */
export function parseServerTiming(header: string | null): ServerTiming | undefined {
  if (!header) return undefined;
  const out: ServerTiming = {};
  let any = false;
  for (const metric of header.split(",")) {
    const [name, ...params] = metric.trim().split(";");
    const key = SERVER_TIMING_KEYS[name?.trim() ?? ""];
    if (!key) continue;
    for (const param of params) {
      const [k, v] = param.trim().split("=");
      if (k !== "dur" || v === undefined) continue;
      const dur = Number(v);
      if (Number.isFinite(dur) && dur >= 0) {
        out[key] = dur;
        any = true;
      }
    }
  }
  return any ? out : undefined;
}

function serverTimingOf(res: Response): ServerTiming | undefined {
  try {
    return parseServerTiming(res.headers?.get?.("server-timing") ?? null);
  } catch {
    return undefined;
  }
}

type TransportSink = (event: TransportEvent) => void;

let sink: TransportSink | null = null;

/** Register the metrics sink (W6). Returns the previous one so a caller can
 *  restore it — used by tests. */
export function setTransportSink(next: TransportSink | null): TransportSink | null {
  const prev = sink;
  sink = next;
  return prev;
}

function emit(event: TransportEvent): void {
  // A broken sink must never break a request. This is telemetry.
  try {
    sink?.(event);
  } catch {
    /* ignore */
  }
}

// ── Route templating ────────────────────────────────────────────────────────

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Collapse a concrete request path to its route template:
 * `/v1/nodes/3f34…/atoms?page=2` → `/v1/nodes/{id}/atoms`.
 *
 * This is doing two jobs at once, and both matter:
 *
 *  1. **It is the caller identity for fair-share scheduling.** Every one of the
 *     3,440 reef-enrichment fetches templates to `/v1/nodes/{id}/atoms`, so
 *     they land in ONE bucket and are collectively capped — while a request
 *     from any other screen templates differently and gets its own share. That
 *     is exactly the grouping the quota needs, and it requires no call-site
 *     changes, so a screen cannot forget to declare itself.
 *  2. **It is the metric dimension for W6.** Templating here means ids never
 *     reach the telemetry pipeline in the first place, rather than being
 *     scrubbed downstream and hoped about.
 */
export function routeTemplate(path: string): string {
  const [pathname] = path.split("?");
  return pathname
    .split("/")
    .map((seg) => {
      if (!seg) return seg;
      if (UUID_RE.test(seg)) return "{id}";
      // Numeric ids and opaque/long tokens are ids too. The length threshold
      // keeps real route words (`atoms`, `candidates`, `supersede`) intact.
      if (/^\d+$/.test(seg)) return "{id}";
      if (seg.length > 40) return "{id}";
      return seg;
    })
    .join("/");
}

export type RequestPriority = "critical" | "normal" | "background";

// A page is closed to warmers until its committed tree and all active section
// observers have settled. Zero active HTTP requests alone is not a mount barrier.
let pageLoadKey = "";
let pageLoadSettled = false;
let criticalRequests = 0;
const criticalListeners = new Set<() => void>();
export const criticalReadCount = () => criticalRequests;
export function subscribeCriticalReads(listener: () => void): () => void {
  criticalListeners.add(listener);
  return () => { criticalListeners.delete(listener); };
}
function changeCriticalReads(delta: number) {
  criticalRequests = Math.max(0, criticalRequests + delta);
  criticalListeners.forEach((listener) => listener());
}
const backgroundAttempts = new Set<AbortController>();

export function setPageLoadGate(key: string, settled: boolean): void {
  if (pageLoadKey !== key) {
    for (const controller of backgroundAttempts) controller.abort();
  }
  pageLoadKey = key;
  pageLoadSettled = settled;
  if (!settled) for (const controller of backgroundAttempts) controller.abort();
  pump();
}

/** Hold across token acquisition, body decoding and credential refresh as well
 * as HTTP attempts. A response header is not a settled page section. */
export function beginCriticalRead(signal?: AbortSignal): () => void {
  if (signal?.aborted) return () => {};
  changeCriticalReads(1);
  for (const controller of backgroundAttempts) controller.abort();
  let released = false;
  const release = () => {
    if (released) return;
    released = true;
    signal?.removeEventListener("abort", release);
    changeCriticalReads(-1);
    pump();
  };
  signal?.addEventListener("abort", release, { once: true });
  if (signal?.aborted) release();
  return release;
}

interface Scheduling { priority: RequestPriority; critical: boolean }
function promote(scheduling: Scheduling, priority: RequestPriority): void {
  const rank = { critical: 0, normal: 1, background: 2 };
  if (rank[priority] < rank[scheduling.priority]) scheduling.priority = priority;
  if (scheduling.priority === "critical" && !scheduling.critical) {
    scheduling.critical = true;
    changeCriticalReads(1);
    // An interrupted GET is requeued without charging an HTTP failure/retry.
    // Its slot is released in dispatch's finally before any critical grant.
    for (const controller of backgroundAttempts) controller.abort();
  }
  pump();
}

// ── Fair-share semaphore ────────────────────────────────────────────────────

interface Waiter {
  scheduling: Scheduling;
  caller: string;
  enqueuedAt: number;
  /** Monotonic sequence for FIFO tie-breaking within an equal fair-share. */
  seq: number;
  resolve: (release: () => void) => void;
  reject: (err: Error) => void;
  settled: boolean;
  timer: ReturnType<typeof setTimeout> | null;
  detachAbort: (() => void) | null;
}

let inFlightTotal = 0;
const inFlightByCaller = new Map<string, number>();
const queuedByCaller = new Map<string, number>();
let queue: Waiter[] = [];
let seqCounter = 0;

function countOf(m: Map<string, number>, k: string): number {
  return m.get(k) ?? 0;
}

function bump(m: Map<string, number>, k: string, delta: number): void {
  const next = countOf(m, k) + delta;
  if (next <= 0) m.delete(k);
  else m.set(k, next);
}

/**
 * How many slots a single caller may hold right now.
 *
 * Deliberately DYNAMIC, and it mirrors W2's server-side fair-share formula so
 * the two layers reason the same way: divide the budget by the number of
 * callers actually contending. Alone, a caller gets `maxPerCallerInFlight`;
 * with three contending callers each gets 2; with six or more, 1 each.
 *
 * A static cap would be wrong in both directions — too low and a lone screen is
 * throttled for no reason, too high and two heavy screens can still lock
 * everyone else out.
 */
function perCallerCap(): number {
  const active = new Set<string>([...inFlightByCaller.keys(), ...queuedByCaller.keys()]).size;
  if (active <= 1) return config.maxPerCallerInFlight;
  return Math.min(
    config.maxPerCallerInFlight,
    Math.max(1, Math.floor(config.maxInFlight / active)),
  );
}

/**
 * Choose which queued waiter gets a freed slot.
 *
 * NOT plain FIFO, and the difference is load-bearing. Under FIFO, a caller with
 * 3,440 queued entries reclaims the slot it just released every single time, so
 * a request from another screen sits behind the whole backlog. Instead: among
 * waiters whose caller is under its fair-share cap, prefer the caller currently
 * holding the FEWEST slots, breaking ties by arrival order.
 *
 * That yields FIFO within a caller (order is preserved per route) and round-robin
 * across callers (no screen can starve another), which is the property the plan
 * requires and a bare semaphore does not provide.
 */
function pickNextWaiter(): Waiter | null {
  const cap = perCallerCap();
  let best: Waiter | null = null;
  let bestInFlight = Number.POSITIVE_INFINITY;
  for (const w of queue) {
    if (w.scheduling.priority === "background" && (!pageLoadSettled || criticalRequests > 0)) continue;
    const held = countOf(inFlightByCaller, w.caller);
    if (held >= cap) continue;
    // NOTE: no `seq` tie-break is needed. `queue` is only ever appended to and
    // filtered, so it is already in ascending `seq` order and the first
    // eligible waiter at a given in-flight count is also the earliest. A
    // `w.seq < best.seq` clause here would be unreachable, and writing one
    // would imply an ordering invariant that nothing enforces.
    const rank = { critical: 0, normal: 1, background: 2 };
    if (!best || rank[w.scheduling.priority] < rank[best.scheduling.priority] ||
        (w.scheduling.priority === best.scheduling.priority && held < bestInFlight)) {
      best = w;
      bestInFlight = held;
    }
  }
  return best;
}

function settle(w: Waiter): void {
  w.settled = true;
  if (w.timer !== null) clearTimeout(w.timer);
  w.detachAbort?.();
  queue = queue.filter((q) => q !== w);
  bump(queuedByCaller, w.caller, -1);
}

function pump(): void {
  while (inFlightTotal < config.maxInFlight) {
    const w = pickNextWaiter();
    if (!w) return;
    settle(w);
    grant(w);
  }
}

function grant(w: Waiter): void {
  inFlightTotal += 1;
  bump(inFlightByCaller, w.caller, 1);
  let released = false;
  w.resolve(() => {
    // Idempotent: a double-release would corrupt the accounting and silently
    // raise the real concurrency ceiling, which is the one failure mode this
    // module cannot tolerate.
    if (released) return;
    released = true;
    inFlightTotal -= 1;
    bump(inFlightByCaller, w.caller, -1);
    pump();
  });
}

interface Slot {
  release: () => void;
  queuedMs: number;
}

/**
 * Wait for a slot. Rejects — rather than waiting — when the caller's queue is
 * full, when the wait cap elapses, or when `signal` aborts.
 *
 * The abort path is why this exists as a real queue rather than a promise
 * chain: `reefs/page.tsx`'s effect cleanup sets a `cancelled` flag that aborts
 * nothing, so today unmounting Reefs frees no capacity at all. Here, aborting
 * removes the entry from the queue outright, so an abandoned screen gives its
 * budget back immediately.
 */
function acquire(caller: string, scheduling: Scheduling, signal?: AbortSignal): Promise<Slot> {
  const enqueuedAt = Date.now();

  if (signal?.aborted) {
    return Promise.reject(new TransportError("aborted", caller, "request aborted before dispatch"));
  }

  // Fast path: capacity available and nobody queued ahead. Checking the queue
  // matters — jumping it would break the FIFO-within-a-caller guarantee.
  if (inFlightTotal < config.maxInFlight && queue.length === 0 &&
      (scheduling.priority !== "background" || (pageLoadSettled && criticalRequests === 0))) {
    const cap = perCallerCap();
    if (countOf(inFlightByCaller, caller) < cap) {
      return new Promise<Slot>((resolve) => {
        grant({
          scheduling,
          caller,
          enqueuedAt,
          seq: seqCounter++,
          resolve: (release) => resolve({ release, queuedMs: 0 }),
          reject: () => {},
          settled: true,
          timer: null,
          detachAbort: null,
        });
      });
    }
  }

  if (countOf(queuedByCaller, caller) >= config.maxPerCallerQueued) {
    return Promise.reject(
      new TransportError(
        "queue-full",
        caller,
        `queue full for ${caller} (${config.maxPerCallerQueued} waiting)`,
      ),
    );
  }

  return new Promise<Slot>((resolve, reject) => {
    const w: Waiter = {
      scheduling,
      caller,
      enqueuedAt,
      seq: seqCounter++,
      resolve: (release) => resolve({ release, queuedMs: Date.now() - enqueuedAt }),
      reject,
      settled: false,
      timer: null,
      detachAbort: null,
    };

    if (scheduling.priority !== "background") w.timer = setTimeout(() => {
      if (w.settled) return;
      settle(w);
      w.reject(
        new TransportError(
          "queue-timeout",
          caller,
          `waited ${config.queueWaitCapMs}ms for a request slot`,
        ),
      );
    }, config.queueWaitCapMs);

    if (signal) {
      const onAbort = () => {
        if (w.settled) return;
        settle(w);
        w.reject(new TransportError("aborted", caller, "request aborted while queued"));
        pump();
      };
      signal.addEventListener("abort", onAbort);
      w.detachAbort = () => signal.removeEventListener("abort", onAbort);
    }

    queue.push(w);
    bump(queuedByCaller, caller, 1);
    pump();
  });
}

// ── Circuit breaker ─────────────────────────────────────────────────────────
//
// Per origin. Purpose is narrow: stop a client that is already failing from
// continuing to amplify against an origin that is already struggling. It counts
// only TRANSPORT failures (network errors, 5xx, 429) — a 404 or a 403 is the
// server working correctly and must never trip it.

interface BreakerState {
  consecutiveFailures: number;
  openedAt: number | null;
  halfOpenInFlight: boolean;
}

const breakers = new Map<string, BreakerState>();

function breakerFor(origin: string): BreakerState {
  let b = breakers.get(origin);
  if (!b) {
    b = { consecutiveFailures: 0, openedAt: null, halfOpenInFlight: false };
    breakers.set(origin, b);
  }
  return b;
}

/** Returns true when the request may proceed. Transitions open → half-open
 *  after the cooldown, allowing exactly one probe through at a time. */
function breakerAdmits(origin: string): boolean {
  const b = breakerFor(origin);
  if (b.openedAt === null) return true;
  if (Date.now() - b.openedAt < config.breakerCooldownMs) return false;
  if (b.halfOpenInFlight) return false;
  b.halfOpenInFlight = true;
  return true;
}

/**
 * Hand the half-open probe token back without recording an outcome.
 *
 * Admission takes the token BEFORE the attempt runs, but several paths leave
 * `dispatch` without ever reaching `breakerOnSuccess`/`breakerOnFailure` — the
 * slot acquisition can reject (queue-full, queue-timeout, aborted) and the
 * caller can abort the in-flight fetch. Without this, `halfOpenInFlight`
 * stays `true` while `openedAt` stays non-null, so `breakerAdmits` refuses
 * **every** subsequent request to that origin for the rest of the page's
 * life. Concretely: the breaker opens, the cooldown elapses, the user
 * navigates away mid-probe, and the app then fails every API call with
 * `breaker-open` until a reload.
 *
 * Deliberately does NOT touch `consecutiveFailures` or `openedAt`: an
 * abandoned probe is not evidence either way about the origin's health, so it
 * must neither count as a failure nor close the breaker. It only returns the
 * token so the NEXT request can probe.
 */
function breakerReleaseProbe(origin: string): void {
  const b = breakers.get(origin);
  if (b) b.halfOpenInFlight = false;
}

function breakerOnSuccess(origin: string): void {
  const b = breakerFor(origin);
  b.consecutiveFailures = 0;
  b.openedAt = null;
  b.halfOpenInFlight = false;
}

function breakerOnFailure(origin: string): void {
  const b = breakerFor(origin);
  b.halfOpenInFlight = false;
  b.consecutiveFailures += 1;
  if (b.consecutiveFailures >= config.breakerThreshold) {
    // Re-stamp on every failure past the threshold so a persistently dead
    // origin keeps the cooldown rolling instead of admitting a probe per call.
    b.openedAt = Date.now();
  }
}

// ── Retry policy ────────────────────────────────────────────────────────────

const AMBIGUOUS_RETRY_STATUS = new Set([502, 503, 504]);

/** Methods whose repetition is safe by definition (RFC 9110 §9.2.2). A retry
 *  of one of these cannot create a second row. */
const IDEMPOTENT_METHODS = new Set(["GET", "HEAD", "OPTIONS", "PUT", "DELETE"]);

/**
 * May this response be retried, given the request method?
 *
 * **The method check is the whole point, and its absence was a data-integrity
 * bug.** `req()` routes every POST/PATCH/DELETE through this layer, so a
 * method-blind retry meant a `POST` that received a 504 from the edge proxy —
 * or that hit a socket reset *after* the backend committed — was re-sent up to
 * three times. Nothing upstream deduplicates, so one user action could create
 * three team invites, three atoms, three query rows. The browser cannot
 * distinguish "never arrived" from "arrived, response lost", which is exactly
 * why 502/503/504 and network errors are the *ambiguous* cases and must not be
 * replayed blind.
 *
 * `429` is different in kind and is retried for ANY method: the server is
 * explicitly telling us it did not act and to ask again. A non-429 4xx is
 * never retried at all — that is the server saying the request itself is
 * wrong, and repeating it is pure amplification.
 *
 * A call site that carries an idempotency key can opt back in via
 * `GuardedFetchOptions.retryNonIdempotent`.
 */
function isRetryableStatus(status: number, method: string, allowUnsafe: boolean): boolean {
  if (status === 429) return true;
  if (!AMBIGUOUS_RETRY_STATUS.has(status)) return false;
  return allowUnsafe || IDEMPOTENT_METHODS.has(method);
}

/** Same reasoning as {@link isRetryableStatus} for transport-level failures: a
 *  network error after a mutation was sent is the most ambiguous case there
 *  is, because the request definitely left the browser. */
function isRetryableFailure(method: string, allowUnsafe: boolean): boolean {
  return allowUnsafe || IDEMPOTENT_METHODS.has(method);
}

/** `Retry-After` is authoritative when present — it is the server's own
 *  statement about when it will be ready, and ignoring it in favour of our
 *  backoff is how a thundering herd forms. Supports both the delta-seconds and
 *  HTTP-date forms. */
function retryAfterMs(res: Response): number | null {
  const raw = res.headers?.get?.("retry-after");
  if (!raw) return null;
  const secs = Number(raw);
  if (Number.isFinite(secs)) return Math.max(0, secs * 1000);
  const at = Date.parse(raw);
  if (Number.isNaN(at)) return null;
  return Math.max(0, at - Date.now());
}

/** Ceiling on a server-supplied `Retry-After`.
 *
 *  `retryMaxMs` bounds OUR backoff but not the server's instruction, and the
 *  sleep happens after the slot is released, so nothing else caps it: a
 *  `Retry-After: 3600` — or an HTTP-date far in the future — would leave
 *  `guardedFetch` pending for an hour with no timeout, while `lib/api.ts`
 *  awaits it. A request that hangs for an hour is indistinguishable from one
 *  that hung forever. */
const MAX_RETRY_AFTER_MS = 30_000;

/** Exponential backoff with FULL jitter (`random(0, base·2^n)`), not
 *  equal jitter: full jitter is what actually decorrelates a fleet of clients
 *  that all failed on the same server hiccup. */
function backoffMs(attempt: number): number {
  const ceiling = Math.min(config.retryMaxMs, config.retryBaseMs * 2 ** (attempt - 1));
  return Math.random() * ceiling;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(new TransportError("aborted", "", "aborted during retry backoff"));
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    function onAbort() {
      clearTimeout(t);
      reject(new TransportError("aborted", "", "aborted during retry backoff"));
    }
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

// ── In-flight GET coalescing ────────────────────────────────────────────────
//
// Identical concurrent GETs share one network call. This is NOT a cache: the
// entry is deleted the moment the response settles, so a subsequent GET always
// hits the network. It exists to kill the duplicate half of a runaway loop (E2)
// — a re-firing effect that requests the same reef four times gets one request.
//
// Subscribers are refcounted so that ONE subscriber aborting does not cancel
// the shared request out from under the others; the underlying request is only
// aborted once every subscriber has gone away.

interface InFlightEntry {
  scheduling: Scheduling;
  promise: Promise<Response>;
  controller: AbortController;
  subscribers: number;
}

const inFlightGets = new Map<string, InFlightEntry>();

/**
 * Derive the coalescing scope for an identity.
 *
 * The invariant is absolute: **two different identities must never share a
 * coalesced response.** This has now been got wrong twice, in opposite
 * directions, so it is worth stating what the code actually does rather than
 * what would sound good.
 *
 *  - A 32-bit FNV-1a digest **cannot** hold the invariant: a collision makes
 *    two tokens the same scope, and a GET issued as user B resolves with the
 *    body fetched as user A. Silent cross-tenant read.
 *  - Interning the token in a `Map` was collision-free but retained live
 *    bearer tokens in a module-scoped structure for the page's lifetime,
 *    while the comment claimed the opposite.
 *  - Four FNV-1a passes differing only in their offset basis are **not** four
 *    independent hashes — same prime, same structure, correlated output. The
 *    "128-bit" claim that replaced them was worth much closer to 32 bits.
 *
 * So this does the only thing that is unconditionally correct and needs no
 * strength argument at all: **the token is compared, not summarised.** The
 * scope key is the token itself, held only for the lifetime of the in-flight
 * request that uses it, in the same `inFlightGets` entry that is deleted the
 * moment the response settles. Nothing is retained beyond that, and no
 * collision is possible because nothing is hashed.
 *
 * The cost is a longer map key. That is not a real cost: the map holds at most
 * a handful of concurrently in-flight GETs, and it is cleared continuously.
 */
function scopeKey(token: string | null): string {
  return token ?? "anon";
}

/**
 * Read the `Authorization` header out of a `RequestInit`, whatever legal shape
 * it is in.
 *
 * `RequestInit.headers` may be a plain record, a `Headers` instance, or a
 * `[key, value][]` array. The previous code indexed it as a record only, so a
 * caller using `new Headers({ Authorization: … })` yielded `undefined` — and
 * every such caller scoped to `"anon"` and coalesced onto ONE response. The
 * collision-free hash above would have been irrelevant: the same cross-tenant
 * read, reached through a different door. No current call site uses those
 * forms, which is exactly why this had to be handled rather than assumed.
 */
function authHeaderOf(init: RequestInit): string | null {
  const h = init.headers;
  if (!h) return null;
  if (typeof Headers !== "undefined" && h instanceof Headers) {
    return h.get("Authorization") ?? h.get("authorization");
  }
  if (Array.isArray(h)) {
    const hit = h.find(([k]) => k.toLowerCase() === "authorization");
    return hit ? hit[1] : null;
  }
  const rec = h as Record<string, string>;
  // Header names are case-insensitive on the wire; a record is not.
  for (const k of Object.keys(rec)) {
    if (k.toLowerCase() === "authorization") return rec[k];
  }
  return null;
}

// ── The guarded fetch ───────────────────────────────────────────────────────

export interface RequestPrioritySource {
  current: () => RequestPriority;
  subscribe: (listener: (priority: RequestPriority) => void) => () => void;
}
export interface GuardedFetchOptions {
  /** Opt-in E2E diagnostic capability; never sent to the server. */
  pageReadTiming?: PageReadTimingRun;
  priority?: RequestPriority;
  /** A deduplicated query can acquire a foreground observer after dispatch. */
  prioritySource?: RequestPrioritySource;
  /** Caller-supplied cancellation. Aborting removes a queued request from the
   *  queue and aborts an in-flight one. */
  signal?: AbortSignal;
  /** Per-call override of the request timeout, for endpoints with a
   *  legitimately longer budget. */
  timeoutMs?: number;
  /** Override the fair-share bucket. Defaults to the route template, which is
   *  right for every current call site. */
  caller?: string;
  /** Opt a NON-idempotent request back into ambiguous retries (502/503/504,
   *  network errors). Only safe when the request carries an idempotency key or
   *  the endpoint is provably safe to repeat — see {@link isRetryableStatus}
   *  for why this is off by default. No call site sets it today. */
  retryNonIdempotent?: boolean;
}

/**
 * Perform a fetch under the full W1 policy: fair-share admission, queue-wait
 * cap, timeout, bounded retry, GET coalescing, circuit breaker, telemetry.
 *
 * Returns the raw `Response` — status handling, JSON parsing and the 401
 * refresh-and-retry stay in `lib/api.ts`, which owns those semantics. This
 * module owns *whether and when* a request goes out, not what it means.
 */
export async function guardedFetch(
  path: string,
  init: RequestInit,
  opts: GuardedFetchOptions = {},
): Promise<Response> {
  const base = apiBase();
  const priority = opts.prioritySource?.current() ?? opts.priority ?? "normal";
  const headers = new Headers(init.headers);
  headers.set("x-coralswarm-priority", priority);
  init = { ...init, headers };
  const provided = [opts.signal, init.signal].filter(Boolean) as AbortSignal[];
  const signal = provided.length > 1 ? AbortSignal.any(provided) : provided[0];
  const url = `${base}${path}`;

  // The kill switch relaxes SCHEDULING POLICY — it does not remove
  // cancellation or the timeout. Call sites pass their `AbortSignal` through
  // `opts`, not `init`, so forwarding only `init` here would make
  // `NEXT_PUBLIC_TRANSPORT_GUARDS=0` silently break unmount cancellation that
  // callers now depend on. A rollback lever that introduces a second bug is
  // not a rollback lever.
  if (!guardsEnabled()) {
    const unguarded = AbortSignal.timeout(opts.timeoutMs ?? config.requestTimeoutMs);
    const provided = [opts.signal, init.signal].filter(Boolean) as AbortSignal[];
    let signal: AbortSignal;
    if (typeof AbortSignal.any === "function") {
      signal = AbortSignal.any([...provided, unguarded]);
    } else {
      // Fallback for runtimes without `AbortSignal.any` (older webviews). The
      // timeout must SURVIVE here: an earlier version fell back to
      // `opts.signal ?? unguarded`, which meant any caller-supplied signal
      // silently removed the timeout and turned the kill switch into an
      // unbounded wait.
      const merged = new AbortController();
      const onAbort = () => merged.abort();
      for (const sig of [...provided, unguarded]) {
        if (sig.aborted) merged.abort();
        else sig.addEventListener("abort", onAbort, { once: true });
      }
      signal = merged.signal;
    }
    return fetch(url, { ...init, signal });
  }

  const method = (init.method ?? "GET").toUpperCase();
  const caller = opts.caller ?? routeTemplate(path);
  const timeoutMs = opts.timeoutMs ?? config.requestTimeoutMs;

  // Coalesce identical concurrent GETs. Keyed on the FULL url (query string
  // included — `?page=2` is a different resource) plus the identity scope.
  const authHeader = authHeaderOf(init);
  const coalesceKey =
    method === "GET" ? `${url}\u0000${scopeKey(authHeader)}\u0000${init.credentials ?? "same-origin"}` : null;

  if (coalesceKey) {
    const existing = inFlightGets.get(coalesceKey);
    if (existing) {
      promote(existing.scheduling, priority);
      existing.subscribers += 1;
      const stopPriority = opts.prioritySource?.subscribe((next) => promote(existing.scheduling, next));
      try { return await subscribeToInFlight(existing, coalesceKey, signal, caller, opts.pageReadTiming); }
      finally { stopPriority?.(); }
    }
  }

  const controller = new AbortController();
  const scheduling: Scheduling = { priority, critical: false };
  promote(scheduling, priority);
  const stopPriority = opts.prioritySource?.subscribe((next) => promote(scheduling, next));
  // For a COALESCED GET the shared request must not be tied to the first
  // subscriber's signal. If it were, the first component to unmount would
  // cancel the request every other subscriber is still waiting on — the exact
  // bug `subscribeToInFlight`'s refcount exists to prevent, reintroduced one
  // layer up. Abort control belongs to the refcount, which aborts
  // `controller` only when the last subscriber leaves.
  const run = dispatch(
    url,
    base,
    init,
    method,
    caller,
    timeoutMs,
    controller,
    coalesceKey ? undefined : signal,
    opts.retryNonIdempotent ?? false,
    scheduling,
    opts.pageReadTiming,
  ).finally(() => {
    stopPriority?.();
    if (scheduling.critical) { changeCriticalReads(-1); scheduling.critical = false; }
    pump();
  });

  if (coalesceKey) {
    const entry: InFlightEntry = { promise: run, controller, subscribers: 1, scheduling };
    inFlightGets.set(coalesceKey, entry);
    // Delete on settle — this is a coalescer, not a cache. Attached before the
    // subscription so a late-arriving GET can never join an already-settled
    // entry (whose body has been consumed).
    const forget = () => {
      if (inFlightGets.get(coalesceKey) === entry) inFlightGets.delete(coalesceKey);
    };
    run.then(forget, forget);
    return subscribeToInFlight(entry, coalesceKey, signal, caller, opts.pageReadTiming);
  }

  return run;
}

/**
 * Attach one subscriber to a shared in-flight GET.
 *
 * The subscriber's own `signal` rejects only ITS promise. The shared request is
 * aborted only when the last subscriber leaves — otherwise one component
 * unmounting would cancel a request three other components are still waiting on,
 * which is a data-loss bug dressed up as an optimization.
 */
function subscribeToInFlight(
  entry: InFlightEntry,
  key: string,
  signal: AbortSignal | undefined,
  caller: string,
  pageReadTiming?: PageReadTimingRun,
): Promise<Response> {
  // One subscription leaves exactly once, whether it leaves by aborting or by
  // the shared request settling. Without this guard an aborting subscriber
  // decrements twice — once on abort and again when the shared promise
  // resolves — which drives the refcount negative and tears down a request
  // other subscribers are still waiting on.
  let left = false;
  const leave = (): number => {
    if (left) return entry.subscribers;
    left = true;
    entry.subscribers -= 1;
    return entry.subscribers;
  };

  return new Promise<Response>((resolve, reject) => {
    let onAbort: (() => void) | null = null;

    if (signal) {
      if (signal.aborted) {
        if (leave() <= 0) {
          if (inFlightGets.get(key) === entry) inFlightGets.delete(key);
          entry.controller.abort();
        }
        reject(new TransportError("aborted", caller, "request aborted before dispatch"));
        return;
      }
      onAbort = () => {
        // Only the LAST subscriber to leave may abort the shared request.
        // Otherwise one component unmounting cancels a request three other
        // components are still waiting on — a data-loss bug dressed up as an
        // optimization.
        if (leave() <= 0) {
          if (inFlightGets.get(key) === entry) inFlightGets.delete(key);
          entry.controller.abort();
        }
        reject(new TransportError("aborted", caller, "request aborted"));
      };
      signal.addEventListener("abort", onAbort, { once: true });
    }

    entry.promise.then(
      (res) => {
        if (onAbort && signal) signal.removeEventListener("abort", onAbort);
        if (left) return; // already rejected via abort
        const remaining = leave();
        // A Response body can only be read once, so every subscriber but the
        // last gets its own clone.
        const result = remaining > 0 ? res.clone() : res;
        if (process.env.NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS === "1") {
          if (result !== res) clonePageReadTiming(res, result);
          joinPageReadTiming(pageReadTiming, result);
        }
        resolve(result);
      },
      (err) => {
        if (onAbort && signal) signal.removeEventListener("abort", onAbort);
        if (left) return;
        leave();
        reject(err);
      },
    );
  });
}

/**
 * One request's full lifecycle: admission → attempts → telemetry.
 *
 * TWO CONTROLLERS, and the distinction is not incidental:
 *
 *  - `lifecycle` spans the whole call, including retries. It is aborted by the
 *    caller's signal, or by the last coalescing subscriber leaving. Once
 *    aborted, the call is over.
 *  - A FRESH controller per attempt carries that attempt's timeout. Reusing one
 *    controller across attempts would mean a timeout on attempt 1 leaves the
 *    signal permanently aborted, so attempts 2 and 3 fail instantly against an
 *    already-aborted signal — retry that only appears to retry.
 */
async function dispatch(
  url: string,
  origin: string,
  init: RequestInit,
  method: string,
  caller: string,
  timeoutMs: number,
  lifecycle: AbortController,
  callerSignal: AbortSignal | undefined,
  allowUnsafeRetry: boolean,
  scheduling: Scheduling,
  pageReadTiming?: PageReadTimingRun,
): Promise<Response> {
  // Propagate the caller's abort into the lifecycle. (For a coalesced GET only
  // the FIRST subscriber reaches here; later ones are handled in
  // `subscribeToInFlight`, which must not abort the shared request.)
  // The listener has to be REMOVED on the way out. `{ once: true }` only fires
  // it once; it does not detach it when the request finishes without aborting.
  // A long-lived caller signal — one `AbortController` reused across a screen's
  // lifetime, which is exactly how the Reefs fan-out uses it — would otherwise
  // accumulate one dead listener per request for as long as that signal lives.
  let detachCallerAbort: (() => void) | null = null;
  if (callerSignal) {
    if (callerSignal.aborted) {
      lifecycle.abort();
    } else {
      const onCallerAbort = () => lifecycle.abort();
      callerSignal.addEventListener("abort", onCallerAbort, { once: true });
      detachCallerAbort = () => callerSignal.removeEventListener("abort", onCallerAbort);
    }
  }

  let lastErr: unknown = null;

  try {
  for (let attempt = 1; attempt <= config.maxAttempts; attempt++) {
    if (!breakerAdmits(origin)) {
      emit({ caller, method, status: null, queuedMs: 0, durationMs: 0, attempt, outcome: "breaker-open" });
      throw new TransportError("breaker-open", caller, `circuit open for ${origin}; not dispatching`);
    }

    let slot: Slot;
    // Measured around `acquire` so a `queue-timeout` reports the wait that
    // actually caused it. Reporting 0 here meant the W6 dashboard showed no
    // queue pressure precisely when queue pressure was the failure.
    const acquireStartedAt = Date.now();
    try {
      slot = await acquire(caller, scheduling, lifecycle.signal);
    } catch (e) {
      // Admission already took the half-open probe token above; this path
      // never reaches a breaker outcome, so hand it back or the breaker
      // wedges open forever.
      breakerReleaseProbe(origin);
      const kind = e instanceof TransportError ? e.kind : "network";
      emit({
        caller,
        method,
        status: null,
        queuedMs: Date.now() - acquireStartedAt,
        durationMs: 0,
        attempt,
        outcome: kind,
      });
      throw e;
    }

    const attemptCtl = new AbortController();
    const linkAbort = () => attemptCtl.abort();
    if (lifecycle.signal.aborted) attemptCtl.abort();
    else lifecycle.signal.addEventListener("abort", linkAbort, { once: true });

    let timedOut = false;
    // Armed only AFTER the slot is held, so queue time is never charged
    // against the request's own budget.
    const timer = setTimeout(() => {
      timedOut = true;
      attemptCtl.abort();
    }, timeoutMs);

    const wasBackground = scheduling.priority === "background";
    if (wasBackground) backgroundAttempts.add(attemptCtl);
    const startedAt = Date.now();
    // Set when this attempt should be retried; the sleep happens AFTER the
    // `finally` has released the slot, so a backoff never occupies capacity.
    let retryWait: number | null = null;

    try {
      const attemptHeaders = new Headers(init.headers);
      attemptHeaders.set("x-coralswarm-priority", scheduling.priority);
      const timingAttempt = process.env.NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS === "1"
        ? dispatchPageReadTiming(pageReadTiming, url, method, scheduling.priority, attemptHeaders.get("Authorization")) : undefined;
      const res = await fetch(url, { ...init, headers: attemptHeaders,
        ...(scheduling.priority === "background" ? { priority: "low" as const } : {}),
        signal: attemptCtl.signal });
      if (process.env.NEXT_PUBLIC_E2E_PAGE_READ_RECEIPTS === "1") responsePageReadTiming(timingAttempt, res);
      const durationMs = Date.now() - startedAt;

      if (isRetryableStatus(res.status, method, allowUnsafeRetry)) {
        breakerOnFailure(origin);
        emit({ caller, method, status: res.status, queuedMs: slot.queuedMs, durationMs, attempt, outcome: "http-error", serverTiming: serverTimingOf(res) });
        const askedFor = retryAfterMs(res);
        if (askedFor !== null && askedFor > MAX_RETRY_AFTER_MS) {
          // The server wants us gone for longer than any request should wait.
          // Honour the INTENT — stop retrying — rather than the duration:
          // sleeping a clamped 30s would just get refused again, and sleeping
          // the full hour hangs the caller. Hand the response back so
          // `lib/api.ts` surfaces the 429/503 and the caller decides.
          breakerOnFailure(origin);
          void res.body?.cancel?.().catch(() => {});
          return res;
        }
        if (attempt < config.maxAttempts) {
          retryWait = askedFor ?? backoffMs(attempt);
          // Release the discarded body so the connection can be reused rather
          // than held until GC.
          void res.body?.cancel?.().catch(() => {});
        } else {
          // Out of attempts: hand the real response back so `lib/api.ts` can
          // surface the server's own error body and status.
          return res;
        }
      } else {
        // A 5xx or 429 is NOT the server functioning correctly, even when we
        // have decided not to retry it. Method-gating the retry made this
        // reachable: a POST that gets a 503 is now non-retryable and falls
        // here, and treating that as a success would RESET
        // `consecutiveFailures` — so a stream of failing writes would hold
        // the breaker permanently closed, which is the opposite of what it
        // is for. Record the outcome by what the server said, not by whether
        // we chose to try again.
        if (res.status >= 500 || res.status == 429) {
          breakerOnFailure(origin);
        } else {
          breakerOnSuccess(origin);
        }
        emit({
          caller,
          method,
          status: res.status,
          queuedMs: slot.queuedMs,
          durationMs,
          attempt,
          outcome: res.ok ? "ok" : "http-error",
          serverTiming: serverTimingOf(res),
        });
        return res;
      }
    } catch (e) {
      const durationMs = Date.now() - startedAt;
      // "The caller changed their mind" is not a transport failure and must
      // not count against the breaker — otherwise normal navigation trips it.
      if (lifecycle.signal.aborted && !timedOut) {
        // A caller changing their mind is not evidence about the origin, so
        // this deliberately records no breaker outcome — which means it must
        // return the probe token explicitly.
        breakerReleaseProbe(origin);
        emit({ caller, method, status: null, queuedMs: slot.queuedMs, durationMs, attempt, outcome: "aborted" });
        throw new TransportError("aborted", caller, "request aborted");
      }
      if (wasBackground && attemptCtl.signal.aborted && !timedOut) {
        breakerReleaseProbe(origin);
        emit({ caller, method, status: null, queuedMs: slot.queuedMs, durationMs, attempt, outcome: "aborted" });
        attempt--; // scheduling preemption is neither a failure nor backoff
        continue;
      }
      breakerOnFailure(origin);
      const kind: TransportFailureKind = timedOut ? "timeout" : "network";
      emit({ caller, method, status: null, queuedMs: slot.queuedMs, durationMs, attempt, outcome: kind });
      lastErr =
        kind === "timeout"
          ? new TransportError("timeout", caller, `request exceeded ${timeoutMs}ms`)
          : new TransportError("network", caller, e instanceof Error ? e.message : String(e));
      if (attempt < config.maxAttempts && isRetryableFailure(method, allowUnsafeRetry)) {
        retryWait = backoffMs(attempt);
      } else {
        throw lastErr;
      }
    } finally {
      backgroundAttempts.delete(attemptCtl);
      clearTimeout(timer);
      lifecycle.signal.removeEventListener("abort", linkAbort);
      slot.release();
    }

    if (retryWait !== null) await sleep(retryWait, lifecycle.signal);
  }

  throw lastErr ?? new TransportError("network", caller, "request failed");
  } finally {
    detachCallerAbort?.();
  }
}

// ── Test seam ───────────────────────────────────────────────────────────────
//
// Module-level mutable state (semaphore counters, breakers, coalescing map) is
// deliberate — there is exactly one browser tab and exactly one origin, so a
// singleton is the honest model. The cost is that tests must be able to reset
// it, which is what these are for. Not exported from `lib/api.ts`.

export function __setTransportConfigForTests(patch: Partial<TransportConfig>): void {
  config = { ...config, ...patch };
}

export function __resetTransportForTests(): void {
  config = { ...DEFAULT_TRANSPORT_CONFIG };
  pageLoadKey = "";
  pageLoadSettled = false;
  criticalRequests = 0;
  criticalListeners.forEach((listener) => listener());
  backgroundAttempts.clear();
  inFlightTotal = 0;
  inFlightByCaller.clear();
  queuedByCaller.clear();
  for (const w of queue) {
    if (w.timer !== null) clearTimeout(w.timer);
    w.detachAbort?.();
    // REJECT rather than abandon. Dropping a waiter leaves its promise
    // unsettled, which turns a reset-while-work-is-queued into a HUNG test
    // instead of a failing one — the worst possible diagnostic.
    w.reject(new TransportError("aborted", w.caller, "transport reset"));
  }
  queue = [];
  seqCounter = 0;
  breakers.clear();
  // Abort the shared controllers too, or a reset leaves real requests running
  // against a torn-down fixture.
  for (const entry of inFlightGets.values()) entry.controller.abort();
  inFlightGets.clear();
  sink = null;
}

export function __transportStatsForTests(): {
  inFlight: number;
  queued: number;
  inFlightByCaller: Record<string, number>;
} {
  return {
    inFlight: inFlightTotal,
    queued: queue.length,
    inFlightByCaller: Object.fromEntries(inFlightByCaller),
  };
}
