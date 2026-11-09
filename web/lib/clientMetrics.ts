// W6 — the client half of the metrics pipeline.
//
// Plan: docs/plans/request-reliability-and-scale.md §6.6.
//
// WHY THIS EXISTS AT ALL. The failure that motivated the whole plan —
// `net::ERR_INSUFFICIENT_RESOURCES` — never reached the server. The browser
// refused to open the socket. No server-side metric could ever have shown it,
// which is exactly why a user found the incident and a monitor did not. The
// same is true of a client-side timeout, a request refused by W1's queue, and
// a request the circuit breaker declined to send. Those outcomes only exist
// here, so they have to be reported from here.
//
// WHY IT POSTS TO OUR OWN BACKEND rather than to a collector. The browser has
// no collector credentials, and shipping any in a public bundle would be
// handing them out. `POST /v1/telemetry/client` rides the session the user
// already holds, so reports are attributable and already covered by the
// server's per-user rate ceiling.
//
// WHAT IS NEVER SENT: no URLs with ids, no query strings, no titles, no atom
// or reef content, no tokens. Only route TEMPLATES, methods, status codes,
// durations and an outcome enum — all of which `lib/transport.ts` already
// produces in that form. The server re-derives every label anyway (it cannot
// trust a client-supplied metric dimension), but sending clean data is the
// primary control, not a fallback.

import type { TransportEvent } from "./transport";
import { setTransportSink } from "./transport";
import { apiBase } from "./apiBase";
import { CLIENT_HEADER, clientName } from "./clientHeader";

/** Flush when this many events have queued. */
const BATCH_SIZE = 50;
/** Or when this long has passed, whichever comes first. */
const FLUSH_INTERVAL_MS = 10_000;
/**
 * Hard ceiling on the buffer. If the backend is unreachable — which is
 * precisely when the most interesting events are being generated — the buffer
 * must not grow without bound. Telemetry about an outage must not become part
 * of the outage.
 */
const MAX_BUFFERED = 500;
/**
 * Hard bound on a single flush. Chosen well above any plausible round trip:
 * the goal is not to make telemetry fast, it is to guarantee `flushing` is
 * always released so the reporter cannot wedge.
 */
const FLUSH_TIMEOUT_MS = 10_000;

interface WireSample {
  route: string;
  method: string;
  status?: number;
  duration_ms: number;
  queued_ms?: number;
  outcome: string;
  server_total_ms?: number;
  server_auth_ms?: number;
  server_app_ms?: number;
  server_tx_open_ms?: number;
}

let buffer: WireSample[] = [];
let timer: ReturnType<typeof setTimeout> | null = null;
let tokenProvider: (() => string | null) | null = null;
/** Set while a flush is in flight, so overlapping flushes cannot duplicate. */
let flushing = false;
let dropped = 0;

function toWire(e: TransportEvent): WireSample {
  return {
    // `caller` is already a route template (`/v1/nodes/{id}/atoms`) — see
    // `routeTemplate` in transport.ts, which templates precisely so ids never
    // reach this layer in the first place.
    route: e.caller,
    method: e.method,
    ...(e.status !== null ? { status: e.status } : {}),
    duration_ms: Math.round(e.durationMs),
    ...(e.queuedMs > 0 ? { queued_ms: Math.round(e.queuedMs) } : {}),
    outcome: e.outcome,
    ...serverTimingWire(e),
  };
}

function serverTimingWire(e: TransportEvent): Partial<WireSample> {
  const t = e.serverTiming;
  if (!t) return {};
  const out: Partial<WireSample> = {};
  if (t.totalMs !== undefined) out.server_total_ms = Math.round(t.totalMs);
  if (t.authMs !== undefined) out.server_auth_ms = Math.round(t.authMs);
  if (t.appMs !== undefined) out.server_app_ms = Math.round(t.appMs);
  if (t.txOpenMs !== undefined) out.server_tx_open_ms = Math.round(t.txOpenMs);
  return out;
}

/**
 * Never report on the reporting endpoint.
 *
 * Without this, a failing telemetry POST generates a transport event, which
 * queues another report, which fails, which generates another event. That is
 * an infinite feedback loop that gets *faster* the worse things are — the same
 * class of bug as the Reefs re-fire loop this plan exists to fix, and it would
 * be embarrassing to reintroduce it in the code that watches for it.
 */
const SELF_ROUTE = "/v1/telemetry/client";

function schedule(): void {
  if (timer !== null) return;
  timer = setTimeout(() => {
    timer = null;
    void flush();
  }, FLUSH_INTERVAL_MS);
}

async function flush(): Promise<void> {
  if (flushing || buffer.length === 0) return;
  const token = tokenProvider?.() ?? null;
  // Unauthenticated: hold the events rather than dropping them — a report
  // during sign-in is still a real measurement, and MAX_BUFFERED bounds the
  // wait.
  if (!token) {
    schedule();
    return;
  }

  flushing = true;
  const batch = buffer;
  buffer = [];
  const droppedNow = dropped;
  dropped = 0;
  let delivered = false;

  try {
    // Deliberately a BARE fetch, not the guarded `req()`. Three reasons, all
    // of them about not letting the observer perturb the observed:
    //   1. It must not consume one of W1's six request slots — telemetry
    //      would then compete with the user's own screens for capacity.
    //   2. It must not appear in its own metrics.
    //   3. It must not retry. A retrying reporter during an outage is an
    //      amplifier pointed at a service that is already struggling.
    await fetch(`${apiBase()}${SELF_ROUTE}`, {
      method: "POST",
      keepalive: true,
      // BOUNDED. `flushing` is cleared only in `finally`, so a request that
      // never settles — captive portal, stalled TCP, precisely the network
      // conditions this pipeline exists to observe — would leave `flushing`
      // true forever. Every later flush would return early, the buffer would
      // fill to MAX_BUFFERED, and the session would silently report nothing
      // from then on. The observer must not be the thing that goes dark.
      signal: AbortSignal.timeout(FLUSH_TIMEOUT_MS),
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
        [CLIENT_HEADER]: clientName(),
      },
      body: JSON.stringify({
        samples: batch,
        ...(droppedNow > 0 ? { dropped: droppedNow } : {}),
      }),
    });
    delivered = true;
  } catch {
    // Swallowed on purpose. Telemetry that can fail a user action, or even
    // surface an error, is worse than no telemetry. The batch is gone; the
    // next window will carry fresh data.
  } finally {
    flushing = false;
    // The dropped count was zeroed BEFORE the request. If the request failed,
    // that erased the very evidence of a gap the count exists to record — and
    // a failed flush is exactly when data is being lost. Carry it forward
    // instead, so the number reaches the server eventually or not at all,
    // never silently vanishes.
    if (!delivered) dropped += droppedNow;
    if (buffer.length > 0) schedule();
  }
}

/**
 * Start reporting. Idempotent-ish: calling it again replaces the token
 * provider and keeps the existing buffer.
 *
 * `getToken` is a getter rather than a token so this module never holds a
 * stale bearer — Clerk session tokens live ~60s.
 */
export function startClientMetrics(getToken: () => string | null): () => void {
  tokenProvider = getToken;

  const prev = setTransportSink((event) => {
    if (event.caller === SELF_ROUTE) return; // see SELF_ROUTE's comment
    if (buffer.length >= MAX_BUFFERED) {
      // Drop the OLDEST, keep the newest: during an incident the most recent
      // events describe what is happening now, and `dropped` is reported so
      // the gap is visible rather than silent.
      buffer.shift();
      dropped += 1;
    }
    buffer.push(toWire(event));
    if (buffer.length >= BATCH_SIZE) void flush();
    else schedule();
  });

  // A best-effort final flush. `visibilitychange` rather than `unload`:
  // `unload` never fires on mobile Safari and is deprecated, and this is the
  // difference between seeing a session's last errors and losing exactly the
  // events that describe why the user left.
  const onHidden = () => {
    if (document.visibilityState === "hidden") void flush();
  };
  if (typeof document !== "undefined") {
    document.addEventListener("visibilitychange", onHidden);
  }

  return () => {
    setTransportSink(prev);
    if (typeof document !== "undefined") {
      document.removeEventListener("visibilitychange", onHidden);
    }
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
    tokenProvider = null;
  };
}

// ── Test seam ───────────────────────────────────────────────────────────────

export function __clientMetricsStateForTests(): {
  buffered: number;
  dropped: number;
} {
  return { buffered: buffer.length, dropped };
}

export function __resetClientMetricsForTests(): void {
  buffer = [];
  dropped = 0;
  flushing = false;
  tokenProvider = null;
  if (timer !== null) {
    clearTimeout(timer);
    timer = null;
  }
}

export const __testConstants = { BATCH_SIZE, MAX_BUFFERED, SELF_ROUTE };
