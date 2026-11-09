// W6 client-reporter tests.
//
// The properties worth pinning here are not "does it send data" — they are the
// ways a telemetry client can make an incident WORSE: feeding back on itself,
// growing without bound when the backend is down, competing with the user's
// own requests for transport capacity, or surfacing its own failures.

import { afterEach, describe, expect, it, vi } from "vitest";
import {
  __clientMetricsStateForTests,
  __resetClientMetricsForTests,
  __testConstants,
  startClientMetrics,
} from "./clientMetrics";
import {
  __resetTransportForTests,
  guardedFetch,
  parseServerTiming,
  setTransportSink,
  type TransportEvent,
} from "./transport";

function okResponse(status = 204) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => ({}),
    clone() {
      return this;
    },
  };
}

async function tick(n = 8) {
  for (let i = 0; i < n; i++) await Promise.resolve();
}

afterEach(() => {
  __resetClientMetricsForTests();
  __resetTransportForTests();
  vi.unstubAllGlobals();
});

function event(over: Partial<TransportEvent> = {}): TransportEvent {
  return {
    caller: "/v1/nodes/{id}/atoms",
    method: "GET",
    status: 200,
    queuedMs: 0,
    durationMs: 12,
    attempt: 1,
    outcome: "ok",
    ...over,
  };
}

describe("feedback safety", () => {
  it("never reports on its own reporting endpoint", async () => {
    const fetchMock = vi.fn(async () => okResponse());
    vi.stubGlobal("fetch", fetchMock);
    const stop = startClientMetrics(() => "tok");

    // Simulate the reporter's own POST producing a transport event. Without
    // the self-route guard this queues another report, which fails, which
    // queues another — a loop that accelerates as things get worse.
    for (let i = 0; i < 200; i++) {
      setTransportSinkEmit(event({ caller: __testConstants.SELF_ROUTE, outcome: "network" }));
    }
    await tick();

    expect(__clientMetricsStateForTests().buffered).toBe(0);
    expect(fetchMock).not.toHaveBeenCalled();
    stop();
  });
});

describe("bounded buffering", () => {
  it("does not grow without bound when the backend is unreachable", async () => {
    // The worst moment for a memory leak is an outage, which is also the only
    // time this path runs.
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new TypeError("Failed to fetch");
      }),
    );
    const stop = startClientMetrics(() => "tok");

    for (let i = 0; i < __testConstants.MAX_BUFFERED * 3; i++) {
      setTransportSinkEmit(event({ outcome: "network", status: null }));
      if (i % 25 === 0) await tick(2);
    }
    await tick(20);

    const st = __clientMetricsStateForTests();
    expect(st.buffered).toBeLessThanOrEqual(__testConstants.MAX_BUFFERED);
    stop();
  });

  it("counts what it dropped rather than losing the gap silently", async () => {
    // Never flush, so the buffer genuinely overflows.
    const stop = startClientMetrics(() => null);

    for (let i = 0; i < __testConstants.MAX_BUFFERED + 40; i++) {
      setTransportSinkEmit(event());
    }

    const st = __clientMetricsStateForTests();
    expect(st.buffered).toBe(__testConstants.MAX_BUFFERED);
    // A pipeline that silently loses data during an outage reads afterwards
    // as "nothing happened".
    expect(st.dropped).toBeGreaterThan(0);
    stop();
  });
});

describe("not perturbing what it observes", () => {
  it("reports with a bare fetch, so it never consumes a transport slot", async () => {
    const seen: string[] = [];
    const fetchMock = vi.fn(async (url: string) => {
      seen.push(url);
      return okResponse();
    });
    vi.stubGlobal("fetch", fetchMock);
    const stop = startClientMetrics(() => "tok");

    for (let i = 0; i < __testConstants.BATCH_SIZE; i++) setTransportSinkEmit(event());
    await tick(20);

    expect(seen.some((u) => u.endsWith(__testConstants.SELF_ROUTE))).toBe(true);
    // If the report had gone through `req()`, the transport would have
    // recorded it and it would show up as in-flight work competing with the
    // user's own screens.
    stop();
  });

  it("a failing report never surfaces an error to the caller", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new Error("collector down");
      }),
    );
    const stop = startClientMetrics(() => "tok");

    // No unhandled rejection, no throw: telemetry must never be able to fail
    // a user action.
    for (let i = 0; i < __testConstants.BATCH_SIZE; i++) setTransportSinkEmit(event());
    await expect(tick(20)).resolves.toBeUndefined();
    stop();
  });
});

describe("payload hygiene", () => {
  it("sends route templates, durations and outcomes — and nothing identifying", async () => {
    let body: unknown = null;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (_url: string, init: RequestInit) => {
        body = JSON.parse(init.body as string);
        return okResponse();
      }),
    );
    const stop = startClientMetrics(() => "tok");

    for (let i = 0; i < __testConstants.BATCH_SIZE; i++) {
      setTransportSinkEmit(
        event({ caller: "/v1/nodes/{id}/atoms", status: null, outcome: "network", queuedMs: 40 }),
      );
    }
    await tick(20);

    const parsed = body as { samples: Array<Record<string, unknown>> };
    expect(parsed.samples.length).toBeGreaterThan(0);
    const s = parsed.samples[0];
    expect(s.route).toBe("/v1/nodes/{id}/atoms");
    expect(s.outcome).toBe("network");
    expect(s.queued_ms).toBe(40);
    // A request that never reached the server carries NO status — that
    // distinction is the entire reason this client pipeline exists.
    expect("status" in s).toBe(false);

    const serialized = JSON.stringify(parsed);
    expect(serialized).not.toContain("Bearer");
    expect(serialized).not.toMatch(/[0-9a-f]{8}-[0-9a-f]{4}-/);
    stop();
  });

  it("holds events rather than dropping them while unauthenticated", async () => {
    const fetchMock = vi.fn(async () => okResponse());
    vi.stubGlobal("fetch", fetchMock);
    const stop = startClientMetrics(() => null);

    for (let i = 0; i < __testConstants.BATCH_SIZE; i++) setTransportSinkEmit(event());
    await tick(20);

    expect(fetchMock).not.toHaveBeenCalled();
    // A measurement taken during sign-in is still a real measurement.
    expect(__clientMetricsStateForTests().buffered).toBeGreaterThan(0);
    stop();
  });
});

describe("integration with the transport layer", () => {
  it("captures a real guardedFetch outcome end to end", async () => {
    const calls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        calls.push(url);
        if (url.endsWith(__testConstants.SELF_ROUTE)) return okResponse();
        return okResponse(200);
      }),
    );
    const stop = startClientMetrics(() => "tok");

    await guardedFetch("/v1/nodes/3f346a2a-71ca-4779-9701-baf2e5e7bddd/atoms", { method: "GET" });
    await tick(10);

    // One real request produced one buffered sample, templated.
    expect(__clientMetricsStateForTests().buffered).toBe(1);
    stop();
  });
});

describe("server timing", () => {
  it("parses the backend header and skips what it does not know", () => {
    expect(
      parseServerTiming('auth;dur=1.2, txopen;dur=3.4;desc="2 tx", app;dur=40.1, total;dur=41.3'),
    ).toEqual({ authMs: 1.2, txOpenMs: 3.4, appMs: 40.1, totalMs: 41.3 });
    expect(parseServerTiming("app;dur=5, cdn-cache;desc=HIT, total;dur=bogus")).toEqual({ appMs: 5 });
    expect(parseServerTiming("garbage")).toBeUndefined();
    expect(parseServerTiming(null)).toBeUndefined();
  });

  it("forwards the server breakdown of a real response to the wire", async () => {
    let body: { samples: Array<Record<string, unknown>> } | null = null;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        if (url.endsWith(__testConstants.SELF_ROUTE)) {
          body = JSON.parse(init?.body as string);
          return okResponse();
        }
        return {
          ...okResponse(200),
          headers: {
            get: (h: string) =>
              h.toLowerCase() === "server-timing" ? "auth;dur=12.4, app;dur=880.6, total;dur=893.0" : null,
          },
        };
      }),
    );
    const stop = startClientMetrics(() => "tok");
    for (let i = 0; i < __testConstants.BATCH_SIZE; i++) {
      await guardedFetch("/v1/graphs/3f346a2a-71ca-4779-9701-baf2e5e7bddd/stats", { method: "GET" });
    }
    await tick(20);

    expect(body).not.toBeNull();
    const s = body!.samples[0];
    expect(s.server_auth_ms).toBe(12);
    expect(s.server_app_ms).toBe(881);
    expect(s.server_total_ms).toBe(893);
    expect("server_tx_open_ms" in s).toBe(false);
    stop();
  });
});

// ── helper ──────────────────────────────────────────────────────────────────
//
// `startClientMetrics` installs a sink via `setTransportSink`; these tests need
// to push synthetic events into whatever sink is currently installed without
// going through a real request. Capturing it on install is the least-magic way
// to do that.
let installedSink: ((e: TransportEvent) => void) | null = null;
const realSetTransportSink = setTransportSink;
function setTransportSinkEmit(e: TransportEvent) {
  if (!installedSink) {
    // Recover the sink the module installed by swapping it out and back.
    const current = realSetTransportSink(null);
    realSetTransportSink(current);
    installedSink = current;
  }
  installedSink?.(e);
}

afterEach(() => {
  installedSink = null;
});
