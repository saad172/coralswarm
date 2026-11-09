// W1 acceptance tests (docs/plans/request-reliability-and-scale.md §6.1).
//
// These assert on OBSERVED CONCURRENCY at the `fetch` boundary rather than on
// the semaphore's internal counters wherever possible — the counters are an
// implementation detail, "never more than N sockets open" is the property that
// actually prevents `ERR_INSUFFICIENT_RESOURCES`.
//
// Timings are compressed via `__setTransportConfigForTests` (millisecond caps,
// 1 ms backoff) rather than fake timers, because the code under test
// interleaves real `fetch` promises with `setTimeout`, and fake timers make the
// ordering between those two the test's problem instead of the scheduler's.

import { afterEach, describe, expect, it, vi } from "vitest";
import {
  DEFAULT_TRANSPORT_CONFIG,
  TransportError,
  __resetTransportForTests,
  __setTransportConfigForTests,
  __transportStatsForTests,
  guardedFetch,
  routeTemplate,
  setTransportSink,
  type TransportEvent,
} from "./transport";

/** A `fetch` stand-in that records peak concurrency and hands back a promise
 *  the test resolves by hand. */
function deferredFetch() {
  let live = 0;
  let peak = 0;
  const pending: Array<{ url: string; resolve: (r: unknown) => void; reject: (e: unknown) => void }> = [];

  const fetchMock = vi.fn((url: string, init?: RequestInit) => {
    live += 1;
    peak = Math.max(peak, live);
    return new Promise((resolve, reject) => {
      const entry = {
        url,
        resolve: (r: unknown) => {
          live -= 1;
          resolve(r);
        },
        reject: (e: unknown) => {
          live -= 1;
          reject(e);
        },
      };
      pending.push(entry);
      // Honour abort so cancellation tests see a real rejection.
      init?.signal?.addEventListener("abort", () => {
        const i = pending.indexOf(entry);
        if (i >= 0) pending.splice(i, 1);
        entry.reject(Object.assign(new Error("aborted"), { name: "AbortError" }));
      });
    });
  });

  vi.stubGlobal("fetch", fetchMock);
  return {
    fetchMock,
    get peak() {
      return peak;
    },
    get liveCount() {
      return pending.length;
    },
    settleAll(body: unknown = {}) {
      const batch = pending.splice(0, pending.length);
      for (const p of batch) p.resolve(okResponse(body));
    },
    settleOne(body: unknown = {}) {
      const p = pending.shift();
      p?.resolve(okResponse(body));
    },
  };
}

/** Count of `body.cancel()` calls, so a test can assert the retry path
 *  actually released the discarded body. */
let bodyCancelled = 0;

/** Models a REAL `Response`: the body can be read once, and `clone()` must be
 *  called before that read. A stub whose `clone()` returns `this` makes the
 *  coalescing test vacuous — it would pass even if every subscriber received
 *  the same already-consumed body, which is the exact bug the refcount exists
 *  to prevent. */
function okResponse(body: unknown = {}, status = 200, headers: Record<string, string> = {}) {
  let bodyRead = false;
  const make = (): Record<string, unknown> => ({
    ok: status >= 200 && status < 300,
    status,
    // A real `Response` always has a body, and `dispatch` cancels it before
    // retrying. Omitting it made that line dead in every test — optional
    // chaining short-circuited — so a regression that cancelled the body
    // eventually handed back to `lib/api.ts` would have passed the suite.
    body: { cancel: async () => { bodyCancelled += 1; } },
    // Header lookup is case-insensitive on a real Response; the stub's map is
    // written verbatim, so normalize BOTH sides or `{"Retry-After": "1"}`
    // silently reads null.
    headers: {
      get: (k: string) => {
        const want = k.toLowerCase();
        for (const hk of Object.keys(headers)) {
          if (hk.toLowerCase() === want) return headers[hk];
        }
        return null;
      },
    },
    async json() {
      if (bodyRead) throw new TypeError("body stream already read");
      bodyRead = true;
      return body;
    },
    clone() {
      if (bodyRead) throw new TypeError("cannot clone a consumed response");
      // A real clone has its own independent body.
      return okResponse(body, status, headers);
    },
  });
  return make();
}

/** Yield to the microtask queue enough times for the scheduler to drain. */
async function tick(times = 6) {
  for (let i = 0; i < times; i++) await Promise.resolve();
}

function sleepReal(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

afterEach(() => {
  __resetTransportForTests();
  vi.unstubAllGlobals();
  bodyCancelled = 0;
});

describe("routeTemplate", () => {
  it("collapses uuids, numeric ids and long opaque segments", () => {
    expect(routeTemplate("/v1/nodes/3f346a2a-71ca-4779-9701-baf2e5e7bddd/atoms")).toBe(
      "/v1/nodes/{id}/atoms",
    );
    expect(routeTemplate("/v1/graphs/3f346a2a-71ca-4779-9701-baf2e5e7bddd/atoms?page=2&page_size=200")).toBe(
      "/v1/graphs/{id}/atoms",
    );
    expect(routeTemplate("/v1/meetings/42")).toBe("/v1/meetings/{id}");
  });

  it("leaves real route words intact so distinct routes stay distinct", () => {
    expect(routeTemplate("/v1/nodes/3f346a2a-71ca-4779-9701-baf2e5e7bddd/candidates")).toBe(
      "/v1/nodes/{id}/candidates",
    );
    expect(routeTemplate("/v1/graphs")).toBe("/v1/graphs");
  });

  it("never carries a query string into the metric dimension", () => {
    // Query strings can contain ocean ids and free-text search terms; letting
    // them through would make this both high-cardinality and a content leak.
    expect(routeTemplate("/v1/nodes?graph_id=abc&page=3")).toBe("/v1/nodes");
  });
});

describe("(a) global concurrency ceiling", () => {
  it("never exceeds maxInFlight sockets no matter how many callers pile on", async () => {
    const f = deferredFetch();
    // 300 requests across 300 DISTINCT route templates (`/v1/probe7` stays
    // literal — not a uuid, not numeric, under the opaque-segment threshold),
    // so the per-caller quota is NOT what limits this. The global ceiling is
    // the only thing standing between these and 300 open sockets.
    const calls = Array.from({ length: 300 }, (_, i) =>
      guardedFetch(`/v1/probe${i}`, { method: "GET" }).catch(() => null),
    );
    await tick(20);

    expect(f.peak).toBeLessThanOrEqual(DEFAULT_TRANSPORT_CONFIG.maxInFlight);
    expect(__transportStatsForTests().inFlight).toBeLessThanOrEqual(
      DEFAULT_TRANSPORT_CONFIG.maxInFlight,
    );

    for (let i = 0; i < 500 && f.liveCount > 0; i++) {
      f.settleAll();
      await tick(4);
    }
    await Promise.allSettled(calls);
    expect(f.peak).toBeLessThanOrEqual(DEFAULT_TRANSPORT_CONFIG.maxInFlight);
  });
});

describe("(f) per-caller quota — the anti-starvation property", () => {
  it("a 500-deep fan-out from one route cannot lock another route out", async () => {
    const f = deferredFetch();

    // The Reefs enrichment shape: 500 requests to 500 different reefs, all
    // templating to ONE caller (`/v1/nodes/{id}/atoms`). Before W1 this is
    // what produced ERR_INSUFFICIENT_RESOURCES. The ids must be well-formed
    // uuids or `routeTemplate` leaves them literal and they stop being one
    // caller, which is the whole thing under test.
    const reefId = (i: number) => `3f346a2a-71ca-4779-9701-${String(i).padStart(12, "0")}`;
    const flood = Array.from({ length: 500 }, (_, i) =>
      guardedFetch(`/v1/nodes/${reefId(i)}/atoms`, { method: "GET" }).catch(() => null),
    );
    await tick(20);

    const held = __transportStatsForTests().inFlightByCaller["/v1/nodes/{id}/atoms"] ?? 0;
    expect(held).toBeLessThanOrEqual(DEFAULT_TRANSPORT_CONFIG.maxPerCallerInFlight);

    // A different screen now asks for one thing. It must NOT wait behind 500.
    const other = guardedFetch("/v1/graphs", { method: "GET" }).catch((e) => e);
    await tick(20);

    const otherHeld = __transportStatsForTests().inFlightByCaller["/v1/graphs"] ?? 0;
    expect(otherHeld).toBe(1);

    for (let i = 0; i < 400 && f.liveCount > 0; i++) {
      f.settleAll();
      await tick(4);
    }
    await Promise.allSettled([...flood, other]);
  });

  it("rejects at enqueue past maxPerCallerQueued instead of growing without bound", async () => {
    const f = deferredFetch();
    __setTransportConfigForTests({ maxPerCallerQueued: 4 });

    // All 40 template to one caller. 3 run (per-caller cap), 4 queue, and the
    // remaining 33 must be REFUSED at enqueue — an unbounded queue is the same
    // bug wearing a different hat.
    const outcomes = Array.from({ length: 40 }, (_, i) =>
      guardedFetch(`/v1/nodes/${i}/atoms`, { method: "GET" }).then(
        () => "ok" as const,
        (e: unknown) => e,
      ),
    );
    await tick(20);

    for (let i = 0; i < 100 && f.liveCount > 0; i++) {
      f.settleAll();
      await tick(4);
    }
    const results = await Promise.all(outcomes);

    const rejected = results.filter((r): r is TransportError => r instanceof TransportError);
    expect(rejected.length).toBe(40 - DEFAULT_TRANSPORT_CONFIG.maxPerCallerInFlight - 4);
    expect(rejected.every((e) => e.kind === "queue-full")).toBe(true);
  });
});

describe("(g)/(h) queue exit paths", () => {
  it("an aborted queued request leaves the queue immediately and frees no slot", async () => {
    const f = deferredFetch();
    const ac = new AbortController();

    // Fill every slot with a different caller so nothing is quota-blocked.
    const holders = Array.from({ length: DEFAULT_TRANSPORT_CONFIG.maxInFlight }, (_, i) =>
      guardedFetch(`/v1/hold${i}`, { method: "GET" }).catch(() => null),
    );
    await tick(20);
    expect(__transportStatsForTests().inFlight).toBe(DEFAULT_TRANSPORT_CONFIG.maxInFlight);

    const queued = guardedFetch("/v1/graphs", { method: "GET" }, { signal: ac.signal }).catch(
      (e) => e,
    );
    await tick(10);
    expect(__transportStatsForTests().queued).toBe(1);

    ac.abort();
    const err = await queued;
    expect(err).toBeInstanceOf(TransportError);
    expect((err as TransportError).kind).toBe("aborted");
    // The whole point: the entry is GONE, not merely ignored. `reefs/page.tsx`
    // today sets `cancelled = true` and the request runs on regardless.
    expect(__transportStatsForTests().queued).toBe(0);

    f.settleAll();
    await tick(10);
    await Promise.allSettled(holders);
  });

  it("rejects with queue-timeout after the wait cap, distinctly from a request timeout", async () => {
    const f = deferredFetch();
    __setTransportConfigForTests({ queueWaitCapMs: 20, requestTimeoutMs: 10_000 });

    const holders = Array.from({ length: DEFAULT_TRANSPORT_CONFIG.maxInFlight }, (_, i) =>
      guardedFetch(`/v1/hold${i}`, { method: "GET" }).catch(() => null),
    );
    await tick(20);

    const queued = guardedFetch("/v1/graphs", { method: "GET" }).catch((e) => e);
    const err = await queued;

    expect(err).toBeInstanceOf(TransportError);
    // `queue-timeout`, NOT `timeout`: a request that waited for a slot and a
    // request the server was slow to answer are different product outcomes and
    // must be separable in the W6 failure taxonomy.
    expect((err as TransportError).kind).toBe("queue-timeout");

    f.settleAll();
    await tick(10);
    await Promise.allSettled(holders);
  });
});

describe("(b) request timeout", () => {
  it("aborts the in-flight request and surfaces a typed timeout after exhausting attempts", async () => {
    deferredFetch();
    __setTransportConfigForTests({ requestTimeoutMs: 10, retryBaseMs: 1, retryMaxMs: 2 });

    const err = await guardedFetch("/v1/graphs", { method: "GET" }).catch((e) => e);

    expect(err).toBeInstanceOf(TransportError);
    expect((err as TransportError).kind).toBe("timeout");
  });

  it("does not charge queue time against the request budget", async () => {
    const f = deferredFetch();
    const events: TransportEvent[] = [];
    setTransportSink((e) => events.push(e));
    __setTransportConfigForTests({ requestTimeoutMs: 10_000, queueWaitCapMs: 10_000 });

    const holders = Array.from({ length: DEFAULT_TRANSPORT_CONFIG.maxInFlight }, (_, i) =>
      guardedFetch(`/v1/hold${i}`, { method: "GET" }).catch(() => null),
    );
    await tick(20);

    const queued = guardedFetch("/v1/graphs", { method: "GET" });
    await sleepReal(30);
    f.settleAll(); // frees slots; the queued request now runs
    await tick(20);
    f.settleAll();
    await queued;

    const ev = events.find((e) => e.caller === "/v1/graphs");
    if (!ev) throw new Error("no transport event for /v1/graphs");
    // The real property: it waited a measurable time, and the request's own
    // duration is BELOW that wait — i.e. queue time was not folded in.
    // `durationMs < queuedMs + 1000` (the earlier form) held for essentially
    // any outcome, including the regression it was meant to catch.
    expect(ev.queuedMs).toBeGreaterThanOrEqual(25);
    expect(ev.durationMs).toBeLessThan(ev.queuedMs);

    await Promise.allSettled(holders);
  });
});

describe("(c) retry policy", () => {
  it("stops at maxAttempts on a retryable status and returns the final response", async () => {
    const fetchMock = vi.fn(async () => okResponse({ error: "busy" }, 503));
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 2 });

    const res = await guardedFetch("/v1/graphs", { method: "GET" });

    expect(fetchMock).toHaveBeenCalledTimes(DEFAULT_TRANSPORT_CONFIG.maxAttempts);
    expect(res.status).toBe(503);
  });

  it("never retries a non-429 4xx — repeating a rejected request is pure amplification", async () => {
    const fetchMock = vi.fn(async () => okResponse({ error: "nope" }, 403));
    vi.stubGlobal("fetch", fetchMock);

    const res = await guardedFetch("/v1/graphs", { method: "GET" });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(res.status).toBe(403);
  });

  it("honours Retry-After over its own backoff", async () => {
    let calls = 0;
    const stamps: number[] = [];
    const fetchMock = vi.fn(async () => {
      stamps.push(Date.now());
      calls += 1;
      return calls === 1 ? okResponse({}, 429, { "retry-after": "0.05" }) : okResponse({});
    });
    vi.stubGlobal("fetch", fetchMock);
    // Backoff base is 1 ms; if Retry-After were ignored the gap would be ~1 ms.
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 1 });

    const res = await guardedFetch("/v1/graphs", { method: "GET" });

    expect(res.status).toBe(200);
    expect(stamps.length).toBe(2);
    expect(stamps[1] - stamps[0]).toBeGreaterThanOrEqual(40);
  });

  it("holds no slot while backing off", async () => {
    let calls = 0;
    let liveDuringBackoff = -1;
    const fetchMock = vi.fn(async () => {
      calls += 1;
      if (calls === 1) return okResponse({}, 503);
      liveDuringBackoff = __transportStatsForTests().inFlight;
      return okResponse({});
    });
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ retryBaseMs: 5, retryMaxMs: 5 });

    await guardedFetch("/v1/graphs", { method: "GET" });

    // On attempt 2 exactly one slot is held (this request's own). If the
    // backoff had held the slot too, capacity would have been occupied by a
    // request that was doing nothing but sleeping.
    expect(liveDuringBackoff).toBe(1);
  });
});

describe("(d) in-flight GET coalescing", () => {
  it("collapses N identical concurrent GETs into exactly one network call", async () => {
    const f = deferredFetch();

    const all = Promise.all(
      Array.from({ length: 8 }, () =>
        guardedFetch("/v1/nodes/3f346a2a-71ca-4779-9701-baf2e5e7bddd/atoms", {
          method: "GET",
          headers: { Authorization: "Bearer t1" },
        }),
      ),
    );
    await tick(10);

    expect(f.fetchMock).toHaveBeenCalledTimes(1);

    f.settleAll({ ok: true });
    const responses = await all;
    expect(responses).toHaveLength(8);
  });

  it("does not share a response across identities", async () => {
    const f = deferredFetch();

    const a = guardedFetch("/v1/graphs", { method: "GET", headers: { Authorization: "Bearer aaa" } });
    const b = guardedFetch("/v1/graphs", { method: "GET", headers: { Authorization: "Bearer bbb" } });
    await tick(10);

    // Two users asking for "their graphs" is not one request. Coalescing on
    // path alone would serve one user the other's data.
    expect(f.fetchMock).toHaveBeenCalledTimes(2);

    f.settleAll();
    await Promise.all([a, b]);
  });

  it("never coalesces a mutation", async () => {
    const f = deferredFetch();

    const a = guardedFetch("/v1/nodes/x", { method: "PATCH", body: "{}" }).catch(() => null);
    const b = guardedFetch("/v1/nodes/x", { method: "PATCH", body: "{}" }).catch(() => null);
    await tick(10);

    expect(f.fetchMock).toHaveBeenCalledTimes(2);
    f.settleAll();
    await Promise.all([a, b]);
  });

  it("one subscriber aborting does not cancel the request the others are waiting on", async () => {
    const f = deferredFetch();
    const ac = new AbortController();

    const aborted = guardedFetch("/v1/graphs", { method: "GET" }, { signal: ac.signal }).catch(
      (e) => e,
    );
    const survivor = guardedFetch("/v1/graphs", { method: "GET" });
    await tick(10);
    expect(f.fetchMock).toHaveBeenCalledTimes(1);

    ac.abort();
    const err = await aborted;
    expect((err as TransportError).kind).toBe("aborted");

    f.settleAll({ value: 1 });
    const res = await survivor;
    expect(res.status).toBe(200);
  });
});

describe("(e) circuit breaker", () => {
  it("opens after consecutive transport failures, then recovers on a half-open probe", async () => {
    let mode: "fail" | "ok" = "fail";
    const fetchMock = vi.fn(async () => {
      if (mode === "fail") throw new TypeError("Failed to fetch");
      return okResponse({});
    });
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({
      retryBaseMs: 1,
      retryMaxMs: 1,
      breakerThreshold: 3,
      breakerCooldownMs: 30,
      maxAttempts: 1,
    });

    for (let i = 0; i < 3; i++) {
      await guardedFetch("/v1/graphs", { method: "GET" }).catch(() => null);
    }
    const callsBeforeOpen = fetchMock.mock.calls.length;

    const blocked = await guardedFetch("/v1/graphs", { method: "GET" }).catch((e) => e);
    expect(blocked).toBeInstanceOf(TransportError);
    expect((blocked as TransportError).kind).toBe("breaker-open");
    // Open means it did not touch the network at all.
    expect(fetchMock.mock.calls.length).toBe(callsBeforeOpen);

    mode = "ok";
    await sleepReal(40);
    const probe = await guardedFetch("/v1/graphs", { method: "GET" });
    expect(probe.status).toBe(200);

    // Recovered: subsequent calls flow normally again.
    const after = await guardedFetch("/v1/graphs", { method: "GET" });
    expect(after.status).toBe(200);
  });

  it("a 404 is the server working and must not trip the breaker", async () => {
    const fetchMock = vi.fn(async () => okResponse({ error: "not found" }, 404));
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ breakerThreshold: 2 });

    for (let i = 0; i < 5; i++) {
      const res = await guardedFetch(`/v1/nodes/missing${i}`, { method: "GET" });
      expect(res.status).toBe(404);
    }
    expect(fetchMock).toHaveBeenCalledTimes(5);
  });
});

describe("(c2) retries must not duplicate writes — CodeRabbit Critical", () => {
  it("does NOT retry a POST on 503: the write may already have landed", async () => {
    // The browser cannot distinguish "never arrived" from "arrived, response
    // lost". Replaying a mutation on an ambiguous failure creates duplicate
    // team invites, duplicate atoms, duplicate query rows — and nothing
    // upstream deduplicates.
    const fetchMock = vi.fn(async () => okResponse({ error: "busy" }, 503));
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 2 });

    const res = await guardedFetch("/v1/nodes", { method: "POST", body: "{}" });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(res.status).toBe(503);
  });

  it("does NOT retry a POST on a network error", async () => {
    // The most ambiguous case of all: the request definitely left the browser.
    const fetchMock = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 2 });

    await guardedFetch("/v1/nodes", { method: "POST", body: "{}" }).catch(() => null);

    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("DOES retry a POST on 429 — the server said it did not act", async () => {
    // 429 is categorically different: an explicit refusal, so replaying is
    // safe for any method.
    let n = 0;
    const fetchMock = vi.fn(async () => {
      n += 1;
      return n === 1 ? okResponse({}, 429) : okResponse({});
    });
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 1 });

    const res = await guardedFetch("/v1/nodes", { method: "POST", body: "{}" });

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(res.status).toBe(200);
  });

  it("still retries idempotent methods on 503", async () => {
    // The fix must not silently disable retry for the safe majority.
    const fetchMock = vi.fn(async () => okResponse({}, 503));
    vi.stubGlobal("fetch", fetchMock);
    // Threshold raised out of the way: 3 methods x 3 attempts is 9 consecutive
    // failures, which would trip the default breaker (5) partway through and
    // fail the later methods for an unrelated reason.
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 2, breakerThreshold: 1000 });

    for (const method of ["GET", "PUT", "DELETE"]) {
      fetchMock.mockClear();
      await guardedFetch("/v1/probe", { method });
      expect(fetchMock, method).toHaveBeenCalledTimes(DEFAULT_TRANSPORT_CONFIG.maxAttempts);
    }
  });

  it("an explicit opt-in restores retry for a non-idempotent method", async () => {
    // For a call site that carries an idempotency key.
    const fetchMock = vi.fn(async () => okResponse({}, 503));
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 2 });

    await guardedFetch("/v1/nodes", { method: "POST", body: "{}" }, { retryNonIdempotent: true });

    expect(fetchMock).toHaveBeenCalledTimes(DEFAULT_TRANSPORT_CONFIG.maxAttempts);
  });
});

describe("(e2) the breaker must not wedge open — CodeRabbit Critical", () => {
  async function openTheBreaker() {
    for (let i = 0; i < 3; i++) {
      await guardedFetch("/v1/probe", { method: "GET" }).catch(() => null);
    }
  }

  it("an ABORTED half-open probe does not lock the origin out forever", async () => {
    // The exact reported sequence: breaker opens, cooldown elapses, the user
    // navigates away mid-probe. Before the fix `halfOpenInFlight` stayed true
    // and every later request threw `breaker-open` until a page reload.
    let mode: "fail" | "ok" = "fail";
    vi.stubGlobal(
      "fetch",
      vi.fn(async (_u: string, init?: RequestInit) => {
        if (mode === "fail") throw new TypeError("Failed to fetch");
        // Hang until aborted, so the probe can be cancelled mid-flight.
        return await new Promise((_res, rej) => {
          init?.signal?.addEventListener("abort", () =>
            rej(Object.assign(new Error("aborted"), { name: "AbortError" })),
          );
        });
      }),
    );
    __setTransportConfigForTests({
      retryBaseMs: 1,
      retryMaxMs: 1,
      breakerThreshold: 3,
      breakerCooldownMs: 20,
      maxAttempts: 1,
    });

    await openTheBreaker();
    await sleepReal(30); // cooldown elapses — next request is the probe

    mode = "ok";
    const ac = new AbortController();
    const probe = guardedFetch("/v1/probe", { method: "GET" }, { signal: ac.signal }).catch(
      (e) => e,
    );
    await tick(10);
    ac.abort(); // user navigates away mid-probe
    const err = await probe;
    expect((err as TransportError).kind).toBe("aborted");

    // The origin must still be probeable. Before the fix this threw
    // `breaker-open` forever.
    vi.stubGlobal("fetch", vi.fn(async () => okResponse({})));
    const after = await guardedFetch("/v1/probe", { method: "GET" });
    expect(after.status).toBe(200);
  });

  it("an ADMISSION-REJECTED half-open probe does not lock the origin out forever", async () => {
    // The second abandonment path: admission takes the probe token, then
    // `acquire` rejects before the request is ever dispatched — queue-full,
    // queue-timeout, or (used here) an already-aborted caller signal. All
    // three land in the same `catch`, which is the line under test.
    //
    // Note why this uses an aborted signal rather than queue saturation: the
    // breaker is per-ORIGIN, so while it is open no other request to that
    // origin is admitted, and nothing could therefore be holding the slots a
    // probe would queue behind. The queue-saturation version of this
    // scenario is unconstructible.
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        throw new TypeError("Failed to fetch");
      }),
    );
    __setTransportConfigForTests({
      retryBaseMs: 1,
      retryMaxMs: 1,
      breakerThreshold: 3,
      breakerCooldownMs: 20,
      maxAttempts: 1,
    });

    await openTheBreaker();
    await sleepReal(30); // cooldown elapses — the next request is the probe

    const pre = new AbortController();
    pre.abort();
    const rejected = await guardedFetch(
      "/v1/probe",
      { method: "GET" },
      { signal: pre.signal },
    ).catch((e) => e);
    expect((rejected as TransportError).kind).toBe("aborted");

    vi.stubGlobal("fetch", vi.fn(async () => okResponse({})));
    const after = await guardedFetch("/v1/probe", { method: "GET" });
    expect(after.status).toBe(200);
  });
});

describe("(r2) second-round review fixes", () => {
  it("a non-retryable 5xx still counts against the breaker", async () => {
    // Method-gating made this reachable: a POST that gets a 503 is no longer
    // retried, so it falls to the non-retry arm. Treating that as a SUCCESS
    // would reset `consecutiveFailures`, holding the breaker permanently
    // closed while every write failed — the opposite of its purpose.
    const fetchMock = vi.fn(async () => okResponse({}, 503));
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ breakerThreshold: 3, breakerCooldownMs: 10_000 });

    for (let i = 0; i < 3; i++) {
      await guardedFetch("/v1/nodes", { method: "POST", body: "{}" });
    }
    const blocked = await guardedFetch("/v1/nodes", { method: "POST", body: "{}" }).catch((e) => e);
    expect((blocked as TransportError).kind).toBe("breaker-open");
  });

  it("a 4xx still does NOT trip the breaker", async () => {
    const fetchMock = vi.fn(async () => okResponse({}, 403));
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ breakerThreshold: 2 });

    for (let i = 0; i < 5; i++) {
      const r = await guardedFetch("/v1/nodes/x", { method: "GET" });
      expect(r.status).toBe(403);
    }
    expect(fetchMock).toHaveBeenCalledTimes(5);
  });

  it("scopes identity correctly when headers are a Headers instance", async () => {
    // `RequestInit.headers` may be a record, a `Headers`, or a [k,v][]. The
    // record-only read yielded undefined for the other two, so every such
    // caller scoped to "anon" and coalesced onto ONE response — the
    // cross-tenant read the collision-free hash was meant to prevent,
    // reached through a different door.
    const f = deferredFetch();
    const a = guardedFetch("/v1/graphs", {
      method: "GET",
      headers: new Headers({ Authorization: "Bearer aaa" }),
    });
    const b = guardedFetch("/v1/graphs", {
      method: "GET",
      headers: new Headers({ Authorization: "Bearer bbb" }),
    });
    await tick(10);

    expect(f.fetchMock).toHaveBeenCalledTimes(2);
    f.settleAll();
    await Promise.all([a, b]);
  });

  it("does not leak an abort listener per request on a shared signal", async () => {
    // The Reefs fan-out reuses ONE AbortController across a screen's
    // lifetime, so a listener that is added but never removed accumulates one
    // dead entry per request — hundreds over a single page view.
    const f = deferredFetch();
    const ac = new AbortController();
    let added = 0;
    let removed = 0;
    const realAdd = ac.signal.addEventListener.bind(ac.signal);
    const realRemove = ac.signal.removeEventListener.bind(ac.signal);
    ac.signal.addEventListener = ((...args: Parameters<typeof realAdd>) => {
      added += 1;
      return realAdd(...args);
    }) as typeof realAdd;
    ac.signal.removeEventListener = ((...args: Parameters<typeof realRemove>) => {
      removed += 1;
      return realRemove(...args);
    }) as typeof realRemove;

    for (let i = 0; i < 5; i++) {
      const p = guardedFetch(`/v1/probe${i}`, { method: "GET" }, { signal: ac.signal });
      await tick(5);
      f.settleAll();
      await p;
    }

    // Every listener this module attached to the caller's signal is detached.
    expect(removed).toBeGreaterThanOrEqual(added);
  });

  it("scopes identity correctly when headers are an entry array", async () => {
    const f = deferredFetch();
    const a = guardedFetch("/v1/graphs", {
      method: "GET",
      headers: [["Authorization", "Bearer aaa"]],
    });
    const b = guardedFetch("/v1/graphs", {
      method: "GET",
      headers: [["authorization", "Bearer bbb"]],
    });
    await tick(10);

    expect(f.fetchMock).toHaveBeenCalledTimes(2);
    f.settleAll();
    await Promise.all([a, b]);
  });

  it("releases the discarded body before retrying", async () => {
    // `res.body?.cancel?.()` was dead in every test while the stub had no
    // body, so a regression here would have passed the suite.
    const fetchMock = vi.fn(async () => okResponse({}, 503));
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 2, breakerThreshold: 1000 });

    await guardedFetch("/v1/probe", { method: "GET" });

    expect(bodyCancelled).toBe(DEFAULT_TRANSPORT_CONFIG.maxAttempts - 1);
  });

  it("refuses an absurd Retry-After rather than hanging on it", async () => {
    // `retryMaxMs` bounds OUR backoff but not the server's instruction, and
    // the sleep happens after the slot is released — so `Retry-After: 3600`
    // would leave guardedFetch pending for an hour with no timeout, which is
    // indistinguishable from hanging forever.
    const fetchMock = vi.fn(async () => okResponse({}, 429, { "retry-after": "3600" }));
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 2, breakerThreshold: 1000 });

    const started = Date.now();
    const res = await guardedFetch("/v1/probe", { method: "GET" });

    // Returned promptly with the server's own response, not slept on.
    expect(res.status).toBe(429);
    expect(Date.now() - started).toBeLessThan(1000);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("still honours a REASONABLE Retry-After", async () => {
    let n = 0;
    const stamps: number[] = [];
    const fetchMock = vi.fn(async () => {
      stamps.push(Date.now());
      n += 1;
      return n === 1 ? okResponse({}, 429, { "retry-after": "0.05" }) : okResponse({});
    });
    vi.stubGlobal("fetch", fetchMock);
    __setTransportConfigForTests({ retryBaseMs: 1, retryMaxMs: 1, breakerThreshold: 1000 });

    const res = await guardedFetch("/v1/probe", { method: "GET" });
    expect(res.status).toBe(200);
    expect(stamps[1] - stamps[0]).toBeGreaterThanOrEqual(40);
  });

  it("keeps the timeout when the kill switch is on and a signal is supplied", async () => {
    // The fallback path previously did `opts.signal ?? unguarded`, so any
    // caller signal silently removed the timeout.
    deferredFetch();
    const prev = process.env.NEXT_PUBLIC_TRANSPORT_GUARDS;
    process.env.NEXT_PUBLIC_TRANSPORT_GUARDS = "0";
    try {
      const ac = new AbortController();
      const p = guardedFetch("/v1/graphs", { method: "GET" }, { signal: ac.signal, timeoutMs: 20 })
        .then(() => "resolved")
        .catch((e: Error) => e.name);
      // Never aborted manually — only the timeout can end this.
      await expect(p).resolves.toBe("AbortError");
    } finally {
      if (prev === undefined) delete process.env.NEXT_PUBLIC_TRANSPORT_GUARDS;
      else process.env.NEXT_PUBLIC_TRANSPORT_GUARDS = prev;
    }
  });
});

describe("(g2) identity scoping must be collision-free — CodeRabbit Major", () => {
  it("never coalesces two identities, however their tokens hash", async () => {
    // A 32-bit digest could collide, and a collision means a GET issued as
    // user B resolves with the body fetched as user A — a silent cross-tenant
    // read. Interning makes that unrepresentable rather than unlikely.
    const f = deferredFetch();
    const calls = Array.from({ length: 40 }, (_, i) =>
      guardedFetch("/v1/graphs", {
        method: "GET",
        headers: { Authorization: `Bearer tok-${i}` },
      }),
    );
    await tick(20);
    // All 40 template to ONE caller (`/v1/graphs`), so the per-caller quota
    // holds most of them in the queue — drain so every one reaches the wire.
    for (let i = 0; i < 400 && f.liveCount > 0; i++) {
      f.settleAll();
      await tick(4);
    }
    await Promise.all(calls);

    // 40 distinct identities, 40 distinct network calls — never fewer. A
    // hash collision would show up here as a smaller number.
    expect(f.fetchMock).toHaveBeenCalledTimes(40);
  });
});

describe("kill switch", () => {
  it("NEXT_PUBLIC_TRANSPORT_GUARDS=0 restores a bare fetch", async () => {
    const f = deferredFetch();
    const prev = process.env.NEXT_PUBLIC_TRANSPORT_GUARDS;
    process.env.NEXT_PUBLIC_TRANSPORT_GUARDS = "0";
    try {
      const calls = Array.from({ length: 50 }, (_, i) =>
        guardedFetch(`/v1/nodes/${i}/atoms`, { method: "GET" }),
      );
      await tick(10);
      // Unguarded: all 50 hit the network at once, which is precisely the
      // pre-W1 behaviour this flag exists to restore.
      expect(f.fetchMock).toHaveBeenCalledTimes(50);
      f.settleAll();
      await Promise.all(calls);
    } finally {
      if (prev === undefined) delete process.env.NEXT_PUBLIC_TRANSPORT_GUARDS;
      else process.env.NEXT_PUBLIC_TRANSPORT_GUARDS = prev;
    }
  });

  it("still honours caller cancellation with guards off", async () => {
    // The rollback lever relaxes SCHEDULING, it does not remove cancellation.
    // Call sites pass their signal via `opts`, so a kill switch that forwards
    // only `init` would silently break every unmount abort — introducing a
    // second bug in the mechanism meant to undo the first.
    deferredFetch();
    const prev = process.env.NEXT_PUBLIC_TRANSPORT_GUARDS;
    process.env.NEXT_PUBLIC_TRANSPORT_GUARDS = "0";
    try {
      const ac = new AbortController();
      const p = guardedFetch("/v1/graphs", { method: "GET" }, { signal: ac.signal }).catch(
        (e) => e,
      );
      await tick(5);
      ac.abort();
      const err = await p;
      expect((err as Error).name).toBe("AbortError");
    } finally {
      if (prev === undefined) delete process.env.NEXT_PUBLIC_TRANSPORT_GUARDS;
      else process.env.NEXT_PUBLIC_TRANSPORT_GUARDS = prev;
    }
  });
});

describe("instrumentation", () => {
  it("emits route templates, never concrete ids", async () => {
    const f = deferredFetch();
    const events: TransportEvent[] = [];
    setTransportSink((e) => events.push(e));

    const p = guardedFetch("/v1/nodes/3f346a2a-71ca-4779-9701-baf2e5e7bddd/atoms", { method: "GET" });
    await tick(5);
    f.settleAll();
    await p;

    expect(events).toHaveLength(1);
    expect(events[0].caller).toBe("/v1/nodes/{id}/atoms");
    expect(events[0].outcome).toBe("ok");
    // Ids must not reach the telemetry pipeline at all — scrubbing downstream
    // is a promise; not collecting them is a property.
    expect(JSON.stringify(events[0])).not.toContain("3f346a2a");
  });

  it("a throwing sink cannot break a request", async () => {
    const f = deferredFetch();
    setTransportSink(() => {
      throw new Error("sink exploded");
    });

    const p = guardedFetch("/v1/graphs", { method: "GET" });
    await tick(5);
    f.settleAll();
    await expect(p).resolves.toBeDefined();
  });
});
