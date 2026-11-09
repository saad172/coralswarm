// `X-CoralSwarm-Client` — the header that attributes requests to a client.
//
// Two things are worth locking down. (1) The value is the closed constant
// `web`. (2) The header actually reaches `fetch` from
// EVERY transport path — a constant nobody sends is worse than no constant, and
// lib/api.ts has three independent `fetch` call sites (`req`, `reqPaged`,
// `askStream`) that are easy to add a fourth to and forget.
//
// Deliberately NOT covered: `lib/shares.ts`. Its public share read is
// unauthenticated, so it produces no `user_access_log` row to attribute the
// header to (the access log is keyed on a Clerk session id) — zero attribution
// value in exchange for turning a CORS-simple request into a preflighted one on
// a public page. That module is intentionally left unstamped.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const ENV_KEYS = ["NEXT_PUBLIC_API_BASE"] as const;

type FetchMock = ReturnType<typeof vi.fn>;

/** Headers handed to the most recent `fetch` call, keys lowercased. */
function sentHeaders(mock: FetchMock): Record<string, string> {
  const init = mock.mock.calls.at(-1)?.[1] as RequestInit | undefined;
  return Object.fromEntries(new Headers(init?.headers).entries());
}

/** Minimal 200-OK stub — enough for `req`/`reqPaged` to resolve. */
function stubFetch(body: unknown): FetchMock {
  const mock = vi.fn(async () => ({
    ok: true,
    status: 200,
    json: async () => body,
    headers: new Headers(),
  }));
  vi.stubGlobal("fetch", mock);
  return mock;
}

async function load() {
  for (const k of ENV_KEYS) delete process.env[k];
  process.env.NEXT_PUBLIC_API_BASE = "http://localhost:8080";
  vi.resetModules();
  return {
    ...(await import("./clientHeader")),
    api: (await import("./api")).api,
  };
}

beforeEach(() => {
  // No `window`: the transport must not depend on the DOM (the standalone build
  // prerenders these modules on the server).
  delete (globalThis as { window?: unknown }).window;
});

afterEach(() => {
  for (const k of ENV_KEYS) delete process.env[k];
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("clientHeader", () => {
  it("uses the exact header name the backend will read", async () => {
    const { CLIENT_HEADER } = await load();
    expect(CLIENT_HEADER).toBe("X-CoralSwarm-Client");
  });

  it("is `web`", async () => {
    expect((await load()).clientName()).toBe("web");
  });
});

describe("every transport path stamps the client", () => {
  const ME = { user_id: "u", account_id: "a", role: "owner", device_id: "d" };

  it("req(): `web` alongside the bearer", async () => {
    const { api } = await load();
    const f = stubFetch(ME);
    await api.me("tok");
    expect(sentHeaders(f)["x-coralswarm-client"]).toBe("web");
    expect(sentHeaders(f)["authorization"]).toBe("Bearer tok");
  });

  it("reqPaged(): the paginated GET path", async () => {
    const { api } = await load();
    const f = stubFetch([]);
    await api.graphs.atomsPage("tok", "graph-1");
    expect(sentHeaders(f)["x-coralswarm-client"]).toBe("web");
  });

  it("askStream(): the hand-rolled SSE POST", async () => {
    const { api } = await load();
    // Fail the response so this exercises only the REQUEST side (the header)
    // and never enters the SSE reader.
    const f = vi.fn(async () => ({
      ok: false,
      status: 500,
      body: null,
      json: async () => ({ error: "boom" }),
      headers: new Headers(),
    }));
    vi.stubGlobal("fetch", f);
    await expect(api.ask.stream("tok", { text: "hi", graphIds: ["g"] }, {})).rejects.toThrow();
    expect(sentHeaders(f)["x-coralswarm-client"]).toBe("web");
  });

  // Pins the deliberate EXCLUSION, so a later "you missed one" fix has to
  // argue with a failing test instead of silently adding a preflight to the
  // public share page. See the module comment for the reasoning.
  it("does NOT stamp the unauthenticated public share read", async () => {
    for (const k of ENV_KEYS) delete process.env[k];
    process.env.NEXT_PUBLIC_API_BASE = "http://localhost:8080";
    vi.resetModules();
    const { fetchPublicShare } = await import("./shares");
    const f = stubFetch({
      title: null,
      occurred_at: "2026-01-01T00:00:00Z",
      duration_ms: null,
      summary: null,
      transcript: null,
      visibility: "link",
    });
    await fetchPublicShare("share-token");
    const h = sentHeaders(f);
    expect(h["x-coralswarm-client"]).toBeUndefined();
    expect(h["authorization"]).toBeUndefined();
    // Still a CORS-simple request: no non-safelisted header at all.
    expect(Object.keys(h)).toEqual([]);
  });
});
