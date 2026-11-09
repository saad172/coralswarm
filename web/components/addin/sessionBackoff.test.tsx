// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AddinHttpError, backoffDelay, isTransientSessionFailure, createAddinClient, type AddinClient } from "@/lib/addin/microsoftSso";
import { MAX_BOOT_RETRIES, useAddinConnection } from "./useAddinConnection";

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { cleanup(); vi.useRealTimers(); });

const org = { account_id: "o", name: "Acme", kind: "org" as const };
const ok = () => new Response(JSON.stringify({ status: "connected", token: "t", expires_at: "2099-01-01T00:00:00Z", account: { email: "a@b.test" }, workspace: org }), { status: 200 });
const unavailable = () => new Response(JSON.stringify({ error: "down" }), { status: 503 });

describe("backoffDelay", () => {
  it("doubles from 1s and caps at 30s", () => {
    expect([0, 1, 2, 3, 4, 5, 6, 20].map(backoffDelay)).toEqual([1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
  });
});

describe("client session gate", () => {
  it("after a 503, calls inside the backoff window do not reach the API; the window grows 1s, 2s", async () => {
    const fetchImpl = vi.fn(async () => unavailable()) as unknown as typeof fetch;
    const client = createAddinClient({ host: { getMicrosoftToken: async () => "ms" }, apiBase: () => "https://api.test", fetchImpl });
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    // A storm of callers (polling, focus handlers) inside the window: zero further requests.
    for (let i = 0; i < 10; i++) await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1000);
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1999);
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it("force bypasses the window, and a success clears it", async () => {
    const fetchImpl = vi.fn().mockImplementationOnce(async () => unavailable()).mockImplementation(async () => ok()) as unknown as typeof fetch;
    const client = createAddinClient({ host: { getMicrosoftToken: async () => "ms" }, apiBase: () => "https://api.test", fetchImpl });
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    await expect(client.session({ force: true })).resolves.toMatchObject({ kind: "connected" });
    await expect(client.session()).resolves.toMatchObject({ kind: "connected" });
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });
});

describe("what counts as transient", () => {
  it("network, 408, 429 and 5xx retry; other 4xx do not", () => {
    for (const status of [408, 429, 500, 502, 503]) expect(isTransientSessionFailure(new AddinHttpError(status)), String(status)).toBe(true);
    for (const status of [400, 401, 403, 404, 409]) expect(isTransientSessionFailure(new AddinHttpError(status)), String(status)).toBe(false);
    expect(isTransientSessionFailure(new TypeError("Failed to fetch"))).toBe(true);
  });

  it("a 403 is not backed off: the next call goes straight to the API", async () => {
    const fetchImpl = vi.fn(async () => new Response(JSON.stringify({ error: "forbidden" }), { status: 403 })) as unknown as typeof fetch;
    const client = createAddinClient({ host: { getMicrosoftToken: async () => "ms" }, apiBase: () => "https://api.test", fetchImpl });
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("a 429 is backed off", async () => {
    const fetchImpl = vi.fn(async () => new Response("{}", { status: 429 })) as unknown as typeof fetch;
    const client = createAddinClient({ host: { getMicrosoftToken: async () => "ms" }, apiBase: () => "https://api.test", fetchImpl });
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("boot does not auto-retry a 400: one request, then the error state", async () => {
    const client = { session: vi.fn(async () => { throw new AddinHttpError(400); }), poll: vi.fn(), disconnect: vi.fn(), forget: vi.fn() } as unknown as AddinClient;
    const { result } = renderHook(() => useAddinConnection({ client, host: { openLink: async () => {} }, toast: () => {} }));
    await act(async () => { await vi.advanceTimersByTimeAsync(60_000); });
    expect((client.session as ReturnType<typeof vi.fn>)).toHaveBeenCalledTimes(1);
    expect(result.current.state.status).toBe("error");
  });
});

describe("credential reset clears backoff", () => {
  it("a 503 that finishes after forget() does not gate the next session", async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const fetchImpl = vi.fn()
      .mockImplementationOnce(async () => { await gate; return unavailable(); })
      .mockImplementation(async () => ok()) as unknown as typeof fetch;
    const client = createAddinClient({ host: { getMicrosoftToken: async () => "ms" }, apiBase: () => "https://api.test", fetchImpl });
    const stale = client.session();
    const settled = expect(stale).rejects.toBeInstanceOf(AddinHttpError);
    await vi.advanceTimersByTimeAsync(0);
    client.forget();
    release();
    await settled;
    await expect(client.session()).resolves.toMatchObject({ kind: "connected" });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it("forget() and disconnect() clear an existing backoff window", async () => {
    const fetchImpl = vi.fn().mockImplementationOnce(async () => unavailable()).mockImplementation(async () => ok()) as unknown as typeof fetch;
    const client = createAddinClient({ host: { getMicrosoftToken: async () => "ms" }, apiBase: () => "https://api.test", fetchImpl });
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    client.forget();
    await expect(client.session()).resolves.toMatchObject({ kind: "connected" });
  });
});

describe("boot retries", () => {
  function fakeClient(session: () => Promise<unknown>) {
    return { session: vi.fn(session), poll: vi.fn(), disconnect: vi.fn(), forget: vi.fn() } as unknown as AddinClient & { session: ReturnType<typeof vi.fn> };
  }

  it("retries at 1s, 2s, 4s then stops on the error state with a Retry; Retry restarts", async () => {
    const client = fakeClient(async () => { throw new AddinHttpError(503); });
    const { result } = renderHook(() => useAddinConnection({ client, host: { openLink: async () => {} }, toast: () => {} }));
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(client.session).toHaveBeenCalledTimes(1);
    expect(result.current.state.status).toBe("booting");
    await act(async () => { await vi.advanceTimersByTimeAsync(999); });
    expect(client.session).toHaveBeenCalledTimes(1);
    await act(async () => { await vi.advanceTimersByTimeAsync(1); });
    expect(client.session).toHaveBeenCalledTimes(2);
    await act(async () => { await vi.advanceTimersByTimeAsync(2000); });
    expect(client.session).toHaveBeenCalledTimes(3);
    await act(async () => { await vi.advanceTimersByTimeAsync(4000); });
    expect(client.session).toHaveBeenCalledTimes(1 + MAX_BOOT_RETRIES);
    expect(result.current.state.status).toBe("error");
    // Nothing more, however long we wait.
    await act(async () => { await vi.advanceTimersByTimeAsync(120_000); });
    expect(client.session).toHaveBeenCalledTimes(1 + MAX_BOOT_RETRIES);
    await act(async () => { void result.current.retry(); await vi.advanceTimersByTimeAsync(0); });
    expect(client.session).toHaveBeenCalledTimes(2 + MAX_BOOT_RETRIES);
  });

  it("recovers on its own when the backend comes back", async () => {
    let n = 0;
    const client = fakeClient(async () => { if (n++ < 1) throw new AddinHttpError(503); return { kind: "connected", token: "t", expiresAt: Date.now() + 1e6, account: { email: "a@b.test" }, workspace: org, autoLinked: false }; });
    const { result } = renderHook(() => useAddinConnection({ client, host: { openLink: async () => {} }, toast: () => {} }));
    await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
    expect(result.current.state.status).toBe("connected");
  });
});
