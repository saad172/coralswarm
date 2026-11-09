import { describe, expect, it, vi } from "vitest";
import { bearerEmailApi, ssoEmailApi } from "./emailTransport";
import { AddinHttpError, ReconnectRequiredError, type AddinClient } from "./microsoftSso";

const client = (call: (...args: unknown[]) => Promise<unknown>) => ({ call } as unknown as AddinClient);

describe("ssoEmailApi", () => {
  it("returns ok with the data and passes method/body through", async () => {
    const call = vi.fn(async () => ({ ingested: 3 }));
    const api = ssoEmailApi(client(call));
    await expect(api("POST", "/v1/connectors/microsoft/ingest", { item_ids: ["a"] })).resolves.toEqual({ status: 200, ok: true, data: { ingested: 3 } });
    expect(call).toHaveBeenCalledWith("/v1/connectors/microsoft/ingest", expect.objectContaining({ method: "POST", body: { item_ids: ["a"] } }));
  });

  it("maps HTTP failures to a status and a lost link to unauthenticated", async () => {
    expect(await ssoEmailApi(client(async () => { throw new AddinHttpError(403); }))("GET", "/x")).toMatchObject({ status: 403, ok: false });
    expect(await ssoEmailApi(client(async () => { throw new ReconnectRequiredError(); }))("GET", "/x")).toMatchObject({ unauthenticated: true, ok: false });
  });

  it("rethrows network failures", async () => {
    await expect(ssoEmailApi(client(async () => { throw new TypeError("offline"); }))("GET", "/x")).rejects.toThrow("offline");
  });

  it("aborts after the timeout", async () => {
    vi.useFakeTimers();
    let signal: AbortSignal | undefined;
    const api = ssoEmailApi(client(async (_p, init) => { signal = (init as { signal?: AbortSignal }).signal; return new Promise(() => {}); }));
    void api("GET", "/x", undefined, 50);
    await vi.advanceTimersByTimeAsync(60);
    expect(signal?.aborted).toBe(true);
    vi.useRealTimers();
  });
});

describe("bearerEmailApi (Clerk fallback)", () => {
  it("mints a token per request and sends it as the bearer", async () => {
    const getToken = vi.fn(async () => "clerk-jwt");
    const fetchImpl = vi.fn(async () => new Response(JSON.stringify({ following: true }), { status: 200 })) as unknown as typeof fetch;
    const result = await bearerEmailApi(getToken, () => "https://api.test/", fetchImpl)("GET", "/v1/connectors/microsoft/watched/abc");
    expect(result).toEqual({ status: 200, ok: true, data: { following: true } });
    const [url, init] = (fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(url).toBe("https://api.test/v1/connectors/microsoft/watched/abc");
    expect((init as RequestInit).headers).toMatchObject({ Authorization: "Bearer clerk-jwt" });
  });

  it("is unauthenticated without a session token and never calls the API", async () => {
    const fetchImpl = vi.fn() as unknown as typeof fetch;
    expect(await bearerEmailApi(async () => null, () => "https://api.test", fetchImpl)("GET", "/x")).toMatchObject({ unauthenticated: true });
    expect(fetchImpl).not.toHaveBeenCalled();
  });
});
