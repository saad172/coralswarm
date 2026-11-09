// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  AddinHttpError, InvalidMicrosoftTokenError, ReconnectRequiredError, SsoDisabledError, SsoUnavailableError,
  createAddinClient, TOKEN_TIMEOUT_MS,
} from "./microsoftSso";

const BASE = "https://api.test";
const personal = { account_id: "acct-p", name: "Personal", kind: "personal" as const };
const org = { account_id: "acct-o", name: "Acme ISD", kind: "org" as const };

function json(status: number, body: unknown): Response {
  return new Response(status === 204 ? null : JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}
const connected = (token: string, extra: Record<string, unknown> = {}) => ({
  status: "connected", token, expires_at: "2099-01-01T00:00:00Z", account: { email: "a@b.test" }, workspace: org, ...extra,
});
const linkRequired = { status: "link_required", pairing: { code: "ABCD1234", poll_secret: "secret", approve_url: "https://app.coralswarm.example/addin/approve?code=ABCD1234&kind=microsoft", expires_at: "2099-01-01T00:00:00Z" } };

function setup(handlers: Array<(url: string, init: RequestInit) => Response | Promise<Response>>, opts: { now?: () => number; ms?: () => Promise<string> } = {}) {
  const calls: Array<{ url: string; init: RequestInit }> = [];
  let i = 0;
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    calls.push({ url: String(input), init: init ?? {} });
    const handler = handlers[Math.min(i++, handlers.length - 1)];
    return handler(String(input), init ?? {});
  }) as unknown as typeof fetch;
  const getMicrosoftToken = vi.fn(opts.ms ?? (async () => "ms-token"));
  const client = createAddinClient({ host: { getMicrosoftToken }, apiBase: () => BASE, fetchImpl, now: opts.now });
  return { client, calls, getMicrosoftToken };
}

beforeEach(() => { localStorage.clear(); sessionStorage.clear(); document.cookie = ""; });

describe("session()", () => {
  it("posts the Microsoft token, keeps the credential in memory only", async () => {
    const { client, calls } = setup([() => json(200, connected("cs-1", { auto_linked: true }))]);
    const result = await client.session();
    expect(calls[0].url).toBe(`${BASE}/v1/addin/microsoft/session`);
    expect(JSON.parse(String(calls[0].init.body))).toEqual({ ms_token: "ms-token" });
    expect(result).toMatchObject({ kind: "connected", token: "cs-1", autoLinked: true, workspace: org, account: { email: "a@b.test" } });
    expect(client.current()?.token).toBe("cs-1");
    // No persistence of any kind.
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
    expect(document.cookie).toBe("");
  });

  it("returns the pairing when the link is not made yet, then connects after approval", async () => {
    const { client, calls } = setup([
      () => json(200, linkRequired),
      (_u, init) => { expect(JSON.parse(String(init.body))).toEqual({ code: "ABCD1234", poll_secret: "secret" }); return json(200, { status: "pending" }); },
      () => json(200, { status: "approved" }),
      () => json(200, connected("cs-2")),
    ]);
    const first = await client.session();
    if (first.kind !== "link_required") throw new Error("expected link_required");
    expect(first.pairing.approveUrl).toContain("kind=microsoft");
    expect(client.current()).toBeNull();
    expect(await client.poll(first.pairing)).toBe("pending");
    expect(await client.poll(first.pairing)).toBe("approved");
    const second = await client.session();
    expect(second).toMatchObject({ kind: "connected", token: "cs-2" });
    expect(calls.map((c) => c.url.replace(BASE, ""))).toEqual(["/v1/addin/microsoft/session", "/v1/addin/pair/poll", "/v1/addin/pair/poll", "/v1/addin/microsoft/session"]);
  });

  it("maps 404 microsoft_sso_disabled to SsoDisabledError (the Clerk-fallback signal)", async () => {
    const { client } = setup([() => json(404, { error: "microsoft_sso_disabled" })]);
    await expect(client.session()).rejects.toBeInstanceOf(SsoDisabledError);
  });

  it("does not treat an unrelated 404 as the feature flag", async () => {
    const { client } = setup([() => json(404, { error: "not_found" })]);
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
  });

  it("maps 401 invalid_microsoft_token to InvalidMicrosoftTokenError", async () => {
    const { client } = setup([() => json(401, { error: "invalid_microsoft_token" })]);
    await expect(client.session()).rejects.toBeInstanceOf(InvalidMicrosoftTokenError);
  });

  it("propagates SsoUnavailableError from the host without calling the API", async () => {
    const { client, calls } = setup([() => json(200, connected("x"))], { ms: async () => { throw new SsoUnavailableError(); } });
    await expect(client.session()).rejects.toBeInstanceOf(SsoUnavailableError);
    expect(calls).toHaveLength(0);
  });

  it("shares one exchange between concurrent callers", async () => {
    const { client, calls } = setup([() => json(200, connected("cs-1"))]);
    await Promise.all([client.session(), client.session(), client.session()]);
    expect(calls).toHaveLength(1);
  });

  it("rejects a malformed connected body rather than half-connecting", async () => {
    const { client } = setup([() => json(200, { status: "connected", token: "t" })]);
    await expect(client.session()).rejects.toBeInstanceOf(AddinHttpError);
    expect(client.current()).toBeNull();
  });
});

describe("disconnect during an in-flight exchange", () => {
  it("does not resurrect the credential when the exchange resolves after disconnect()/forget()", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const { client } = setup([async () => { await gate; return json(200, connected("late")); }, () => json(204, null)]);
    const pending = client.session();
    const disconnecting = client.disconnect();
    release();
    await pending;
    await disconnecting;
    expect(client.current()).toBeNull();
  });

  it("forget() has the same effect", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const { client } = setup([async () => { await gate; return json(200, connected("late")); }]);
    const pending = client.session();
    client.forget();
    release();
    await pending;
    expect(client.current()).toBeNull();
  });
});

describe("silent refresh", () => {
  it("re-trades on a 401 and retries the call once with the new credential", async () => {
    const seen: string[] = [];
    const { client, getMicrosoftToken } = setup([
      () => json(200, connected("old")),
      (_u, init) => { seen.push(String((init.headers as Record<string, string>).Authorization)); return json(401, { error: "expired" }); },
      () => json(200, connected("new")),
      (_u, init) => { seen.push(String((init.headers as Record<string, string>).Authorization)); return json(200, { ok: true }); },
    ]);
    await client.session();
    await expect(client.call("/v1/x")).resolves.toEqual({ ok: true });
    expect(seen).toEqual(["Bearer old", "Bearer new"]);
    expect(getMicrosoftToken).toHaveBeenCalledTimes(2);
  });

  it("does not loop: a second 401 surfaces", async () => {
    const { client } = setup([
      () => json(200, connected("old")), () => json(401, {}), () => json(200, connected("new")), () => json(401, {}),
    ]);
    await client.session();
    await expect(client.call("/v1/x")).rejects.toMatchObject({ status: 401 });
  });

  it("re-trades before the credential's own expiry", async () => {
    let now = Date.parse("2026-10-01T00:00:00Z");
    const { client, calls } = setup([
      () => json(200, connected("short", { expires_at: new Date(now + 60_000).toISOString() })),
      () => json(200, connected("fresh")),
      () => json(200, { ok: true }),
    ], { now: () => now });
    await client.session();
    now += 45_000; // inside the 30 s skew
    await client.call("/v1/x");
    expect(calls[1].url).toContain("/session");
    expect((calls[2].init.headers as Record<string, string>).Authorization).toBe("Bearer fresh");
  });

  it("asks for reconnect when the link is gone at refresh time", async () => {
    const { client } = setup([() => json(200, connected("old")), () => json(401, {}), () => json(200, linkRequired)]);
    await client.session();
    await expect(client.call("/v1/x")).rejects.toBeInstanceOf(ReconnectRequiredError);
  });

  it("a rejected Microsoft token during a refresh means reconnect, not a generic error", async () => {
    const { client } = setup([() => json(200, connected("old")), () => json(401, {}), () => json(401, { error: "invalid_microsoft_token" })]);
    await client.session();
    await expect(client.call("/v1/x")).rejects.toBeInstanceOf(ReconnectRequiredError);
  });

  it("an expired credential whose re-trade is rejected also means reconnect", async () => {
    let now = Date.parse("2026-10-01T00:00:00Z");
    const { client } = setup([() => json(200, connected("short", { expires_at: new Date(now + 60_000).toISOString() })), () => json(401, { error: "invalid_microsoft_token" })], { now: () => now });
    await client.session();
    now += 45_000;
    await expect(client.call("/v1/x")).rejects.toBeInstanceOf(ReconnectRequiredError);
  });

  it("withToken wraps third-party clients that throw status-bearing errors", async () => {
    const { client } = setup([() => json(200, connected("old")), () => json(200, connected("new"))]);
    await client.session();
    const fn = vi.fn(async (token: string) => { if (token === "old") throw Object.assign(new Error("HTTP 401"), { status: 401 }); return token; });
    await expect(client.withToken(fn)).resolves.toBe("new");
  });
});

describe("link management", () => {
  it("reads the link and its workspaces", async () => {
    const { client, calls } = setup([() => json(200, connected("t")), () => json(200, { account: { email: "a@b.test" }, workspace: org, workspaces: [personal, org] })]);
    await client.session();
    const link = await client.link();
    expect(link.workspaces.map((w) => w.name)).toEqual(["Personal", "Acme ISD"]);
    expect((calls[1].init.headers as Record<string, string>).Authorization).toBe("Bearer t");
  });

  it("changes workspace and adopts the new token", async () => {
    const { client, calls } = setup([() => json(200, connected("t")), () => json(200, connected("t2", { workspace: personal }))]);
    await client.session();
    const next = await client.changeWorkspace("acct-p");
    expect(calls[1].init.method).toBe("PUT");
    expect(JSON.parse(String(calls[1].init.body))).toEqual({ account_id: "acct-p" });
    expect(next.workspace.kind).toBe("personal");
    expect(client.current()?.token).toBe("t2");
  });

  it("disconnect revokes server-side and forgets the credential", async () => {
    const { client, calls } = setup([() => json(200, connected("t")), () => json(204, null)]);
    await client.session();
    await expect(client.disconnect()).resolves.toEqual({ revoked: true });
    expect(calls[1].init.method).toBe("DELETE");
    expect(calls[1].url).toBe(`${BASE}/v1/addin/microsoft/link`);
    expect(client.current()).toBeNull();
  });

  it("disconnect reports an unrevoked credential honestly but still forgets it locally", async () => {
    const { client } = setup([() => json(200, connected("t")), () => json(500, {})]);
    await client.session();
    await expect(client.disconnect()).resolves.toEqual({ revoked: false });
    expect(client.current()).toBeNull();
  });
});

describe("poll()", () => {
  it("treats transport failures and unknown bodies as pending", async () => {
    const { client } = setup([() => { throw new Error("offline"); }, () => json(200, { status: "weird" }), () => json(500, {})]);
    const p = { code: "c", pollSecret: "s" };
    expect(await client.poll(p)).toBe("pending");
    expect(await client.poll(p)).toBe("pending");
    expect(await client.poll(p)).toBe("pending");
  });

  it("reports expiry", async () => {
    const { client } = setup([() => json(200, { status: "expired" })]);
    expect(await client.poll({ code: "c", pollSecret: "s" })).toBe("expired");
  });
});

describe("hung token acquisition", () => {
  it("a refresh whose Microsoft token never arrives fails with a transient 408 within the timeout instead of waiting forever", async () => {
    vi.useFakeTimers();
    try {
      const { client: stuck } = setup([() => json(200, connected("cs-2"))], { ms: () => new Promise<string>(() => {}) });
      const pending = stuck.call("/v1/anything").then(() => "resolved", (cause: unknown) => cause);
      await vi.advanceTimersByTimeAsync(TOKEN_TIMEOUT_MS + 100);
      const cause = await pending;
      expect(cause).toBeInstanceOf(AddinHttpError);
      expect(cause).toMatchObject({ status: 408, code: "token_timeout" });
    } finally { vi.useRealTimers(); }
  });

  it("the first sign-in is not cut short: a slow user-driven session() still resolves", async () => {
    vi.useFakeTimers();
    try {
      const { client } = setup([() => json(200, connected("cs-1"))], { ms: () => new Promise<string>((resolve) => setTimeout(() => resolve("ms"), TOKEN_TIMEOUT_MS * 3)) });
      const result = client.session();
      await vi.advanceTimersByTimeAsync(TOKEN_TIMEOUT_MS * 3 + 10);
      await expect(result).resolves.toMatchObject({ kind: "connected" });
    } finally { vi.useRealTimers(); }
  });

  it("a timeout ends the exchange: the next withToken starts a NEW one and succeeds (no waiting on the stuck promise)", async () => {
    vi.useFakeTimers();
    try {
      let calls = 0; const release: Array<(token: string) => void> = [];
      const { client, getMicrosoftToken } = setup(
        [(_url, init) => json(200, connected(`cs-for-${JSON.parse(String(init.body)).ms_token}`))],
        { ms: () => (++calls === 1 ? new Promise<string>((resolve) => release.push(resolve)) : Promise.resolve("ms-new")) },
      );
      const first = client.call("/v1/x").then(() => "ok", (cause: unknown) => cause);
      await vi.advanceTimersByTimeAsync(TOKEN_TIMEOUT_MS + 100);
      expect(await first).toMatchObject({ status: 408 });
      // Fresh attempt: a second Microsoft token request, which this time answers.
      const second = client.call("/v1/x").then(() => "ok", (cause: unknown) => cause);
      await vi.advanceTimersByTimeAsync(10);
      expect(getMicrosoftToken).toHaveBeenCalledTimes(2);
      expect(client.current()?.token).toBe("cs-for-ms-new");
      await second; // the API call itself 404s in the stub: what matters is that a fresh exchange ran
    } finally { vi.useRealTimers(); }
  });

  it("a late result from the stale exchange does not overwrite the newer credential", async () => {
    vi.useFakeTimers();
    try {
      let calls = 0; const release: Array<(token: string) => void> = [];
      const { client } = setup(
        [(_url, init) => json(200, connected(`cs-for-${JSON.parse(String(init.body)).ms_token}`))],
        { ms: () => (++calls === 1 ? new Promise<string>((resolve) => release.push(resolve)) : Promise.resolve("ms-new")) },
      );
      const first = client.call("/v1/x").catch((cause: unknown) => cause);
      await vi.advanceTimersByTimeAsync(TOKEN_TIMEOUT_MS + 100);
      await first;
      await client.call("/v1/x").catch(() => {});
      expect(client.current()?.token).toBe("cs-for-ms-new");
      release[0]("ms-old"); // the stuck exchange finally completes
      await vi.advanceTimersByTimeAsync(50);
      expect(client.current()?.token).toBe("cs-for-ms-new");
    } finally { vi.useRealTimers(); }
  });

  it("a stale 401 cannot clear a newer credential", async () => {
    vi.useFakeTimers();
    try {
      let calls = 0; const release: Array<(token: string) => void> = [];
      const { client } = setup(
        [(_url, init) => (JSON.parse(String(init.body)).ms_token === "ms-old" ? json(401, {}) : json(200, connected("cs-new")))],
        { ms: () => (++calls === 1 ? new Promise<string>((resolve) => release.push(resolve)) : Promise.resolve("ms-new")) },
      );
      const first = client.call("/v1/x").catch((cause: unknown) => cause);
      await vi.advanceTimersByTimeAsync(TOKEN_TIMEOUT_MS + 100);
      await first;
      await client.call("/v1/x").catch(() => {});
      release[0]("ms-old");
      await vi.advanceTimersByTimeAsync(50);
      expect(client.current()?.token).toBe("cs-new");
    } finally { vi.useRealTimers(); }
  });
});
