import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ADDIN_OAUTH_SCOPE, OAuthTransientError, SignedOutError, addinOAuthEnabled, base64url, buildAuthorizeUrl, buildStartUrl, createClerkSession, createPkce,
  createState, exchangeCode, fapiFromPublishableKey, hostFromState, jwtClaims, parseStartParams, readClerkOAuthConfig, readClerkOAuthConfigFor, sessionFromTokens,
  type ClerkOAuthConfig, type TokenSet, type TokenStore,
} from "./clerkOAuth";

const DEV_PK = `pk_test_${btoa("example.clerk.accounts.dev$")}`;
const PROD_PK = `pk_live_${btoa("clerk.coralswarm.example$")}`;
const PROD: ClerkOAuthConfig = { clientId: "example_18", fapi: "https://clerk.coralswarm.example" };

afterEach(() => { vi.unstubAllEnvs(); });

function jwt(claims: Record<string, unknown>): string {
  const enc = (o: unknown) => base64url(new TextEncoder().encode(JSON.stringify(o)));
  return `${enc({ alg: "RS256" })}.${enc(claims)}.sig`;
}

describe("env (read lazily, both states)", () => {
  it("derives the frontend API host from the publishable key, prod and dev", () => {
    expect(fapiFromPublishableKey(PROD_PK)).toBe("https://clerk.coralswarm.example");
    expect(fapiFromPublishableKey(DEV_PK)).toBe("https://example.clerk.accounts.dev");
  });

  it("rejects keys that are not publishable keys", () => {
    for (const bad of [undefined, "", "sk_live_abc", "pk_live_", "pk_live_!!!", `pk_live_${btoa("no-dollar.example.com")}`, `pk_live_${btoa("nodot$")}`, `pk_live_${btoa("evil.com/path$")}`]) {
      expect(fapiFromPublishableKey(bad)).toBeNull();
    }
  });

  it("is off without a client id, whatever else is set", () => {
    expect(readClerkOAuthConfig({ publishableKey: PROD_PK })).toBeNull();
    expect(readClerkOAuthConfig({ clientId: "   ", publishableKey: PROD_PK })).toBeNull();
    expect(addinOAuthEnabled({ publishableKey: PROD_PK })).toBe(false);
  });

  it("is on with a client id and a derivable host", () => {
    expect(readClerkOAuthConfig({ clientId: "example_18", publishableKey: PROD_PK })).toEqual(PROD);
    expect(readClerkOAuthConfig({ clientId: "exampleclientid0", publishableKey: DEV_PK })).toEqual({ clientId: "exampleclientid0", fapi: "https://example.clerk.accounts.dev" });
  });

  it("stays off (today's pairing) when the host cannot be derived", () => {
    expect(readClerkOAuthConfig({ clientId: "example_18", publishableKey: "garbage" })).toBeNull();
  });

  it("reads process.env at call time, not at import time", () => {
    expect(addinOAuthEnabled()).toBe(false);
    vi.stubEnv("NEXT_PUBLIC_ADDIN_CLERK_OAUTH_CLIENT_ID", "example_18");
    vi.stubEnv("NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY", PROD_PK);
    expect(readClerkOAuthConfig()).toEqual(PROD);
    vi.stubEnv("NEXT_PUBLIC_ADDIN_CLERK_OAUTH_CLIENT_ID", "");
    expect(readClerkOAuthConfig()).toBeNull();
  });
});

describe("PKCE, state, start params", () => {
  it("makes an S256 challenge that matches its verifier", async () => {
    const { verifier, challenge } = await createPkce();
    expect(verifier).toMatch(/^[A-Za-z0-9_-]{64}$/);
    expect(challenge).toMatch(/^[A-Za-z0-9_-]{43}$/);
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(verifier));
    expect(challenge).toBe(base64url(new Uint8Array(digest)));
    expect((await createPkce()).verifier).not.toBe(verifier);
  });

  it("makes a 33-character base64url state that names its host", () => {
    const o = createState("outlook"); const t = createState("teams");
    expect(o).toMatch(/^o[A-Za-z0-9_-]{32}$/);
    expect(hostFromState(o)).toBe("outlook");
    expect(hostFromState(t)).toBe("teams");
    expect(hostFromState("x" + "a".repeat(32))).toBeNull();
    expect(hostFromState("oshort")).toBeNull();
  });

  it("accepts only a well-formed start query", async () => {
    const { challenge } = await createPkce();
    const state = createState("outlook");
    const url = buildStartUrl("https://app.coralswarm.example", { cc: challenge, state, host: "outlook" });
    expect(url).toBe(`https://app.coralswarm.example/addin/auth/start?cc=${challenge}&state=${state}&host=outlook`);
    expect(parseStartParams(new URL(url).search)).toEqual({ cc: challenge, state, host: "outlook", prompt: null });
    expect(parseStartParams(new URL(buildStartUrl("https://x.test", { cc: challenge, state, host: "outlook", prompt: "consent" })).search)?.prompt).toBe("consent");

    const bad = (query: string) => expect(parseStartParams(query)).toBeNull();
    bad("");
    bad(`?cc=short&state=${state}&host=outlook`);
    bad(`?cc=${challenge}&state=short&host=outlook`);
    bad(`?cc=${challenge}&state=${state}&host=evil`);
    bad(`?cc=${challenge}&state=${state}&host=teams`); // state says outlook
    bad(`?cc=${challenge}&state=${state}&host=outlook&prompt=login`);
    bad(`?cc=${challenge}!&state=${state}&host=outlook`);
  });
});

describe("Google Meet host (marker g, its own client)", () => {
  it("marks a Meet state with g and reads it back as meet", () => {
    const g = createState("meet");
    expect(g).toMatch(/^g[A-Za-z0-9_-]{32}$/);
    expect(hostFromState(g)).toBe("meet");
    expect(hostFromState("o" + g.slice(1))).toBe("outlook");
  });

  it("accepts host=meet only with a g state, and the other hosts never with one", async () => {
    const { challenge } = await createPkce();
    const g = createState("meet");
    expect(parseStartParams(`?cc=${challenge}&state=${g}&host=meet`)).toEqual({ cc: challenge, state: g, host: "meet", prompt: null });
    expect(parseStartParams(`?cc=${challenge}&state=${g}&host=outlook`)).toBeNull();
    expect(parseStartParams(`?cc=${challenge}&state=${g}&host=teams`)).toBeNull();
    expect(parseStartParams(`?cc=${challenge}&state=${createState("teams")}&host=meet`)).toBeNull();
  });

  it("picks the Meet client id for meet and the shared add-in client for Outlook/Teams, read lazily", () => {
    vi.stubEnv("NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY", PROD_PK);
    vi.stubEnv("NEXT_PUBLIC_ADDIN_CLERK_OAUTH_CLIENT_ID", "example_18");
    expect(readClerkOAuthConfigFor("meet")).toBeNull();
    vi.stubEnv("NEXT_PUBLIC_MEET_ADDIN_CLERK_OAUTH_CLIENT_ID", "example_18");
    expect(readClerkOAuthConfigFor("meet")).toEqual({ clientId: "example_18", fapi: "https://clerk.coralswarm.example" });
    expect(readClerkOAuthConfigFor("outlook")?.clientId).toBe("example_18");
    expect(readClerkOAuthConfigFor("teams")?.clientId).toBe("example_18");
    expect(readClerkOAuthConfig()?.clientId).toBe("example_18");
    // The authorize URL carries whichever client the host picked.
    const meet = readClerkOAuthConfigFor("meet") ?? PROD;
    expect(new URL(buildAuthorizeUrl(meet, "https://app.coralswarm.example", { cc: "c".repeat(43), state: createState("meet") })).searchParams.get("client_id")).toBe("example_18");
  });
});

describe("authorize URL", () => {
  it("is built from constants plus the challenge and state, nothing else", () => {
    const url = new URL(buildAuthorizeUrl(PROD, "https://app.coralswarm.example", { cc: "c".repeat(43), state: "o" + "s".repeat(32) }));
    expect(url.origin + url.pathname).toBe("https://clerk.coralswarm.example/oauth/authorize");
    expect(Object.fromEntries(url.searchParams)).toEqual({
      response_type: "code", client_id: "example_18", redirect_uri: "https://app.coralswarm.example/addin/auth/callback",
      scope: ADDIN_OAUTH_SCOPE, code_challenge: "c".repeat(43), code_challenge_method: "S256", state: "o" + "s".repeat(32),
    });
    expect(ADDIN_OAUTH_SCOPE.split(" ")).toContain("user:org:read");
  });

  it("adds prompt=consent only when asked", () => {
    const consent = new URL(buildAuthorizeUrl(PROD, "https://app.coralswarm.example", { cc: "c".repeat(43), state: "o" + "s".repeat(32), prompt: "consent" }));
    expect(consent.searchParams.get("prompt")).toBe("consent");
    expect(new URL(buildAuthorizeUrl(PROD, "https://app.coralswarm.example", { cc: "c", state: "s", prompt: null })).searchParams.has("prompt")).toBe(false);
  });
});

function tokenResponse(body: Record<string, unknown>, status = 200): Response { return new Response(JSON.stringify(body), { status }); }
const grant = (n: number, extra: Record<string, unknown> = {}) => ({ access_token: `access-${n}`, refresh_token: `refresh-${n}`, expires_in: 86400, token_type: "Bearer", ...extra });

describe("code exchange", () => {
  it("posts a form to the frontend API's token endpoint with the verifier", async () => {
    const fetchImpl = vi.fn(async () => tokenResponse(grant(1, { id_token: "idt" }))) as unknown as typeof fetch;
    const tokens = await exchangeCode(PROD, { code: "the-code", verifier: "the-verifier", redirectUri: "https://app.coralswarm.example/addin/auth/callback" }, { fetchImpl, now: () => 1000 });
    const [url, init] = (fetchImpl as unknown as ReturnType<typeof vi.fn>).mock.calls[0] as [string, RequestInit];
    expect(url).toBe("https://clerk.coralswarm.example/oauth/token");
    expect((init.headers as Record<string, string>)["Content-Type"]).toBe("application/x-www-form-urlencoded");
    expect(Object.fromEntries(new URLSearchParams(String(init.body)))).toEqual({
      grant_type: "authorization_code", code: "the-code", redirect_uri: "https://app.coralswarm.example/addin/auth/callback", client_id: "example_18", code_verifier: "the-verifier",
    });
    expect(tokens).toEqual({ accessToken: "access-1", refreshToken: "refresh-1", expiresAt: 1000 + 86400_000, idToken: "idt" });
  });

  it("never touches storage", async () => {
    const fetchImpl = vi.fn(async () => tokenResponse(grant(1))) as unknown as typeof fetch;
    const store = vi.fn();
    vi.stubGlobal("localStorage", { setItem: store, getItem: store });
    vi.stubGlobal("sessionStorage", { setItem: store, getItem: store });
    const session = createClerkSession(PROD, { fetchImpl });
    session.set(await exchangeCode(PROD, { code: "c", verifier: "v", redirectUri: "r" }, { fetchImpl }));
    await session.accessToken();
    expect(store).not.toHaveBeenCalled();
    vi.unstubAllGlobals();
  });
});

describe("refresh: single-flight, rotation, signed-out", () => {
  const expired = { accessToken: "access-0", refreshToken: "refresh-0", expiresAt: 0, idToken: null };

  it("returns a live token without a network call", async () => {
    const fetchImpl = vi.fn() as unknown as typeof fetch;
    const session = createClerkSession(PROD, { fetchImpl, now: () => 0 });
    session.set({ ...expired, expiresAt: 10 * 60_000 });
    expect(await session.accessToken()).toBe("access-0");
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("shares ONE refresh between concurrent callers", async () => {
    let release!: (r: Response) => void;
    const fetchImpl = vi.fn(() => new Promise<Response>((resolve) => { release = resolve; })) as unknown as typeof fetch;
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 });
    session.set(expired);
    const calls = [session.accessToken(), session.accessToken(), session.accessToken({ force: true })];
    release(tokenResponse(grant(1)));
    expect(await Promise.all(calls)).toEqual(["access-1", "access-1", "access-1"]);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("rotates: the next refresh presents the NEW refresh token, never the old one", async () => {
    const used: string[] = [];
    let n = 0;
    const fetchImpl = vi.fn(async (_u: unknown, init?: RequestInit) => {
      const form = new URLSearchParams(String(init?.body));
      used.push(form.get("refresh_token") ?? "");
      expect(form.get("grant_type")).toBe("refresh_token");
      expect(form.get("client_id")).toBe("example_18");
      return tokenResponse(grant(++n));
    }) as unknown as typeof fetch;
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 });
    session.set(expired);
    expect(await session.accessToken()).toBe("access-1");
    expect(await session.accessToken({ force: true })).toBe("access-2");
    expect(await session.accessToken({ force: true })).toBe("access-3");
    expect(used).toEqual(["refresh-0", "refresh-1", "refresh-2"]);
  });

  it("keeps the old refresh token when the server sends none", async () => {
    const used: string[] = [];
    const fetchImpl = vi.fn(async (_u: unknown, init?: RequestInit) => { used.push(new URLSearchParams(String(init?.body)).get("refresh_token") ?? ""); return tokenResponse({ access_token: "a", expires_in: 100 }); }) as unknown as typeof fetch;
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 });
    session.set(expired);
    await session.accessToken({ force: true });
    await session.accessToken({ force: true });
    expect(used).toEqual(["refresh-0", "refresh-0"]);
  });

  it("invalid_grant means signed out: tokens are dropped and the error is SignedOutError", async () => {
    const fetchImpl = vi.fn(async () => tokenResponse({ error: "invalid_grant" }, 400)) as unknown as typeof fetch;
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 });
    session.set(expired);
    await expect(session.accessToken()).rejects.toBeInstanceOf(SignedOutError);
    expect(session.hasTokens()).toBe(false);
    await expect(session.accessToken()).rejects.toBeInstanceOf(SignedOutError);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("a network or 5xx failure is transient and keeps the tokens", async () => {
    const fetchImpl = vi.fn()
      .mockRejectedValueOnce(new TypeError("offline"))
      .mockResolvedValueOnce(tokenResponse({ error: "server_error" }, 503))
      .mockResolvedValueOnce(tokenResponse(grant(1))) as unknown as typeof fetch;
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 });
    session.set(expired);
    await expect(session.accessToken()).rejects.toBeInstanceOf(OAuthTransientError);
    await expect(session.accessToken()).rejects.toBeInstanceOf(OAuthTransientError);
    expect(session.hasTokens()).toBe(true);
    expect(await session.accessToken()).toBe("access-1");
  });

  it("an in-flight refresh cannot write back after clear()", async () => {
    let release!: (r: Response) => void;
    const fetchImpl = vi.fn(() => new Promise<Response>((resolve) => { release = resolve; })) as unknown as typeof fetch;
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 });
    session.set(expired);
    const pending = session.accessToken();
    session.clear();
    release(tokenResponse(grant(1)));
    await pending;
    expect(session.hasTokens()).toBe(false);
  });
});

describe("persisted session (a TokenStore): the Meet panel signs in once per browser", () => {
  const live = { accessToken: "access-0", refreshToken: "refresh-0", expiresAt: 10 * 60_000, idToken: null };
  const expired = { ...live, expiresAt: 0 };

  function memoryStore(initial: TokenSet | null = null): TokenStore & { current: () => TokenSet | null } {
    let record = initial;
    return { load: () => record, save: (next) => { record = next; }, current: () => record };
  }

  function refreshingFetch(used: string[], respond: (presented: string, n: number) => Response) {
    let n = 0;
    return vi.fn(async (_u: unknown, init?: RequestInit) => {
      const presented = new URLSearchParams(String(init?.body)).get("refresh_token") ?? "";
      used.push(presented);
      return respond(presented, ++n);
    }) as unknown as typeof fetch;
  }

  it("a new session over a store that already holds tokens starts signed in, with no network call", async () => {
    const fetchImpl = vi.fn() as unknown as typeof fetch;
    const session = createClerkSession(PROD, { fetchImpl, now: () => 0 }, memoryStore(live));
    expect(session.hasTokens()).toBe(true);
    expect(await session.accessToken()).toBe("access-0");
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("set() and a successful refresh write through to the store", async () => {
    const store = memoryStore();
    const fetchImpl = refreshingFetch([], (_p, n) => tokenResponse(grant(n)));
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 }, store);
    session.set(expired);
    expect(store.current()).toEqual(expired);
    expect(await session.accessToken()).toBe("access-1");
    expect(store.current()).toMatchObject({ accessToken: "access-1", refreshToken: "refresh-1" });
  });

  it("clear() and invalid_grant both remove the stored record", async () => {
    const cleared = memoryStore(live);
    createClerkSession(PROD, { now: () => 0 }, cleared).clear();
    expect(cleared.current()).toBeNull();

    const revoked = memoryStore(expired);
    const fetchImpl = vi.fn(async () => tokenResponse({ error: "invalid_grant" }, 400)) as unknown as typeof fetch;
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 }, revoked);
    await expect(session.accessToken()).rejects.toBeInstanceOf(SignedOutError);
    expect(session.hasTokens()).toBe(false);
    expect(revoked.current()).toBeNull();
  });

  it("presents a refresh token another panel rotated in the store, not the stale in-memory one", async () => {
    const store = memoryStore();
    const used: string[] = [];
    const fetchImpl = refreshingFetch(used, (_p, n) => tokenResponse(grant(n + 1)));
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 }, store);
    session.set(expired);
    // A second Meet tab refreshed first; refresh-0 is dead and refresh-1 is the live one.
    store.save({ ...expired, accessToken: "access-1", refreshToken: "refresh-1" });
    expect(await session.accessToken()).toBe("access-2");
    expect(used).toEqual(["refresh-1"]);
    expect(store.current()).toMatchObject({ refreshToken: "refresh-2" });
  });

  it("invalid_grant while another panel rotated adopts the newer stored token instead of signing out", async () => {
    const store = memoryStore();
    const used: string[] = [];
    const fetchImpl = refreshingFetch(used, (presented) => {
      if (presented === "refresh-0") {
        store.save({ ...expired, accessToken: "access-9", refreshToken: "refresh-9" });
        return tokenResponse({ error: "invalid_grant" }, 400);
      }
      return tokenResponse(grant(10));
    });
    const session = createClerkSession(PROD, { fetchImpl, now: () => 1_000_000 }, store);
    session.set(expired);
    expect(await session.accessToken()).toBe("access-10");
    expect(used).toEqual(["refresh-0", "refresh-9"]);
    expect(session.hasTokens()).toBe(true);
    expect(store.current()).toMatchObject({ accessToken: "access-10", refreshToken: "refresh-10" });
  });
});

describe("what a Clerk-direct pane shows", () => {
  it("reads email from the id token and the org from the access token", () => {
    const session = sessionFromTokens({ accessToken: jwt({ org_id: "org_1", client_id: "x" }), refreshToken: "r", expiresAt: 5, idToken: jwt({ email: "ada@acme.test", org_name: "Acme ISD" }) });
    expect(session).toMatchObject({ account: { email: "ada@acme.test" }, workspace: { account_id: "org_1", name: "Acme ISD", kind: "org" }, expiresAt: 5 });
    expect(jwtClaims("not-a-jwt")).toBeNull();
  });

  it("is personal when no org was picked", () => {
    expect(sessionFromTokens({ accessToken: jwt({ sub: "u" }), refreshToken: null, expiresAt: 1, idToken: null }).workspace).toEqual({ account_id: "personal", name: "Personal", kind: "personal" });
  });
});
