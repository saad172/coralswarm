import { afterEach, describe, expect, it, vi } from "vitest";
import { ReconnectRequiredError, SsoUnavailableError, type AddinHost } from "./microsoftSso";
import { SignInRequiredError, WORKSPACE_LOOKUP_TIMEOUT_MS, createPaneClient, oauthControlsFor } from "./oauthClient";
import { DialogCancelledError } from "./authDialog";
import { base64url, type ClerkOAuthConfig } from "./clerkOAuth";

const CONFIG: ClerkOAuthConfig = { clientId: "example_18", fapi: "https://clerk.coralswarm.example" };
const BASE = "https://api.test";
const org = { account_id: "acct-o", name: "Acme ISD", kind: "org" };
const connected = (token: string) => ({ status: "connected", token, expires_at: "2099-01-01T00:00:00Z", account: { email: "ada@acme.test" }, workspace: org });
const jwt = (claims: Record<string, unknown>) => { const e = (o: unknown) => base64url(new TextEncoder().encode(JSON.stringify(o))); return `${e({ alg: "x" })}.${e(claims)}.s`; };
const json = (status: number, body: unknown) => new Response(status === 204 ? null : JSON.stringify(body), { status });
const dialogOk = vi.fn(async (_options?: unknown) => ({ code: "code-1", verifier: "ver", redirectUri: "https://app.coralswarm.example/addin/auth/callback" }));

interface Call { method: string; url: string; headers: Record<string, string>; body: string }
function setup(routes: Record<string, Array<() => Response | Promise<Response>> | (() => Response | Promise<Response>)>, opts: { ms?: () => Promise<string>; runDialog?: typeof dialogOk } = {}) {
  const calls: Call[] = [];
  const counters = new Map<string, number>();
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const method = init?.method ?? "GET";
    const key = `${method} ${url.replace(BASE, "").replace(CONFIG.fapi, "FAPI")}`;
    calls.push({ method, url, headers: (init?.headers ?? {}) as Record<string, string>, body: String(init?.body ?? "") });
    const entry = routes[key];
    if (!entry) return json(500, { error: `unrouted ${key}` });
    const n = counters.get(key) ?? 0; counters.set(key, n + 1);
    return (Array.isArray(entry) ? entry[Math.min(n, entry.length - 1)] : entry)();
  }) as unknown as typeof fetch;
  const host: AddinHost = { name: "outlook", getMicrosoftToken: opts.ms ?? (async () => "ms-token"), openLink: async () => {} };
  const client = createPaneClient({ host, apiBase: () => BASE, dialog: { kind: "teams" }, config: CONFIG, fetchImpl, runDialog: opts.runDialog ?? dialogOk });
  return { client, calls, host, fetchImpl };
}
const TOKEN = "POST FAPI/oauth/token";
const grant = (access: string, refresh: string, extra: Record<string, unknown> = {}) => () => json(200, { access_token: access, refresh_token: refresh, expires_in: 86400, ...extra });

afterEach(() => { vi.clearAllMocks(); });

describe("rollout gate", () => {
  it("off: a plain pairing client, no OAuth controls", () => {
    const client = createPaneClient({ host: { name: "outlook", getMicrosoftToken: async () => "x", openLink: async () => {} }, apiBase: () => BASE, dialog: { kind: "teams" }, config: null });
    expect(oauthControlsFor(client)).toBeNull();
  });

  it("off when the env has no client id (the default read)", () => {
    const client = createPaneClient({ host: { name: "outlook", getMicrosoftToken: async () => "x", openLink: async () => {} }, apiBase: () => BASE, dialog: { kind: "teams" } });
    expect(oauthControlsFor(client)).toBeNull();
  });

  it("on: controls are attached", () => {
    expect(oauthControlsFor(setup({}).client)).not.toBeNull();
  });
});

describe("boot (returning user)", () => {
  it("connects silently through the existing session endpoint when the Microsoft link exists", async () => {
    const { client } = setup({ "POST /v1/addin/microsoft/session": () => json(200, connected("csms_1")) });
    expect(await client.session({ force: true })).toMatchObject({ kind: "connected", token: "csms_1" });
  });

  it("asks for a sign-in, never a pairing, when there is no link", async () => {
    const { client } = setup({ "POST /v1/addin/microsoft/session": () => json(200, { status: "link_required", pairing: { code: "ABCD1234", poll_secret: "s", approve_url: "u", expires_at: "2099-01-01T00:00:00Z" } }) });
    await expect(client.session({ force: true })).rejects.toBeInstanceOf(SignInRequiredError);
  });

  it("asks for a sign-in when Microsoft SSO is unavailable or off", async () => {
    await expect(setup({}, { ms: async () => { throw new SsoUnavailableError(); } }).client.session()).rejects.toBeInstanceOf(SignInRequiredError);
    await expect(setup({ "POST /v1/addin/microsoft/session": () => json(404, { error: "microsoft_sso_disabled" }) }).client.session()).rejects.toBeInstanceOf(SignInRequiredError);
  });
});

describe("sign-in with a Microsoft token: the link call", () => {
  it("posts {ms_token} with the Clerk access token as Bearer, then runs on the csms_ session", async () => {
    const clerkAccess = jwt({ org_id: "org_1", client_id: CONFIG.clientId });
    const { client, calls } = setup({
      [TOKEN]: grant(clerkAccess, "refresh-1"),
      "POST /v1/addin/microsoft/link": () => json(200, connected("csms_9")),
      "GET /v1/connectors/microsoft/watched": () => json(200, { ok: true }),
    });
    const session = await oauthControlsFor(client)!.signIn();
    expect(session).toMatchObject({ token: "csms_9", workspace: org, account: { email: "ada@acme.test" } });

    const exchange = calls.find((c) => c.url.endsWith("/oauth/token"))!;
    expect(Object.fromEntries(new URLSearchParams(exchange.body))).toMatchObject({ grant_type: "authorization_code", code: "code-1", code_verifier: "ver", client_id: CONFIG.clientId });
    const link = calls.find((c) => c.url === `${BASE}/v1/addin/microsoft/link`)!;
    expect(link.method).toBe("POST");
    expect(link.headers.Authorization).toBe(`Bearer ${clerkAccess}`);
    expect(JSON.parse(link.body)).toEqual({ ms_token: "ms-token" });

    // "exactly as today": the client now carries the csms_ credential.
    await client.call("/v1/connectors/microsoft/watched");
    expect(calls.at(-1)!.headers.Authorization).toBe("Bearer csms_9");
    expect(client.current()?.token).toBe("csms_9");
  });

  it("re-runs the dialog with prompt=consent for Change workspace", async () => {
    const runDialog = vi.fn(dialogOk);
    const { client } = setup({ [TOKEN]: grant(jwt({}), "r"), "POST /v1/addin/microsoft/link": [() => json(200, connected("csms_1")), () => json(200, connected("csms_2"))] }, { runDialog });
    const controls = oauthControlsFor(client)!;
    await controls.signIn();
    await controls.signIn({ prompt: "consent" });
    expect(runDialog.mock.calls[1][0]).toMatchObject({ prompt: "consent" });
    expect(client.current()?.token).toBe("csms_2");
  });

  it("refreshes the Clerk token once and retries when the link answers 401", async () => {
    const { client, calls } = setup({
      [TOKEN]: [grant("access-1", "refresh-1"), grant("access-2", "refresh-2")],
      "POST /v1/addin/microsoft/link": [() => json(401, { error: "unauthorized" }), () => json(200, connected("csms_1"))],
    });
    await oauthControlsFor(client)!.signIn();
    const links = calls.filter((c) => c.url.endsWith("/v1/addin/microsoft/link"));
    expect(links.map((c) => c.headers.Authorization)).toEqual(["Bearer access-1", "Bearer access-2"]);
    expect(Object.fromEntries(new URLSearchParams(calls.filter((c) => c.url.endsWith("/oauth/token"))[1].body)).refresh_token).toBe("refresh-1");
  });

  it("propagates a cancelled dialog without touching the API", async () => {
    const { client, fetchImpl } = setup({}, { runDialog: vi.fn(async () => { throw new DialogCancelledError(); }) });
    await expect(oauthControlsFor(client)!.signIn()).rejects.toBeInstanceOf(DialogCancelledError);
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it("concurrent signIn calls share one dialog", async () => {
    const runDialog = vi.fn(dialogOk);
    const { client } = setup({ [TOKEN]: grant(jwt({}), "r"), "POST /v1/addin/microsoft/link": () => json(200, connected("csms_1")) }, { runDialog });
    const controls = oauthControlsFor(client)!;
    await Promise.all([controls.signIn(), controls.signIn()]);
    expect(runDialog).toHaveBeenCalledTimes(1);
  });
});

describe("sign-in without a Microsoft token: Clerk-direct", () => {
  const noMs = async (): Promise<string> => { throw new SsoUnavailableError(); };
  const access = jwt({ org_id: "org_1" });
  const idToken = jwt({ email: "ada@acme.test" });

  it("connects from the tokens and sends the Clerk access token on every call", async () => {
    const { client, calls } = setup({ [TOKEN]: grant(access, "refresh-1", { id_token: idToken }), "GET /v1/connectors/microsoft/watched": () => json(200, { ok: true }) }, { ms: noMs });
    const session = await oauthControlsFor(client)!.signIn();
    expect(session).toMatchObject({ token: access, account: { email: "ada@acme.test" }, workspace: { account_id: "org_1", kind: "org" } });
    expect(calls.some((c) => c.url.endsWith("/v1/addin/microsoft/link"))).toBe(false);
    await client.call("/v1/connectors/microsoft/watched");
    expect(calls.at(-1)!.headers.Authorization).toBe(`Bearer ${access}`);
  });

  it("on a 401 refreshes (rotating) and retries once", async () => {
    const { client, calls } = setup({
      [TOKEN]: [grant(access, "refresh-1", { id_token: idToken }), grant("access-2", "refresh-2")],
      "GET /v1/x": [() => json(401, { error: "unauthorized" }), () => json(200, { ok: true })],
    }, { ms: noMs });
    await oauthControlsFor(client)!.signIn();
    expect(await client.call("/v1/x")).toEqual({ ok: true });
    const xs = calls.filter((c) => c.url.endsWith("/v1/x"));
    expect(xs.map((c) => c.headers.Authorization)).toEqual([`Bearer ${access}`, "Bearer access-2"]);
    const refresh = calls.filter((c) => c.url.endsWith("/oauth/token"))[1];
    expect(Object.fromEntries(new URLSearchParams(refresh.body))).toMatchObject({ grant_type: "refresh_token", refresh_token: "refresh-1" });
  });

  it("invalid_grant ends the sign-in: the call fails as reconnect-required and the client is empty", async () => {
    const { client } = setup({
      [TOKEN]: [grant(access, "refresh-1", { id_token: idToken }), () => json(400, { error: "invalid_grant" })],
      "GET /v1/x": () => json(401, { error: "unauthorized" }),
    }, { ms: noMs });
    await oauthControlsFor(client)!.signIn();
    await expect(client.call("/v1/x")).rejects.toBeInstanceOf(ReconnectRequiredError);
    expect(client.current()).toBeNull();
  });

  it("disconnect clears the Clerk tokens before the revoke finishes", async () => {
    let release!: (r: Response) => void;
    const { client } = setup({
      [TOKEN]: grant(jwt({}), "refresh-1"),
      "POST /v1/addin/microsoft/link": () => json(200, connected("csms_1")),
      "DELETE /v1/addin/microsoft/link": () => new Promise<Response>((resolve) => { release = resolve; }),
    });
    const controls = oauthControlsFor(client)!;
    await controls.signIn();
    expect(controls.hasClerkTokens()).toBe(true);
    const pending = client.disconnect();
    await vi.waitFor(() => expect(release).toBeDefined());
    expect(controls.hasClerkTokens()).toBe(false); // revoke still pending
    release(json(204, null));
    expect(await pending).toEqual({ revoked: true });
  });

  it("disconnect forgets the tokens", async () => {
    const { client, calls } = setup({ [TOKEN]: grant(access, "refresh-1", { id_token: idToken }) }, { ms: noMs });
    await oauthControlsFor(client)!.signIn();
    await client.disconnect();
    expect(client.current()).toBeNull();
    // No Clerk credential is left to send: the call falls back to the (here unavailable) Microsoft path.
    await expect(client.call("/v1/x")).rejects.toBeInstanceOf(SsoUnavailableError);
    expect(calls.some((c) => c.url.endsWith("/v1/x"))).toBe(false);
  });
});

describe("Clerk-direct workspace lookup (GET /v1/addin/workspace)", () => {
  const noMs = async (): Promise<string> => { throw new SsoUnavailableError(); };
  const access = jwt({ org_id: "org_1" });
  const idToken = jwt({ email: "ada@acme.test" });
  const WS = "GET /v1/addin/workspace";

  it("uses name and account_id from the endpoint, sending the Clerk bearer", async () => {
    const { client, calls } = setup({
      [TOKEN]: grant(access, "refresh-1", { id_token: idToken }),
      [WS]: () => json(200, { kind: "org", name: "Acme ISD", account_id: "acct-o", org_id: "org_1" }),
    }, { ms: noMs });
    const session = await oauthControlsFor(client)!.signIn();
    expect(session.workspace).toEqual({ account_id: "acct-o", name: "Acme ISD", kind: "org" });
    expect(session.account.email).toBe("ada@acme.test");
    expect(client.current()?.workspace.name).toBe("Acme ISD");
    expect(calls.find((c) => c.url.endsWith("/v1/addin/workspace"))!.headers.Authorization).toBe(`Bearer ${access}`);
  });

  it("a personal workspace comes through as personal", async () => {
    const { client } = setup({
      [TOKEN]: grant(jwt({}), "refresh-1", { id_token: idToken }),
      [WS]: () => json(200, { kind: "personal", name: "Ada Lovelace", account_id: "acct-p", org_id: null }),
    }, { ms: noMs });
    expect((await oauthControlsFor(client)!.signIn()).workspace).toEqual({ account_id: "acct-p", name: "Ada Lovelace", kind: "personal" });
  });

  it("404 (backend not deployed) keeps today's behaviour, silently", async () => {
    const { client } = setup({ [TOKEN]: grant(access, "refresh-1", { id_token: idToken }), [WS]: () => json(404, { error: "not_found" }) }, { ms: noMs });
    const session = await oauthControlsFor(client)!.signIn();
    expect(session.workspace).toEqual({ account_id: "org_1", name: "Your organization", kind: "org" });
  });

  it("a 500, a network error or a malformed body also fall back", async () => {
    for (const route of [() => json(500, {}), () => json(200, { name: "", account_id: "x", kind: "org" }), () => json(200, { name: "A", kind: "org" })]) {
      const { client } = setup({ [TOKEN]: grant(jwt({}), "refresh-1", { id_token: idToken }), [WS]: route }, { ms: noMs });
      expect((await oauthControlsFor(client)!.signIn()).workspace).toEqual({ account_id: "personal", name: "Personal", kind: "personal" });
    }
    const failing = setup({ [TOKEN]: grant(jwt({}), "refresh-1", { id_token: idToken }), [WS]: () => { throw new TypeError("network"); } }, { ms: noMs });
    expect((await oauthControlsFor(failing.client)!.signIn()).workspace.name).toBe("Personal");
  });

  it("Change workspace (consent re-sign-in) looks the workspace up again", async () => {
    const access2 = jwt({ org_id: "org_2" });
    const { client, calls } = setup({
      [TOKEN]: [grant(access, "refresh-1", { id_token: idToken }), grant(access2, "refresh-2", { id_token: idToken })],
      [WS]: [
        () => json(200, { kind: "org", name: "Acme ISD", account_id: "acct-1", org_id: "org_1" }),
        () => json(200, { kind: "org", name: "Globex", account_id: "acct-2", org_id: "org_2" }),
      ],
    }, { ms: noMs });
    const controls = oauthControlsFor(client)!;
    await controls.signIn();
    const second = await controls.signIn({ prompt: "consent" });
    expect(second.workspace).toEqual({ account_id: "acct-2", name: "Globex", kind: "org" });
    expect(client.current()?.workspace.account_id).toBe("acct-2");
    expect(dialogOk).toHaveBeenLastCalledWith(expect.objectContaining({ prompt: "consent" }));
    const lookups = calls.filter((c) => c.url.endsWith("/v1/addin/workspace"));
    expect(lookups.map((c) => c.headers.Authorization)).toEqual([`Bearer ${access}`, `Bearer ${access2}`]);
  });

  it("a lookup that never answers times out and sign-in completes from the tokens", async () => {
    vi.useFakeTimers();
    try {
      const { client } = setup({ [TOKEN]: grant(access, "refresh-1", { id_token: idToken }), [WS]: () => new Promise<Response>(() => {}) }, { ms: noMs });
      const pending = oauthControlsFor(client)!.signIn();
      await vi.advanceTimersByTimeAsync(WORKSPACE_LOOKUP_TIMEOUT_MS + 1);
      expect((await pending).workspace).toEqual({ account_id: "org_1", name: "Your organization", kind: "org" });
    } finally { vi.useRealTimers(); }
  });

  it("the session reflects a token refreshed by a 401 retry inside the lookup", async () => {
    const { client } = setup({
      [TOKEN]: [grant(access, "refresh-1", { id_token: idToken }), grant("access-2", "refresh-2")],
      [WS]: [() => json(401, { error: "unauthorized" }), () => json(200, { kind: "org", name: "Acme ISD", account_id: "acct-o", org_id: "org_1" })],
    }, { ms: noMs });
    const session = await oauthControlsFor(client)!.signIn();
    expect(session).toMatchObject({ token: "access-2", workspace: { name: "Acme ISD" } });
  });

  it("disconnect during the lookup does not restore the session", async () => {
    let release!: (r: Response) => void;
    const { client } = setup({
      [TOKEN]: grant(access, "refresh-1", { id_token: idToken }),
      [WS]: () => new Promise<Response>((resolve) => { release = resolve; }),
    }, { ms: noMs });
    const controls = oauthControlsFor(client)!;
    const pending = controls.signIn();
    await vi.waitFor(() => expect(release).toBeDefined());
    await client.disconnect();
    release(json(200, { kind: "org", name: "Acme ISD", account_id: "acct-o", org_id: "org_1" }));
    await expect(pending).rejects.toBeInstanceOf(SignInRequiredError);
    expect(client.current()).toBeNull();
    expect(controls.hasClerkTokens()).toBe(false);
  });
});
