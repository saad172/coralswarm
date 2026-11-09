import { describe, expect, it, vi } from "vitest";
import { base64url, type ClerkOAuthConfig } from "./clerkOAuth";
import { MeetApiError, MeetSignedOutError, captureForMeeting, createMeetClient, type MeetCapture } from "./meetClient";

const CONFIG: ClerkOAuthConfig = { clientId: "example_18", fapi: "https://clerk.coralswarm.example" };
const API = "https://api.coralswarm.example";

function jwt(claims: Record<string, unknown>): string {
  const enc = (o: unknown) => base64url(new TextEncoder().encode(JSON.stringify(o)));
  return `${enc({ alg: "RS256" })}.${enc(claims)}.sig`;
}

function json(status: number, body: unknown) {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

/** The panel origin's localStorage, as one browser profile sees it across meetings. */
function memoryStorage(): Storage {
  const items = new Map<string, string>();
  return {
    get length() { return items.size; },
    key: (i: number) => Array.from(items.keys())[i] ?? null,
    getItem: (k: string) => items.get(k) ?? null,
    setItem: (k: string, v: string) => { items.set(k, String(v)); },
    removeItem: (k: string) => { items.delete(k); },
    clear: () => { items.clear(); },
  };
}

const SESSION_KEY = `coralswarm.meet.session.${CONFIG.clientId}`;

function harness(api: (url: string, init: RequestInit) => Response, storage: Storage | null = memoryStorage()) {
  let n = 0;
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    if (url.startsWith(`${CONFIG.fapi}/oauth/token`)) {
      n++;
      return json(200, { access_token: jwt({ sub: "user_a", org_id: "org_x", client_id: CONFIG.clientId, n }), refresh_token: `r${n}`, expires_in: 86400, id_token: jwt({ email: "ada@acme.test" }) });
    }
    return api(url, init ?? {});
  }) as unknown as typeof fetch;
  const runDialog = vi.fn(async () => ({ code: "c", verifier: "v", redirectUri: "https://app.coralswarm.example/addin/auth/callback" }));
  const client = createMeetClient({ apiBase: () => API, config: CONFIG, fetchImpl, runDialog, storage });
  return { client, fetchImpl: fetchImpl as unknown as ReturnType<typeof vi.fn>, runDialog };
}

const workspaceApi = (url: string) => (url.endsWith("/v1/addin/workspace") ? json(200, { account_id: "acc-1", name: "Acme", kind: "org" }) : json(200, { captures: [] }));
const bearerOf = (h: ReturnType<typeof harness>, path: string) => {
  const call = h.fetchImpl.mock.calls.find(([u]) => String(u) === `${API}${path}`);
  return ((call?.[1] as RequestInit | undefined)?.headers as Record<string, string> | undefined)?.Authorization ?? null;
};

describe("Meet client", () => {
  it("signs in through the meet popup target with the Meet client and reads the workspace label", async () => {
    const h = harness((url) => (url.endsWith("/v1/addin/workspace") ? json(200, { account_id: "acc-1", name: "Acme", kind: "org" }) : json(404, {})));
    const popup = { closed: false, location: { href: "about:blank" }, close: vi.fn() };
    const account = await h.client.signIn(popup);
    expect(h.runDialog).toHaveBeenCalledWith({ target: { kind: "meet", popup }, prompt: undefined });
    const tokenBody = String((h.fetchImpl.mock.calls[0][1] as RequestInit).body);
    expect(new URLSearchParams(tokenBody).get("client_id")).toBe("example_18");
    expect(account).toEqual({ email: "ada@acme.test", workspace: { account_id: "acc-1", name: "Acme", kind: "org" } });
    expect(h.client.signedIn()).toBe(true);
  });

  it("calls the capture routes with the bearer; maps the API's error code", async () => {
    const h = harness((url, init) => {
      if (url.endsWith("/v1/addin/workspace")) return json(500, {});
      if (init.method === "POST") return json(409, { error: "google_meet_not_connected", message: "connect" });
      return json(200, { captures: [] });
    });
    await h.client.signIn(null);
    await expect(h.client.addCapture("spaces/abc")).rejects.toMatchObject({ status: 409, code: "google_meet_not_connected" });
    const postCall = h.fetchImpl.mock.calls.find(([u, init]) => String(u) === `${API}/v1/connectors/google/meet/captures` && (init as RequestInit).method === "POST");
    expect(postCall).toBeDefined();
    const postInit = postCall?.[1] as RequestInit;
    expect(JSON.parse(String(postInit.body))).toEqual({ space: "spaces/abc" });
    expect((postInit.headers as Record<string, string>).Authorization).toMatch(/^Bearer /);
  });

  it("a 401 refreshes once and retries; a second 401 is a lost sign-in", async () => {
    let calls = 0;
    const h = harness((url) => {
      if (url.endsWith("/v1/addin/workspace")) return json(200, { account_id: "a", name: "P", kind: "personal" });
      calls++;
      return calls === 1 ? json(401, {}) : json(200, { captures: [] });
    });
    await h.client.signIn(null);
    await expect(h.client.listCaptures()).resolves.toEqual({ captures: [] });
    const always401 = harness(() => json(401, {}));
    await always401.client.signIn(null).catch(() => {});
    await expect(always401.client.listCaptures()).rejects.toBeInstanceOf(MeetSignedOutError);
    expect(always401.client.signedIn()).toBe(false);
  });

  it("without a Meet client id the client is disabled and never signs in", async () => {
    const client = createMeetClient({ apiBase: () => API, config: null });
    expect(client.enabled).toBe(false);
    const popup = { closed: false, location: { href: "about:blank" }, close: vi.fn() };
    await expect(client.signIn(popup)).rejects.toBeInstanceOf(MeetSignedOutError);
    expect(popup.close).toHaveBeenCalled();
    await expect(client.listCaptures()).rejects.toBeInstanceOf(MeetSignedOutError);
  });

  it("a new panel instance (the next meeting) resumes the sign-in without the popup", async () => {
    const storage = memoryStorage();
    const first = harness(workspaceApi, storage);
    const account = await first.client.signIn(null);
    const stored = storage.getItem(SESSION_KEY);
    expect(stored).not.toBeNull();

    const next = harness(workspaceApi, storage);
    expect(next.client.signedIn()).toBe(true);
    await expect(next.client.resume()).resolves.toEqual(account);
    expect(next.runDialog).not.toHaveBeenCalled();
    await next.client.listCaptures();
    expect(bearerOf(next, "/v1/connectors/google/meet/captures")).toBe(`Bearer ${JSON.parse(stored ?? "{}").accessToken}`);
  });

  it("signing out removes the record, so the next panel instance resumes to nothing", async () => {
    const storage = memoryStorage();
    const first = harness(workspaceApi, storage);
    await first.client.signIn(null);
    first.client.signOut();
    expect(storage.getItem(SESSION_KEY)).toBeNull();

    const next = harness(workspaceApi, storage);
    expect(next.client.signedIn()).toBe(false);
    await expect(next.client.resume()).resolves.toBeNull();
  });

  it("a malformed stored record is ignored", async () => {
    const storage = memoryStorage();
    storage.setItem(SESSION_KEY, "{not json");
    expect(harness(workspaceApi, storage).client.signedIn()).toBe(false);
    storage.setItem(SESSION_KEY, JSON.stringify({ accessToken: "", refreshToken: "r", expiresAt: 1, idToken: null }));
    const h = harness(workspaceApi, storage);
    expect(h.client.signedIn()).toBe(false);
    await expect(h.client.resume()).resolves.toBeNull();
  });

  it("storage that throws (third-party storage blocked) leaves the client memory-only", async () => {
    const blocked = { getItem() { throw new DOMException("blocked", "SecurityError"); }, setItem() { throw new DOMException("blocked", "SecurityError"); }, removeItem() { throw new DOMException("blocked", "SecurityError"); } } as unknown as Storage;
    const first = harness(workspaceApi, blocked);
    await expect(first.client.resume()).resolves.toBeNull();
    const account = await first.client.signIn(null);
    expect(account.email).toBe("ada@acme.test");
    expect(first.client.signedIn()).toBe(true);
    await expect(first.client.listCaptures()).resolves.toEqual({ captures: [] });

    const next = harness(workspaceApi, blocked);
    expect(next.client.signedIn()).toBe(false);
    await expect(next.client.resume()).resolves.toBeNull();
  });

  it("MeetApiError keeps status and code", () => {
    const e = new MeetApiError(403, "org_google_meet_disabled");
    expect([e.status, e.code]).toEqual([403, "org_google_meet_disabled"]);
  });
});

describe("captureForMeeting", () => {
  const row = (over: Partial<MeetCapture>): MeetCapture => ({ id: "x", conference_record_id: null, space_name: null, meeting_code: null, status: "allowed", created_at: "", expires_at: "", ...over });
  it("matches an open approval by space or code, newest first, ignoring ingested and revoked rows", () => {
    const meeting = { meetingId: "spaces/s1", meetingCode: "GWM-DVAO-JHB" };
    expect(captureForMeeting([row({ id: "a", space_name: "spaces/s1" })], meeting)?.id).toBe("a");
    expect(captureForMeeting([row({ id: "b", meeting_code: "gwm-dvao-jhb" })], meeting)?.id).toBe("b");
    expect(captureForMeeting([row({ id: "c", space_name: "spaces/s1", status: "ingested" }), row({ id: "d", space_name: "spaces/s1", status: "revoked" })], meeting)).toBeNull();
    expect(captureForMeeting([row({ id: "e", space_name: "spaces/s2" })], { meetingId: "spaces/s1", meetingCode: null })).toBeNull();
    expect(captureForMeeting([row({ id: "new", space_name: "spaces/s1" }), row({ id: "old", space_name: "spaces/s1" })], meeting)?.id).toBe("new");
  });
});
