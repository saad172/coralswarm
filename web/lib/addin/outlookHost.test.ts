// @vitest-environment jsdom
import { describe, expect, it, vi } from "vitest";
import { createOutlookHost, type NestedClient } from "./outlookHost";
import { MICROSOFT_APP_ID, MICROSOFT_SSO_SCOPE } from "./config";
import { SsoUnavailableError } from "./microsoftSso";
import type { OfficeApi } from "./office";

function office(opts: { naa?: boolean; officeAuth?: () => Promise<string>; openBrowser?: boolean } = {}): OfficeApi {
  return {
    auth: opts.officeAuth ? { getAccessToken: vi.fn(opts.officeAuth) } : undefined,
    context: {
      mailbox: { item: null, convertToRestId: (id: string) => id },
      ui: opts.openBrowser ? { openBrowserWindow: vi.fn() } : undefined,
      requirements: { isSetSupported: (name: string) => (name === "NestedAppAuth" ? opts.naa === true : name === "OpenBrowserWindowApi" ? opts.openBrowser === true : false) },
    },
  } as unknown as OfficeApi;
}

describe("scope", () => {
  it("asks for access_as_user on the CoralSwarm app", () => {
    expect(MICROSOFT_APP_ID).toBe("00000000-0000-0000-0000-000000000000");
    expect(MICROSOFT_SSO_SCOPE).toBe("api://app.coralswarm.example/00000000-0000-0000-0000-000000000000/access_as_user");
  });
});

describe("getMicrosoftToken", () => {
  it("uses nested app auth silently when available", async () => {
    const client: NestedClient = { acquireTokenSilent: vi.fn(async () => ({ accessToken: "naa-token" })), acquireTokenPopup: vi.fn() };
    const host = createOutlookHost({ office: async () => office({ naa: true }), createNested: async () => client });
    await expect(host.getMicrosoftToken()).resolves.toBe("naa-token");
    expect(client.acquireTokenSilent).toHaveBeenCalledWith({ scopes: [MICROSOFT_SSO_SCOPE] });
    expect(client.acquireTokenPopup).not.toHaveBeenCalled();
  });

  it("asks the user once (popup) when NAA needs interaction", async () => {
    const client: NestedClient = {
      acquireTokenSilent: vi.fn(async () => { throw Object.assign(new Error("ir"), { errorCode: "interaction_required" }); }),
      acquireTokenPopup: vi.fn(async () => ({ accessToken: "popup-token" })),
    };
    const host = createOutlookHost({ office: async () => office({ naa: true }), createNested: async () => client });
    await expect(host.getMicrosoftToken()).resolves.toBe("popup-token");
  });

  it("falls back to Office.auth.getAccessToken when NAA is not supported", async () => {
    const createNested = vi.fn();
    const host = createOutlookHost({ office: async () => office({ naa: false, officeAuth: async () => "office-token" }), createNested });
    await expect(host.getMicrosoftToken()).resolves.toBe("office-token");
    expect(createNested).not.toHaveBeenCalled();
  });

  it("falls back to Office.auth when NAA is advertised but fails", async () => {
    const host = createOutlookHost({ office: async () => office({ naa: true, officeAuth: async () => "office-token" }), createNested: async () => { throw new Error("boom"); } });
    await expect(host.getMicrosoftToken()).resolves.toBe("office-token");
  });

  it("reports SSO unavailable (the Clerk-fallback signal) when neither path yields a token", async () => {
    const host = createOutlookHost({ office: async () => office({ naa: false, officeAuth: async () => { throw new Error("13003") } }) });
    await expect(host.getMicrosoftToken()).rejects.toBeInstanceOf(SsoUnavailableError);
  });

  it("reports SSO unavailable outside an Office host", async () => {
    const host = createOutlookHost({ office: async () => { throw new Error("Office.js failed to load"); } });
    await expect(host.getMicrosoftToken()).rejects.toBeInstanceOf(SsoUnavailableError);
  });
});

describe("openLink", () => {
  it("uses Office's browser window when available", async () => {
    const o = office({ openBrowser: true });
    await createOutlookHost({ office: async () => o }).openLink("https://app.coralswarm.example/addin/approve?code=X");
    expect(o.context.ui?.openBrowserWindow).toHaveBeenCalledWith("https://app.coralswarm.example/addin/approve?code=X");
  });

  it("falls back to window.open without an opener", async () => {
    const open = vi.spyOn(window, "open").mockReturnValue(null);
    await createOutlookHost({ office: async () => office() }).openLink("https://app.coralswarm.example/today");
    expect(open).toHaveBeenCalledWith("https://app.coralswarm.example/today", "_blank", "noopener,noreferrer");
    open.mockRestore();
  });
});

describe("warm (startup runs in parallel, not in series)", () => {
  // Simulated phase costs. The old path was office.js, THEN the MSAL chunk, THEN the client, THEN the token: 200+150+100+100.
  const OFFICE_MS = 200, MSAL_MS = 150, NESTED_MS = 100, SILENT_MS = 100;
  const delay = <T,>(ms: number, value: T) => new Promise<T>((resolve) => setTimeout(() => resolve(value), ms));

  function timed() {
    const events: Array<{ what: string; at: number }> = [];
    const t0 = Date.now();
    const log = (what: string) => events.push({ what, at: Date.now() - t0 });
    const client: NestedClient = { acquireTokenSilent: vi.fn(async () => { log("silent-start"); await delay(SILENT_MS, 0); return { accessToken: "naa-token" }; }), acquireTokenPopup: vi.fn() };
    const deps = {
      office: vi.fn(async () => { log("office-start"); await delay(OFFICE_MS, 0); return office({ naa: true }); }),
      loadMsal: vi.fn(async () => { log("msal-start"); await delay(MSAL_MS, 0); }),
      createNested: vi.fn(async () => { log("nested-start"); await delay(NESTED_MS, 0); return client; }),
    };
    return { events, deps, at: (what: string) => events.find((e) => e.what === what)?.at };
  }

  it("starts office.js and the MSAL download together, and the client the moment office.js is ready", async () => {
    vi.useFakeTimers();
    try {
      const t = timed();
      const host = createOutlookHost(t.deps);
      void host.warm();
      await vi.advanceTimersByTimeAsync(OFFICE_MS + NESTED_MS + 10);
      expect(t.at("office-start")).toBe(0);
      expect(t.at("msal-start")).toBe(0);                // not after office.js
      expect(t.at("nested-start")).toBe(OFFICE_MS);      // not after office.js + the MSAL download
    } finally { vi.useRealTimers(); }
  });

  it("a token asked for during warm-up arrives at office + client + silent (400 ms), not the serial 550 ms", async () => {
    vi.useFakeTimers();
    try {
      const t = timed();
      const host = createOutlookHost(t.deps);
      void host.warm();
      const started = Date.now();
      let done: number | null = null;
      const token = host.getMicrosoftToken().then((value) => { done = Date.now() - started; return value; });
      await vi.advanceTimersByTimeAsync(OFFICE_MS + NESTED_MS + SILENT_MS + 10);
      await expect(token).resolves.toBe("naa-token");
      expect(done).toBe(OFFICE_MS + NESTED_MS + SILENT_MS);
      expect(done ?? Infinity).toBeLessThan(OFFICE_MS + MSAL_MS + NESTED_MS + SILENT_MS);
    } finally { vi.useRealTimers(); }
  });

  it("is idempotent: one office load, one MSAL download, one client, however often it is called", async () => {
    const t = timed();
    const host = createOutlookHost({ ...t.deps, office: vi.fn(async () => office({ naa: true })), loadMsal: vi.fn(async () => {}) });
    await Promise.all([host.warm(), host.warm()]);
    await host.getMicrosoftToken();
    await host.getMicrosoftToken();
    expect(host.warm()).toBeInstanceOf(Promise);
    expect(t.deps.createNested).toHaveBeenCalledTimes(1);
  });

  it("never starts the NAA client when the host does not offer NAA", async () => {
    const createNested = vi.fn(async () => ({ acquireTokenSilent: vi.fn(), acquireTokenPopup: vi.fn() }) as NestedClient);
    const host = createOutlookHost({ office: async () => office({ naa: false }), loadMsal: async () => {}, createNested });
    await host.warm();
    expect(createNested).not.toHaveBeenCalled();
  });

  it("never rejects, and a failed office.js load still surfaces as SSO unavailable from getMicrosoftToken", async () => {
    const host = createOutlookHost({ office: async () => { throw new Error("offline"); }, loadMsal: async () => { throw new Error("chunk"); } });
    await expect(host.warm()).resolves.toBeUndefined();
    await expect(host.getMicrosoftToken()).rejects.toBeInstanceOf(SsoUnavailableError);
  });

  it("a warm-up failure of the client does not poison the token request: it falls back to Office SSO", async () => {
    const host = createOutlookHost({
      office: async () => office({ naa: true, officeAuth: async () => "office-token" }), loadMsal: async () => {},
      createNested: async () => { throw new Error("bridge"); },
    });
    await host.warm();
    await expect(host.getMicrosoftToken()).resolves.toBe("office-token");
  });
});
