import { describe, expect, it, vi } from "vitest";
import { DialogCancelledError, DialogError, interpretCallbackMessage, openSignInPopup, runAuthDialog, type MessageSource } from "./authDialog";
import { parseStartParams } from "./clerkOAuth";
import type { OfficeApi, OfficeDialog } from "./office";

const ORIGIN = "https://app.coralswarm.example";
type Handler = (arg: { message?: string; origin?: string; error?: number }) => void;

/** A fake Office with a controllable dialog. */
function fakeOffice(opts: { openStatus?: string } = {}) {
  const handlers = new Map<string, Handler>();
  const dialog: OfficeDialog & { closed: number } = {
    closed: 0,
    addEventHandler: (event, handler) => { handlers.set(event, handler); },
    close() { this.closed++; },
  };
  const displayDialogAsync = vi.fn((_url: string, _options: Record<string, unknown>, cb: (r: { status: string; value: OfficeDialog; error?: { code?: number } }) => void) => {
    cb(opts.openStatus && opts.openStatus !== "succeeded" ? { status: opts.openStatus, value: dialog, error: { code: 12007 } } : { status: "succeeded", value: dialog });
  });
  const office = {
    EventType: { ItemChanged: "ItemChanged", DialogMessageReceived: "msg", DialogEventReceived: "evt" },
    AsyncResultStatus: { Succeeded: "succeeded", Failed: "failed" },
    context: { ui: { displayDialogAsync } },
  } as unknown as OfficeApi;
  return { office, dialog, displayDialogAsync, send: (arg: Parameters<Handler>[0]) => handlers.get("msg")?.(arg), event: (arg: Parameters<Handler>[0]) => handlers.get("evt")?.(arg) };
}

/** Start the dialog and pull out the state the pane generated from the URL it opened. */
async function start(f: ReturnType<typeof fakeOffice>, prompt?: "consent") {
  const result = runAuthDialog({ target: { kind: "outlook", office: async () => f.office }, origin: ORIGIN, prompt });
  await vi.waitFor(() => expect(f.displayDialogAsync).toHaveBeenCalled());
  const url = new URL(f.displayDialogAsync.mock.calls[0][0]);
  return { result, url, state: url.searchParams.get("state") ?? "" };
}

describe("Outlook dialog", () => {
  it("opens our start page with the challenge and state, never the verifier, without the Allow prompt", async () => {
    const f = fakeOffice();
    const { result, url, state } = await start(f);
    expect(url.origin + url.pathname).toBe(`${ORIGIN}/addin/auth/start`);
    expect(parseStartParams(url.search)).toMatchObject({ host: "outlook", prompt: null, state });
    expect(f.displayDialogAsync.mock.calls[0][1]).toMatchObject({ promptBeforeOpen: false });
    f.send({ origin: ORIGIN, message: JSON.stringify({ code: "the-code", state }) });
    const outcome = await result;
    expect(outcome.code).toBe("the-code");
    expect(outcome.redirectUri).toBe(`${ORIGIN}/addin/auth/callback`);
    expect(outcome.verifier).toMatch(/^[A-Za-z0-9_-]{64}$/);
    expect(url.toString()).not.toContain(outcome.verifier);
    expect(f.dialog.closed).toBe(1);
  });

  it("asks for the consent screen when changing workspace", async () => {
    const f = fakeOffice();
    const { result, url, state } = await start(f, "consent");
    expect(url.searchParams.get("prompt")).toBe("consent");
    f.send({ origin: ORIGIN, message: JSON.stringify({ code: "c", state }) });
    await result;
  });

  it("ignores a message from another origin and keeps waiting", async () => {
    const f = fakeOffice();
    const { result, state } = await start(f);
    f.send({ origin: "https://evil.example", message: JSON.stringify({ code: "stolen", state }) });
    expect(f.dialog.closed).toBe(0);
    f.send({ origin: ORIGIN, message: JSON.stringify({ code: "real", state }) });
    expect((await result).code).toBe("real");
  });

  it("with no origin, ignores a forged message and resolves with the real one", async () => {
    const f = fakeOffice();
    const { result, state } = await start(f);
    f.send({ message: JSON.stringify({ code: "forged", state: "o" + "x".repeat(32) }) });
    f.send({ message: "not json" });
    expect(f.dialog.closed).toBe(0);
    f.send({ message: JSON.stringify({ code: "real", state }) });
    expect((await result).code).toBe("real");
  });

  it("rejects a state mismatch and closes the dialog", async () => {
    const f = fakeOffice();
    const { result } = await start(f);
    f.send({ origin: ORIGIN, message: JSON.stringify({ code: "c", state: "o" + "x".repeat(32) }) });
    await expect(result).rejects.toMatchObject({ name: "DialogError", code: "state_mismatch" });
    expect(f.dialog.closed).toBe(1);
  });

  it("a message after the first is ignored (state is checked once)", async () => {
    const f = fakeOffice();
    const { result, state } = await start(f);
    f.send({ origin: ORIGIN, message: JSON.stringify({ code: "first", state }) });
    f.send({ origin: ORIGIN, message: JSON.stringify({ code: "second", state }) });
    expect((await result).code).toBe("first");
  });

  it("treats a user-closed dialog (12006) as cancelled", async () => {
    const f = fakeOffice();
    const { result } = await start(f);
    f.event({ error: 12006 });
    await expect(result).rejects.toBeInstanceOf(DialogCancelledError);
  });

  it("other dialog events are errors, not cancellations", async () => {
    const f = fakeOffice();
    const { result } = await start(f);
    f.event({ error: 12002 });
    await expect(result).rejects.toBeInstanceOf(DialogError);
    await expect(result).rejects.not.toBeInstanceOf(DialogCancelledError);
  });

  it("declined consent (access_denied) is cancelled", async () => {
    const f = fakeOffice();
    const { result, state } = await start(f);
    f.send({ origin: ORIGIN, message: JSON.stringify({ error: "access_denied", state }) });
    await expect(result).rejects.toBeInstanceOf(DialogCancelledError);
  });

  it("reports a dialog that will not open", async () => {
    const f = fakeOffice({ openStatus: "failed" });
    await expect(runAuthDialog({ target: { kind: "outlook", office: async () => f.office }, origin: ORIGIN })).rejects.toMatchObject({ code: "open_failed_12007" });
  });
});

describe("Teams dialog", () => {
  it("authenticates against our start page and returns the code", async () => {
    const authenticate = vi.fn(async (params: { url: string }) => JSON.stringify({ code: "tc", state: new URL(params.url).searchParams.get("state") }));
    const outcome = await runAuthDialog({ target: { kind: "teams", authenticate }, origin: ORIGIN });
    expect(outcome.code).toBe("tc");
    expect(parseStartParams(new URL(authenticate.mock.calls[0][0].url).search)).toMatchObject({ host: "teams" });
  });

  it("maps a closed popup to cancelled and other failures to errors", async () => {
    await expect(runAuthDialog({ target: { kind: "teams", authenticate: async () => { throw new Error("CancelledByUser"); } }, origin: ORIGIN })).rejects.toBeInstanceOf(DialogCancelledError);
    await expect(runAuthDialog({ target: { kind: "teams", authenticate: async () => { throw "access_denied"; } }, origin: ORIGIN })).rejects.toBeInstanceOf(DialogCancelledError);
    await expect(runAuthDialog({ target: { kind: "teams", authenticate: async () => { throw new Error("FailedToOpenWindow"); } }, origin: ORIGIN })).rejects.toBeInstanceOf(DialogError);
  });

  it("rejects a result whose state is not ours", async () => {
    await expect(runAuthDialog({ target: { kind: "teams", authenticate: async () => JSON.stringify({ code: "c", state: "t" + "z".repeat(32) }) }, origin: ORIGIN })).rejects.toMatchObject({ code: "state_mismatch" });
  });
});

describe("Meet popup", () => {
  type Listener = (event: MessageEvent) => void;
  function fakePopup() {
    const listeners = new Set<Listener>();
    const events: MessageSource = { addEventListener: (_t, l) => { listeners.add(l); }, removeEventListener: (_t, l) => { listeners.delete(l); } };
    const popup = { closed: false, location: { href: "about:blank" }, close: vi.fn(function (this: { closed: boolean }) { this.closed = true; }) };
    const post = (data: unknown, opts: { origin?: string; source?: unknown } = {}) => {
      for (const l of [...listeners]) l({ data, origin: opts.origin ?? ORIGIN, source: "source" in opts ? opts.source : popup } as unknown as MessageEvent);
    };
    return { popup, events, post, listeners };
  }
  async function begin(f: ReturnType<typeof fakePopup>, prompt?: "consent") {
    const result = runAuthDialog({ target: { kind: "meet", popup: f.popup, events: f.events, pollMs: 5 }, origin: ORIGIN, prompt });
    await vi.waitFor(() => expect(f.popup.location.href).not.toBe("about:blank"));
    const url = new URL(f.popup.location.href);
    return { result, url, state: url.searchParams.get("state") ?? "" };
  }

  it("navigates the pre-opened popup to our start page with a g state and resolves on the callback's message", async () => {
    const f = fakePopup();
    const { result, url, state } = await begin(f);
    expect(url.origin + url.pathname).toBe(`${ORIGIN}/addin/auth/start`);
    expect(parseStartParams(url.search)).toMatchObject({ host: "meet", state, prompt: null });
    f.post({ code: "meet-code", state });
    const outcome = await result;
    expect(outcome.code).toBe("meet-code");
    expect(outcome.redirectUri).toBe(`${ORIGIN}/addin/auth/callback`);
    expect(url.toString()).not.toContain(outcome.verifier);
    expect(f.listeners.size).toBe(0);
  });

  it("ignores messages from another origin, another window, or with another state", async () => {
    const f = fakePopup();
    const { result, state } = await begin(f, "consent");
    expect(new URL(f.popup.location.href).searchParams.get("prompt")).toBe("consent");
    f.post({ code: "stolen", state }, { origin: "https://meet.google.com" });
    f.post({ code: "stolen", state }, { source: {} });
    f.post({ code: "stolen", state: "g" + "x".repeat(32) });
    f.post("not json");
    expect(f.listeners.size).toBe(1);
    f.post(JSON.stringify({ code: "real", state }));
    expect((await result).code).toBe("real");
  });

  it("a closed popup is a cancel", async () => {
    const f = fakePopup();
    const { result } = await begin(f);
    f.popup.closed = true;
    await expect(result).rejects.toBeInstanceOf(DialogCancelledError);
    expect(f.listeners.size).toBe(0);
  });

  it("declined consent is a cancel; no popup at all is a blocked error", async () => {
    const f = fakePopup();
    const { result, state } = await begin(f);
    f.post({ error: "access_denied", state });
    await expect(result).rejects.toBeInstanceOf(DialogCancelledError);
    await expect(runAuthDialog({ target: { kind: "meet", popup: null }, origin: ORIGIN })).rejects.toMatchObject({ code: "popup_blocked" });
  });

  it("openSignInPopup opens about:blank synchronously and returns null when blocked", () => {
    const open = vi.fn(() => ({}) as Window);
    expect(openSignInPopup({ open })).not.toBeNull();
    expect(open).toHaveBeenCalledWith("about:blank", "coralswarm-signin", expect.stringContaining("popup"));
    expect(openSignInPopup({ open: () => null })).toBeNull();
    expect(openSignInPopup({ open: () => { throw new Error("blocked"); } })).toBeNull();
  });
});

describe("interpretCallbackMessage", () => {
  it("rejects malformed input", () => {
    for (const bad of ["not json", "null", "[]", JSON.stringify({ state: "s" }), JSON.stringify({ code: "", state: "s" })]) expect(() => interpretCallbackMessage(bad, "s")).toThrow(DialogError);
    expect(interpretCallbackMessage(JSON.stringify({ code: "c", state: "s" }), "s")).toBe("c");
  });
});
