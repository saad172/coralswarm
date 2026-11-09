// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { OutlookMailPane } from "./OutlookMailPane";
import { SsoUnavailableError } from "@/lib/addin/microsoftSso";
import type { OfficeApi, OfficeMailItem } from "@/lib/addin/office";

const h = vi.hoisted(() => ({ office: null as unknown, token: vi.fn(), openLink: vi.fn() }));
vi.mock("@/lib/addin/outlookHost", () => ({
  createOutlookHost: () => ({ name: "outlook", office: async () => h.office, getMicrosoftToken: () => h.token(), openLink: h.openLink, warm: async () => {} }),
}));
// The Clerk fallback is covered separately; here it only needs to be recognisable.
vi.mock("./ClerkEmailPane", () => ({ ClerkEmailPane: () => <div role="region" aria-label="Clerk fallback" /> }));

const org = { account_id: "acct-o", name: "Acme ISD", kind: "org" };
const connected = { status: "connected", token: "cs-1", expires_at: "2099-01-01T00:00:00Z", account: { email: "ada@acme.test" }, workspace: org };
const message = (over: Partial<OfficeMailItem> = {}): OfficeMailItem => ({ itemType: "message", itemId: "AAMk-1", conversationId: "AAQk-1", subject: "Board packet", from: { displayName: "Grace Hopper" }, ...over });

function fakeOffice(item: OfficeMailItem | null) {
  let handler: (() => void) | null = null;
  const office = {
    EventType: { ItemChanged: "itemChanged" },
    MailboxEnums: { ItemType: { Message: "message", Appointment: "appointment" }, RestVersion: { v2_0: "v2.0" } },
    context: {
      requirements: { isSetSupported: () => false },
      mailbox: { item, convertToRestId: (id: string) => `rest:${id}`, addHandlerAsync: (_e: string, cb: () => void) => { handler = cb; }, removeHandlerAsync: vi.fn() },
    },
  } as unknown as OfficeApi;
  return { office, change: (next: OfficeMailItem | null) => { office.context.mailbox.item = next; act(() => handler?.()); } };
}

type Route = (init: RequestInit) => { status: number; body?: unknown };
function stubFetch(routes: Record<string, Route | Route[]>) {
  const log: Array<{ key: string; body: unknown }> = [];
  const counts = new Map<string, number>();
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    const key = `${init?.method ?? "GET"} ${url.pathname}`;
    log.push({ key, body: init?.body ? JSON.parse(String(init.body)) : undefined });
    const entry = routes[key];
    if (!entry) return new Response(JSON.stringify({ error: "unrouted" }), { status: 500 });
    const n = counts.get(key) ?? 0; counts.set(key, n + 1);
    const route = Array.isArray(entry) ? entry[Math.min(n, entry.length - 1)] : entry;
    const out = route(init ?? {});
    return new Response(out.status === 204 ? null : JSON.stringify(out.body ?? {}), { status: out.status });
  }));
  return log;
}
const SESSION = "POST /v1/addin/microsoft/session";
const WATCHED = "GET /v1/connectors/microsoft/watched/rest%3AAAQk-1";
const EVERYTHING_GET = "GET /v1/connectors/microsoft/everything-mode";
const EVERYTHING_POST = "POST /v1/connectors/microsoft/everything-mode";

beforeEach(() => { vi.clearAllMocks(); h.token.mockResolvedValue("ms-token"); });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

describe("email pane, SSO path", () => {
  it("connects from the Microsoft token (no Clerk) and shows the open message's save card", async () => {
    h.office = fakeOffice(message()).office;
    const log = stubFetch({ [SESSION]: () => ({ status: 200, body: connected }), [WATCHED]: () => ({ status: 200, body: { following: false } }), [EVERYTHING_GET]: () => ({ status: 200, body: { enabled: false } }) });
    render(<OutlookMailPane />);
    expect(await screen.findByRole("button", { name: "Save to CoralSwarm" })).toBeVisible();
    expect(screen.getByText("Board packet")).toBeVisible();
    expect(screen.getByText("ada@acme.test")).toBeVisible();
    expect(screen.queryByRole("region", { name: "Clerk fallback" })).not.toBeInTheDocument();
    expect(log[0]).toEqual({ key: SESSION, body: { ms_token: "ms-token" } });
  });

  it("sends the save with the add-in session token as the bearer", async () => {
    h.office = fakeOffice(message()).office;
    const calls: RequestInit[] = [];
    stubFetch({
      [SESSION]: () => ({ status: 200, body: connected }), [WATCHED]: () => ({ status: 200, body: { following: false } }), [EVERYTHING_GET]: () => ({ status: 404 }),
      "POST /v1/connectors/microsoft/ingest": (init) => { calls.push(init); return { status: 200, body: { ingested: 3 } }; },
    });
    render(<OutlookMailPane />);
    await screen.findByRole("radio", { name: "Whole thread" }); // the watched check has settled: knobs rendered
    fireEvent.click(await screen.findByRole("button", { name: "Save to CoralSwarm" }));
    expect(await screen.findByText("Saved to CoralSwarm · 3 messages")).toBeVisible();
    expect((calls[0].headers as Record<string, string>).Authorization).toBe("Bearer cs-1");
    expect(JSON.parse(String(calls[0].body))).toEqual({ item_ids: ["rest:AAMk-1"], scope: "thread", follow: true });
  });

  it("a 401 on save, with the link gone, returns to the start card with the reconnect toast", async () => {
    h.office = fakeOffice(message()).office;
    stubFetch({
      [SESSION]: [() => ({ status: 200, body: connected }), () => ({ status: 200, body: { status: "link_required", pairing: { code: "ABCD1234", poll_secret: "s", approve_url: "https://app.coralswarm.example/addin/approve?code=ABCD1234&kind=microsoft", expires_at: "2099-01-01T00:00:00Z" } } })],
      [WATCHED]: () => ({ status: 200, body: { following: false } }), [EVERYTHING_GET]: () => ({ status: 404 }),
      "POST /v1/connectors/microsoft/ingest": () => ({ status: 401 }),
    });
    render(<OutlookMailPane />);
    fireEvent.click(await screen.findByRole("button", { name: "Save to CoralSwarm" }));
    expect(await screen.findByText("CoralSwarm needs to be reconnected.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Connect CoralSwarm" })).toBeVisible();
    expect(screen.getByText("Your inbox, in your workspace")).toBeVisible();
  });

  it("no message open shows the home state; opening a message swaps in its card (and a different message resets it)", async () => {
    const fake = fakeOffice(null);
    h.office = fake.office;
    stubFetch({ [SESSION]: () => ({ status: 200, body: connected }), [WATCHED]: () => ({ status: 200, body: { following: false } }), [EVERYTHING_GET]: () => ({ status: 404 }) });
    render(<OutlookMailPane />);
    expect(await screen.findByText("Open a message to save its thread")).toBeVisible();
    expect(screen.getByText("Nothing from Outlook yet")).toBeVisible();
    fake.change(message());
    expect(await screen.findByText("Board packet")).toBeVisible();
    fake.change(message({ itemId: "AAMk-2", subject: "Second subject" }));
    expect(await screen.findByText("Second subject")).toBeVisible();
    expect(screen.queryByText("Board packet")).not.toBeInTheDocument();
  });

  it("falls back to the existing Clerk pane when the flag is off", async () => {
    h.office = fakeOffice(message()).office;
    stubFetch({ [SESSION]: () => ({ status: 404, body: { error: "microsoft_sso_disabled" } }) });
    render(<OutlookMailPane />);
    expect(await screen.findByRole("region", { name: "Clerk fallback" })).toBeVisible();
  });

  it("falls back to the existing Clerk pane when Outlook cannot issue a Microsoft token", async () => {
    h.office = fakeOffice(message()).office;
    h.token.mockRejectedValue(new SsoUnavailableError());
    const log = stubFetch({});
    render(<OutlookMailPane />);
    expect(await screen.findByRole("region", { name: "Clerk fallback" })).toBeVisible();
    expect(log).toHaveLength(0);
  });
});

describe("whole mailbox", () => {
  async function connectedPane(enabled: boolean, extra: Record<string, Route> = {}) {
    h.office = fakeOffice(message()).office;
    const log = stubFetch({
      [SESSION]: () => ({ status: 200, body: connected }), [WATCHED]: () => ({ status: 200, body: { following: false } }),
      [EVERYTHING_GET]: [() => ({ status: 200, body: { enabled } }), () => ({ status: 200, body: { enabled: !enabled } })], ...extra,
    });
    render(<OutlookMailPane />);
    await screen.findByRole("switch", { name: "Ingest my whole mailbox" });
    return log;
  }

  it("turning it on shows the confirm card first and writes nothing until Turn on", async () => {
    const log = await connectedPane(false, { [EVERYTHING_POST]: () => ({ status: 200, body: { enabled: true } }) });
    fireEvent.click(screen.getByRole("switch", { name: "Ingest my whole mailbox" }));
    expect(await screen.findByText("Every message, including existing mail")).toBeVisible();
    expect(screen.getByText("Into your private workspace only")).toBeVisible();
    expect(screen.getByText("Nothing is shared unless you promote it yourself.")).toBeVisible();
    expect(screen.getByText("Turn it off any time")).toBeVisible();
    expect(log.some((entry) => entry.key === EVERYTHING_POST)).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Turn on" }));
    expect(await screen.findByText("Whole-mailbox ingest is on. Your mailbox is being pulled in.")).toBeVisible();
    expect(log.find((entry) => entry.key === EVERYTHING_POST)?.body).toEqual({ enabled: true });
    expect(await screen.findByRole("switch", { name: "Ingest my whole mailbox" })).toBeChecked();
  });

  it("Cancel goes back without writing", async () => {
    const log = await connectedPane(false);
    fireEvent.click(screen.getByRole("switch", { name: "Ingest my whole mailbox" }));
    fireEvent.click(await screen.findByRole("button", { name: "Cancel" }));
    expect(await screen.findByRole("switch", { name: "Ingest my whole mailbox" })).not.toBeChecked();
    expect(log.some((entry) => entry.key === EVERYTHING_POST)).toBe(false);
  });

  it("turning it off needs no confirmation", async () => {
    const log = await connectedPane(true, { [EVERYTHING_POST]: () => ({ status: 200, body: { enabled: false } }) });
    fireEvent.click(screen.getByRole("switch", { name: "Ingest my whole mailbox" }));
    expect(await screen.findByText("Whole-mailbox ingest is off. Saved mail stays.")).toBeVisible();
    expect(log.find((entry) => entry.key === EVERYTHING_POST)?.body).toEqual({ enabled: false });
    expect(screen.queryByText("Every message, including existing mail")).not.toBeInTheDocument();
  });

  it("says so when the workspace does not allow it (403)", async () => {
    await connectedPane(false, { [EVERYTHING_POST]: () => ({ status: 403 }) });
    fireEvent.click(screen.getByRole("switch", { name: "Ingest my whole mailbox" }));
    fireEvent.click(await screen.findByRole("button", { name: "Turn on" }));
    expect(await screen.findByText("Whole-mailbox ingest is not available for this workspace.")).toBeVisible();
  });

  it("hides the switch when its state cannot be read", async () => {
    h.office = fakeOffice(message()).office;
    stubFetch({ [SESSION]: () => ({ status: 200, body: connected }), [WATCHED]: () => ({ status: 200, body: { following: false } }), [EVERYTHING_GET]: () => ({ status: 403 }) });
    render(<OutlookMailPane />);
    await screen.findByRole("button", { name: "Save to CoralSwarm" });
    await waitFor(() => expect(screen.queryByRole("switch", { name: "Ingest my whole mailbox" })).not.toBeInTheDocument());
  });
});
