// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AddinApp } from "./AddinApp";
import { HomeBody } from "./screens";
import { isSafeApprovalUrl, POLL_INTERVAL_MS } from "./useAddinConnection";
import { createAddinClient, SsoUnavailableError, type AddinHost } from "@/lib/addin/microsoftSso";

const org = { account_id: "acct-o", name: "Acme ISD", kind: "org" };
const personal = { account_id: "acct-p", name: "Personal", kind: "personal" };
const approveUrl = "https://app.coralswarm.example/addin/approve?code=ABCD1234&kind=microsoft";
const connected = (token = "cs-1", workspace = org, extra = {}) => ({ status: "connected", token, expires_at: "2099-01-01T00:00:00Z", account: { email: "ada@acme.test" }, workspace, ...extra });
const linkRequired = { status: "link_required", pairing: { code: "ABCD1234", poll_secret: "sec", approve_url: approveUrl, expires_at: "2099-01-01T00:00:00Z" } };

type Route = (init: RequestInit) => { status: number; body?: unknown };
function harness(routes: Record<string, Route | Route[]>, ms: () => Promise<string> = async () => "ms") {
  const log: string[] = [];
  const counters = new Map<string, number>();
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input).replace("https://api.test", "");
    const key = `${init?.method ?? "GET"} ${path}`;
    log.push(key);
    const entry = routes[key];
    if (!entry) return new Response(JSON.stringify({ error: "unrouted" }), { status: 500 });
    const n = counters.get(key) ?? 0; counters.set(key, n + 1);
    const route = Array.isArray(entry) ? entry[Math.min(n, entry.length - 1)] : entry;
    const out = route(init ?? {});
    return new Response(out.status === 204 ? null : JSON.stringify(out.body ?? {}), { status: out.status });
  }) as unknown as typeof fetch;
  const openLink = vi.fn(async () => {});
  const host: AddinHost = { name: "teams", getMicrosoftToken: ms, openLink };
  const client = createAddinClient({ host, apiBase: () => "https://api.test", fetchImpl });
  return { client, host, openLink, log, fetchImpl };
}

const SESSION = "POST /v1/addin/microsoft/session";
const POLL = "POST /v1/addin/pair/poll";

function mount(h: ReturnType<typeof harness>, fallback = <p>CLERK FALLBACK</p>) {
  return render(
    <AddinApp client={h.client} host={h.host} surface="meeting" label="CoralSwarm meeting" fallback={fallback}>
      {(ctx) => <div><p>connected-content:{ctx.session.workspace.name}</p><button type="button" onClick={ctx.reconnectRequired}>force-reconnect</button><button type="button" onClick={ctx.openWorkspacePicker}>open-picker</button><HomeBody surface="meeting" /></div>}
    </AddinApp>,
  );
}

beforeEach(() => { vi.clearAllMocks(); });
afterEach(() => { cleanup(); vi.useRealTimers(); });

describe("start card", () => {
  it("shows the Gmail-style start card with meeting value rows and the Connect footer", async () => {
    const h = harness({ [SESSION]: () => ({ status: 200, body: linkRequired }) });
    mount(h);
    expect(await screen.findByRole("button", { name: "Connect CoralSwarm" })).toBeVisible();
    expect(screen.getByText("Your meetings, in your workspace")).toBeVisible();
    expect(screen.getByText("Save any meeting")).toBeVisible();
    expect(screen.getByText("Transcripts follow automatically")).toBeVisible();
    expect(screen.getByText("Nothing to copy")).toBeVisible();
    expect(screen.getByText("Approve once in your browser.")).toBeVisible();
    // The old Outlook wordmark header and the removed vocabulary must not exist.
    expect(screen.queryByText(/^coralswarm$/)).not.toBeInTheDocument();
    expect(document.body.textContent).not.toMatch(/Grow|Change ocean|capture/i);
  });

  it("goes straight to connected when the backend auto-links (no pairing screen)", async () => {
    // Connected → Disconnect → Connect: the backend links this Microsoft identity again without a pairing.
    const h = harness({
      [SESSION]: [() => ({ status: 200, body: connected() }), () => ({ status: 200, body: connected("cs-2", org, { auto_linked: true }) })],
      "DELETE /v1/addin/microsoft/link": () => ({ status: 204 }),
    });
    mount(h);
    await screen.findByText("connected-content:Acme ISD");
    fireEvent.click(screen.getByRole("button", { name: "More" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Disconnect" }));
    fireEvent.click(await screen.findByRole("button", { name: "Connect CoralSwarm" }));
    expect(await screen.findByText("connected-content:Acme ISD")).toBeVisible();
    expect(screen.queryByText("Pairing code")).not.toBeInTheDocument();
    expect(await screen.findByText("Connected as ada@acme.test")).toBeVisible(); // toast
  });
});

describe("connected on open (no start card, no Clerk)", () => {
  it("resumes silently with the account row and the SSO token in memory only", async () => {
    const h = harness({ [SESSION]: () => ({ status: 200, body: connected() }) });
    mount(h);
    expect(await screen.findByText("connected-content:Acme ISD")).toBeVisible();
    expect(screen.getByText("ada@acme.test")).toBeVisible();
    expect(screen.getByText("Connected")).toBeVisible();
    expect(screen.queryByText("CLERK FALLBACK")).not.toBeInTheDocument();
    expect(localStorage.length).toBe(0);
    expect(document.cookie).toBe("");
  });
});

describe("pairing", () => {
  async function toPairing(h: ReturnType<typeof harness>) {
    mount(h);
    fireEvent.click(await screen.findByRole("button", { name: "Connect CoralSwarm" }));
    await screen.findByText("Pairing code");
  }

  it("shows the code, the three steps and the footer; opens the approval page in the host browser", async () => {
    const h = harness({ [SESSION]: () => ({ status: 200, body: linkRequired }) });
    await toPairing(h);
    expect(screen.getByTestId("pairing-code")).toHaveTextContent("ABCD1234");
    expect(screen.getByText("Approve in your browser")).toBeVisible();
    for (const step of ["Open the approval page", "Tap Approve", "Come back and continue"]) expect(screen.getByText(step)).toBeVisible();
    expect(screen.getByRole("button", { name: "I've approved — continue" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Start over" })).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Open approval page" }));
    await waitFor(() => expect(h.openLink).toHaveBeenCalledWith(approveUrl));
  });

  it("refuses to open an approval link that is not the https approve page", async () => {
    const bad = { ...linkRequired, pairing: { ...linkRequired.pairing, approve_url: "https://evil.test/steal?code=ABCD1234" } };
    const h = harness({ [SESSION]: () => ({ status: 200, body: bad }) });
    await toPairing(h);
    fireEvent.click(screen.getByRole("button", { name: "Open approval page" }));
    await screen.findByText("Could not start pairing. Try again in a moment.");
    expect(h.openLink).not.toHaveBeenCalled();
  });

  it("says so when tapped before approval", async () => {
    const h = harness({ [SESSION]: () => ({ status: 200, body: linkRequired }), [POLL]: () => ({ status: 200, body: { status: "pending" } }) });
    await toPairing(h);
    fireEvent.click(screen.getByRole("button", { name: "I've approved — continue" }));
    expect(await screen.findByText("Not approved yet — approve in your browser, then tap this again.")).toBeVisible();
    expect(screen.getByText("Pairing code")).toBeVisible();
  });

  it("connects after the continue tap once approved (poll → session again)", async () => {
    const h = harness({
      [SESSION]: [() => ({ status: 200, body: linkRequired }), () => ({ status: 200, body: connected("cs-9") })],
      [POLL]: () => ({ status: 200, body: { status: "approved" } }),
    });
    await toPairing(h);
    fireEvent.click(screen.getByRole("button", { name: "I've approved — continue" }));
    expect(await screen.findByText("connected-content:Acme ISD")).toBeVisible();
    expect(await screen.findByText("Connected as ada@acme.test")).toBeVisible();
    expect(h.log.filter((l) => l === SESSION)).toHaveLength(2); // boot (minted the pairing), after approval
  });

  it("auto-polls and connects without any tap", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const h = harness({
      [SESSION]: [() => ({ status: 200, body: linkRequired }), () => ({ status: 200, body: connected() })],
      [POLL]: [() => ({ status: 200, body: { status: "pending" } }), () => ({ status: 200, body: { status: "approved" } })],
    });
    await toPairing(h);
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS); });
    expect(screen.getByText("Pairing code")).toBeVisible();
    await act(async () => { await vi.advanceTimersByTimeAsync(POLL_INTERVAL_MS); });
    expect(await screen.findByText("connected-content:Acme ISD")).toBeVisible();
  });

  it("returns to the start card with the expiry toast when the pairing expired", async () => {
    const h = harness({ [SESSION]: () => ({ status: 200, body: linkRequired }), [POLL]: () => ({ status: 200, body: { status: "expired" } }) });
    await toPairing(h);
    fireEvent.click(screen.getByRole("button", { name: "I've approved — continue" }));
    expect(await screen.findByText("Pairing expired — start over.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Connect CoralSwarm" })).toBeVisible();
  });

  it("Start over discards the pairing and shows the start card", async () => {
    const h = harness({ [SESSION]: () => ({ status: 200, body: linkRequired }) });
    await toPairing(h);
    fireEvent.click(screen.getByRole("button", { name: "Start over" }));
    expect(await screen.findByRole("button", { name: "Connect CoralSwarm" })).toBeVisible();
    expect(screen.queryByText("Pairing code")).not.toBeInTheDocument();
  });
});

describe("Clerk fallback", () => {
  it("renders the existing flow when the backend flag is off (404 microsoft_sso_disabled)", async () => {
    const h = harness({ [SESSION]: () => ({ status: 404, body: { error: "microsoft_sso_disabled" } }) });
    mount(h);
    expect(await screen.findByText("CLERK FALLBACK")).toBeVisible();
    expect(screen.queryByRole("button", { name: "Connect CoralSwarm" })).not.toBeInTheDocument();
  });

  it("renders the existing flow when the host cannot issue a Microsoft token", async () => {
    const h = harness({ [SESSION]: () => ({ status: 200, body: connected() }) }, async () => { throw new SsoUnavailableError(); });
    mount(h);
    expect(await screen.findByText("CLERK FALLBACK")).toBeVisible();
    expect(h.fetchImpl).not.toHaveBeenCalled();
  });

  it("does NOT fall back on a rejected Microsoft token; it offers a retry", async () => {
    const h = harness({ [SESSION]: [() => ({ status: 401, body: { error: "invalid_microsoft_token" } }), () => ({ status: 200, body: connected() })] });
    mount(h);
    expect(await screen.findByRole("alert")).toHaveTextContent("couldn’t verify your Microsoft sign-in");
    expect(screen.queryByText("CLERK FALLBACK")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(await screen.findByText("connected-content:Acme ISD")).toBeVisible();
  });
});

describe("account menu", () => {
  async function connectedPane(extra: Record<string, Route> = {}) {
    const h = harness({
      [SESSION]: () => ({ status: 200, body: connected() }),
      "GET /v1/addin/microsoft/link": () => ({ status: 200, body: { account: { email: "ada@acme.test" }, workspace: org, workspaces: [personal, org] } }),
      ...extra,
    });
    mount(h);
    await screen.findByText("connected-content:Acme ISD");
    return h;
  }

  it("⋮ shows Connected as …, Change workspace and Disconnect", async () => {
    await connectedPane();
    fireEvent.click(screen.getByRole("button", { name: "More" }));
    expect(screen.getByText("Connected as ada@acme.test")).toBeVisible();
    expect(screen.getByText("Workspace: Acme ISD")).toBeVisible();
    expect(screen.getByRole("menuitem", { name: "Change workspace" })).toBeVisible();
    expect(screen.getByRole("menuitem", { name: "Disconnect" })).toBeVisible();
    expect(screen.queryByRole("combobox")).not.toBeInTheDocument(); // no dropdown on the main screen
  });

  it("Change workspace lists the link's workspaces and switches with the new token", async () => {
    const h = await connectedPane({ "PUT /v1/addin/microsoft/link/workspace": () => ({ status: 200, body: connected("cs-2", personal) }) });
    fireEvent.click(screen.getByRole("button", { name: "More" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Change workspace" }));
    fireEvent.click(await screen.findByRole("button", { name: /Personal/ }));
    expect(await screen.findByText("connected-content:Personal")).toBeVisible();
    expect(await screen.findByText("Workspace changed to Personal")).toBeVisible();
    expect(h.log).toContain("PUT /v1/addin/microsoft/link/workspace");
  });

  it("Disconnect revokes and says so honestly, then shows the start card", async () => {
    await connectedPane({ "DELETE /v1/addin/microsoft/link": () => ({ status: 204 }) });
    fireEvent.click(screen.getByRole("button", { name: "More" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Disconnect" }));
    expect(await screen.findByText("Disconnected — the CoralSwarm credential was revoked.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Connect CoralSwarm" })).toBeVisible();
  });

  it("Disconnect does not claim a revoke that failed", async () => {
    await connectedPane({ "DELETE /v1/addin/microsoft/link": () => ({ status: 500 }) });
    fireEvent.click(screen.getByRole("button", { name: "More" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Disconnect" }));
    expect(await screen.findByText(/revoking the server credential failed/)).toBeVisible();
    expect(screen.queryByText(/credential was revoked/)).not.toBeInTheDocument();
  });

  it("after Disconnect nothing re-trades the Microsoft token until Connect is tapped", async () => {
    const h = await connectedPane({ "DELETE /v1/addin/microsoft/link": () => ({ status: 204 }) });
    fireEvent.click(screen.getByRole("button", { name: "More" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Disconnect" }));
    await screen.findByRole("button", { name: "Connect CoralSwarm" });
    const sessions = h.log.filter((l) => l === SESSION).length;
    await new Promise((r) => setTimeout(r, 50));
    expect(h.log.filter((l) => l === SESSION).length).toBe(sessions);
  });

  it("a child reporting a lost link returns to the start card with the reconnect toast", async () => {
    await connectedPane();
    fireEvent.click(screen.getByRole("button", { name: "force-reconnect" }));
    expect(await screen.findByText("CoralSwarm needs to be reconnected.")).toBeVisible();
    expect(screen.getByRole("button", { name: "Connect CoralSwarm" })).toBeVisible();
  });
});

describe("home", () => {
  it("shows the muted + row and the empty illustration copy", async () => {
    const h = harness({ [SESSION]: () => ({ status: 200, body: connected() }) });
    mount(h);
    expect(await screen.findByText("Open a meeting to save it")).toBeVisible();
    expect(screen.getByText("Nothing from Teams yet")).toBeVisible();
  });
});

describe("isSafeApprovalUrl", () => {
  it("accepts only the https approve page without credentials", () => {
    expect(isSafeApprovalUrl("https://app.coralswarm.example/addin/approve?code=X&kind=microsoft", "https://app.coralswarm.example")).toBe(true);
    expect(isSafeApprovalUrl("http://app.coralswarm.example/addin/approve?code=X", "https://app.coralswarm.example")).toBe(false);
    expect(isSafeApprovalUrl("https://u:p@app.coralswarm.example/addin/approve", "https://app.coralswarm.example")).toBe(false);
    expect(isSafeApprovalUrl("https://app.coralswarm.example/login", "https://app.coralswarm.example")).toBe(false);
    // Origin is pinned: a foreign https host is refused even with the right path.
    expect(isSafeApprovalUrl("https://evil.test/addin/approve?code=X", "https://app.coralswarm.example")).toBe(false);
    expect(isSafeApprovalUrl("https://app.coralswarm.example.evil.test/addin/approve", "https://app.coralswarm.example")).toBe(false);
    // Local-dev and staging origins only when they are this page's own origin.
    expect(isSafeApprovalUrl("https://localhost:3001/addin/approve?code=X", "https://localhost:3001")).toBe(true);
    expect(isSafeApprovalUrl("https://localhost:3001/addin/approve?code=X", "https://app.coralswarm.example")).toBe(false);
    expect(isSafeApprovalUrl("https://app.staging.example.test/addin/approve", "https://app.staging.example.test")).toBe(true);
    expect(isSafeApprovalUrl("https://evil.test/addin/approve", "https://evil.test")).toBe(false);
    expect(isSafeApprovalUrl("javascript:alert(1)", "https://app.coralswarm.example")).toBe(false);
    expect(isSafeApprovalUrl("http://localhost:3000/addin/approve?code=X", "http://localhost:3000")).toBe(true);
  });
});
