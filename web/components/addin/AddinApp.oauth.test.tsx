// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AddinApp } from "./AddinApp";
import { TeamsSimpleApp } from "./TeamsSimpleApp";
import { SsoUnavailableError, type AddinHost } from "@/lib/addin/microsoftSso";
import { createPaneClient } from "@/lib/addin/oauthClient";
import { DialogCancelledError } from "@/lib/addin/authDialog";
import { base64url, type ClerkOAuthConfig } from "@/lib/addin/clerkOAuth";

const CONFIG: ClerkOAuthConfig = { clientId: "example_18", fapi: "https://clerk.coralswarm.example" };
const org = { account_id: "acct-o", name: "Acme ISD", kind: "org" };
const personal = { account_id: "acct-p", name: "Personal", kind: "personal" };
const connected = (token: string, workspace = org) => ({ status: "connected", token, expires_at: "2099-01-01T00:00:00Z", account: { email: "ada@acme.test" }, workspace });
const linkRequired = { status: "link_required", pairing: { code: "ABCD1234", poll_secret: "sec", approve_url: "https://app.coralswarm.example/addin/approve?code=ABCD1234&kind=microsoft", expires_at: "2099-01-01T00:00:00Z" } };
const jwt = (claims: Record<string, unknown>) => { const e = (o: unknown) => base64url(new TextEncoder().encode(JSON.stringify(o))); return `${e({ alg: "x" })}.${e(claims)}.s`; };

function build(options: { gate: boolean; routes: Record<string, () => { status: number; body?: unknown }>; ms?: () => Promise<string>; runDialog?: ReturnType<typeof vi.fn> }) {
  const runDialog = options.runDialog ?? vi.fn(async () => ({ code: "c", verifier: "v", redirectUri: "r" }));
  const log: string[] = [];
  const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    const key = `${init?.method ?? "GET"} ${url.replace("https://api.test", "").replace(CONFIG.fapi, "FAPI")}`;
    log.push(key);
    if (key === "POST FAPI/oauth/token") return new Response(JSON.stringify({ access_token: jwt({ org_id: "org_1" }), refresh_token: "r", expires_in: 86400 }), { status: 200 });
    const route = options.routes[key];
    if (!route) return new Response(JSON.stringify({ error: "unrouted" }), { status: 500 });
    const out = route();
    return new Response(out.status === 204 ? null : JSON.stringify(out.body ?? {}), { status: out.status });
  }) as unknown as typeof fetch;
  const host: AddinHost = { name: "outlook", getMicrosoftToken: options.ms ?? (async () => "ms"), openLink: vi.fn(async () => {}) };
  const client = createPaneClient({ host, apiBase: () => "https://api.test", dialog: { kind: "teams" }, config: options.gate ? CONFIG : null, fetchImpl, runDialog: runDialog as never });
  return { client, host, runDialog, log };
}

function mountApp(h: ReturnType<typeof build>) {
  return render(
    <AddinApp client={h.client} host={h.host} surface="email" label="CoralSwarm email" fallback={<p>CLERK FALLBACK</p>}>
      {(ctx) => <p>connected-content:{ctx.session.workspace.name}</p>}
    </AddinApp>,
  );
}

const SESSION = "POST /v1/addin/microsoft/session";
const LINK = "POST /v1/addin/microsoft/link";

afterEach(() => { cleanup(); vi.clearAllMocks(); });

describe("gate off: today's behaviour", () => {
  it("shows the pairing start card and never mentions the OAuth sign-in", async () => {
    const h = build({ gate: false, routes: { [SESSION]: () => ({ status: 200, body: linkRequired }) } });
    mountApp(h);
    expect(await screen.findByRole("button", { name: "Connect CoralSwarm" })).toBeVisible();
    expect(screen.queryByText("Sign in to CoralSwarm")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Connect CoralSwarm" }));
    expect(await screen.findByTestId("pairing-code")).toHaveTextContent("ABCD1234");
    expect(h.runDialog).not.toHaveBeenCalled();
  });

  it("still falls back to the Clerk flow when SSO is unavailable", async () => {
    const h = build({ gate: false, routes: {}, ms: async () => { throw new SsoUnavailableError(); } });
    mountApp(h);
    expect(await screen.findByText("CLERK FALLBACK")).toBeVisible();
  });

  it("is the default when the env var is not set (no config override)", async () => {
    const host: AddinHost = { name: "outlook", getMicrosoftToken: async () => "ms", openLink: async () => {} };
    const fetchImpl = vi.fn(async () => new Response(JSON.stringify(linkRequired), { status: 200 })) as unknown as typeof fetch;
    const client = createPaneClient({ host, apiBase: () => "https://api.test", dialog: { kind: "teams" }, fetchImpl });
    render(<AddinApp client={client} host={host} surface="email" label="x" fallback={null}>{() => null}</AddinApp>);
    expect(await screen.findByRole("button", { name: "Connect CoralSwarm" })).toBeVisible();
  });
});

describe("gate on", () => {
  it("replaces the pairing card with Sign in to CoralSwarm, then connects through the link call", async () => {
    const h = build({ gate: true, routes: { [SESSION]: () => ({ status: 200, body: linkRequired }), [LINK]: () => ({ status: 200, body: connected("csms_1") }) } });
    mountApp(h);
    const button = await screen.findByRole("button", { name: "Sign in to CoralSwarm" });
    expect(screen.queryByRole("button", { name: "Connect CoralSwarm" })).not.toBeInTheDocument();
    expect(screen.queryByTestId("pairing-code")).not.toBeInTheDocument();
    fireEvent.click(button);
    expect(await screen.findByText("connected-content:Acme ISD")).toBeVisible();
    expect(h.log).toContain(LINK);
    expect(h.runDialog).toHaveBeenCalledTimes(1);
  });

  it("a returning user is connected silently, with no sign-in screen", async () => {
    const h = build({ gate: true, routes: { [SESSION]: () => ({ status: 200, body: connected("csms_1") }) } });
    mountApp(h);
    expect(await screen.findByText("connected-content:Acme ISD")).toBeVisible();
    expect(h.runDialog).not.toHaveBeenCalled();
  });

  it("offers the sign-in (not the Clerk fallback) when Microsoft SSO is unavailable, and connects Clerk-direct", async () => {
    const h = build({ gate: true, routes: {}, ms: async () => { throw new SsoUnavailableError(); } });
    mountApp(h);
    expect(screen.queryByText("CLERK FALLBACK")).not.toBeInTheDocument();
    fireEvent.click(await screen.findByRole("button", { name: "Sign in to CoralSwarm" }));
    expect(await screen.findByText("connected-content:Your organization")).toBeVisible();
    expect(h.log).not.toContain(LINK);
  });

  it("says Sign-in was cancelled with a Try again button, and retries", async () => {
    const runDialog = vi.fn()
      .mockRejectedValueOnce(new DialogCancelledError())
      .mockResolvedValue({ code: "c", verifier: "v", redirectUri: "r" });
    const h = build({ gate: true, runDialog, routes: { [SESSION]: () => ({ status: 200, body: linkRequired }), [LINK]: () => ({ status: 200, body: connected("csms_1") }) } });
    mountApp(h);
    fireEvent.click(await screen.findByRole("button", { name: "Sign in to CoralSwarm" }));
    expect(await screen.findByText("Sign-in was cancelled.")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Try again" }));
    expect(await screen.findByText("connected-content:Acme ISD")).toBeVisible();
    expect(runDialog).toHaveBeenCalledTimes(2);
  });

  it("Change workspace re-runs the dialog with prompt=consent and shows the new workspace", async () => {
    const routes = { [SESSION]: () => ({ status: 200, body: connected("csms_1") }), [LINK]: () => ({ status: 200, body: connected("csms_2", personal) }) };
    const h = build({ gate: true, routes });
    mountApp(h);
    await screen.findByText("connected-content:Acme ISD");
    fireEvent.click(screen.getByRole("button", { name: "More" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Change workspace" }));
    expect(await screen.findByText("connected-content:Personal")).toBeVisible();
    expect(h.runDialog.mock.calls[0][0]).toMatchObject({ prompt: "consent" });
    // The old link-based picker is not used.
    expect(h.log).not.toContain("GET /v1/addin/microsoft/link");
  });

  it("a cancelled Change workspace keeps the current workspace", async () => {
    const runDialog = vi.fn().mockRejectedValue(new DialogCancelledError());
    const h = build({ gate: true, runDialog, routes: { [SESSION]: () => ({ status: 200, body: connected("csms_1") }) } });
    mountApp(h);
    await screen.findByText("connected-content:Acme ISD");
    fireEvent.click(screen.getByRole("button", { name: "More" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Change workspace" }));
    expect(await screen.findByText("Sign-in was cancelled.")).toBeVisible();
    expect(screen.getByText("connected-content:Acme ISD")).toBeVisible();
  });

  it("Disconnect returns to the sign-in", async () => {
    const h = build({ gate: true, routes: { [SESSION]: () => ({ status: 200, body: connected("csms_1") }), "DELETE /v1/addin/microsoft/link": () => ({ status: 204 }) } });
    mountApp(h);
    await screen.findByText("connected-content:Acme ISD");
    fireEvent.click(screen.getByRole("button", { name: "More" }));
    fireEvent.click(screen.getByRole("menuitem", { name: "Disconnect" }));
    expect(await screen.findByRole("button", { name: "Sign in to CoralSwarm" })).toBeVisible();
  });

  it("the Teams pane gets the same sign-in", async () => {
    const h = build({ gate: true, routes: { [SESSION]: () => ({ status: 200, body: linkRequired }), [LINK]: () => ({ status: 200, body: connected("csms_1") }) } });
    render(<TeamsSimpleApp client={h.client} host={h.host} label="CoralSwarm meeting" fallback={<p>CLERK FALLBACK</p>}>{(ctx) => <p>teams-connected:{ctx.session.workspace.name}</p>}</TeamsSimpleApp>);
    fireEvent.click(await screen.findByRole("button", { name: "Sign in to CoralSwarm" }));
    expect(await screen.findByText("teams-connected:Acme ISD")).toBeVisible();
  });
});
