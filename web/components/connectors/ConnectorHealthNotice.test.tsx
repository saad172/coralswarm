// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api, type ConnectorHealthItem, type ConnectorHealthResponse, type DirectoryAttribution } from "@/lib/api";
import { ConnectorHealthNotice } from "./ConnectorHealthNotice";

const identity = vi.hoisted(() => ({ app: { token: "TOKEN" as string | null } }));
vi.mock("@/lib/providers", () => ({
  useApp: () => ({ ...identity.app, authReady: identity.app.token !== null, getAuthToken: () => identity.app.token, queryScope: "user::org_acme" }),
}));

const orgAdmin = vi.hoisted(() => ({
  state: {
    loaded: true,
    hasOrg: true,
    isAdmin: true,
    orgId: "org_acme" as string | null,
    orgName: "Acme" as string | null,
    orgSlug: "acme" as string | null,
  },
}));
vi.mock("@/components/org/useOrgAdmin", () => ({ useOrgAdmin: () => orgAdmin.state }));

vi.mock("@/lib/api", async () => ({
  ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")),
  api: { orgs: { connectorHealth: vi.fn() } },
}));

const connectorHealth = vi.mocked(api.orgs.connectorHealth);

function degradation(overrides: Partial<ConnectorHealthItem> = {}): ConnectorHealthItem {
  return {
    provider: "google_chat",
    capability: "domain_directory",
    condition: "backing_member_departed",
    backing_email: "person@example.com",
    first_observed_at: "2026-09-22T10:00:00Z",
    last_observed_at: "2026-09-22T10:15:00Z",
    detail: { rung: 1, ladder_effect: "auto_add_now_decides_on_rung_2" },
    ...overrides,
  };
}

function attribution(overrides: Partial<DirectoryAttribution> = {}): DirectoryAttribution {
  return {
    provider: "google_chat",
    capability: "domain_directory",
    members: [{ email: "person@example.com", disconnected_at: "2026-09-18T09:00:00Z" }],
    ...overrides,
  };
}

/** The response envelope, so every existing test keeps saying what it said
 *  before CS-448 changed the shape: "these degradations, and nothing
 *  attributed". */
function response(overrides: Partial<ConnectorHealthResponse> = {}): ConnectorHealthResponse {
  return { degradations: [], directory_attribution: null, ...overrides };
}

beforeEach(() => {
  identity.app = { token: "TOKEN" };
  orgAdmin.state = {
    loaded: true,
    hasOrg: true,
    isAdmin: true,
    orgId: "org_acme",
    orgName: "Acme",
    orgSlug: "acme",
  };
  connectorHealth.mockReset();
});
afterEach(() => {
  cleanup();
});

describe("ConnectorHealthNotice — the surface D16 asks for (CS-446, item 16)", () => {
  /** THE test for this item. D16's whole complaint is that the drop from
   *  rung 1 to rung 2 is invisible; a record nobody is shown is the same
   *  failure with extra steps. This asserts the degradation is RENDERED —
   *  what stopped working, whose credential it was, what it means for
   *  joining, and what fixes it. */
  it("renders the degradation, not merely the fact that one exists", async () => {
    connectorHealth.mockResolvedValue(response({ degradations: [degradation()] }));
    render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);

    await waitFor(() => {
      expect(screen.getByText(/Google Workspace directory/i)).toBeInTheDocument();
    });
    // Whose credential it was — the part that makes it actionable.
    expect(screen.getByText("person@example.com")).toBeInTheDocument();
    // What it now means for the decision D16 is about.
    expect(screen.getByText(/weaker check/i)).toBeInTheDocument();
    // And the remedy, attached to the person reading it.
    expect(screen.getByRole("button", { name: /Connect Google Chat/i })).toBeInTheDocument();
  });

  it("offers the remedy as the same act the connector tile's own button performs", async () => {
    const onConnect = vi.fn();
    connectorHealth.mockResolvedValue(response({ degradations: [degradation()] }));
    render(<ConnectorHealthNotice onConnectGoogleChat={onConnect} />);

    const button = await screen.findByRole("button", { name: /Connect Google Chat/i });
    fireEvent.click(button);
    expect(onConnect).toHaveBeenCalledTimes(1);
  });

  it("renders nothing when nothing is degraded", async () => {
    connectorHealth.mockResolvedValue(response());
    const { container } = render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);
    await waitFor(() => expect(connectorHealth).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  /** Admin-only on the server; admin-only here, so a member never sees a
   *  departed colleague's address and never fires a request that would 403.
   *  Same for the personal plane, which has no org connector health at all. */
  it("never fetches, and renders nothing, for a non-admin or on the personal plane", async () => {
    orgAdmin.state = { ...orgAdmin.state, isAdmin: false };
    const { container: memberView } = render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);
    expect(memberView).toBeEmptyDOMElement();

    cleanup();
    orgAdmin.state = { ...orgAdmin.state, isAdmin: true, hasOrg: false };
    const { container: personalView } = render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);
    expect(personalView).toBeEmptyDOMElement();

    expect(connectorHealth).not.toHaveBeenCalled();
  });

  /** A failed read shows nothing, never an error banner: the request is
   *  admin-only and the condition is durable, so a swallowed transient
   *  costs a page load, not the signal. */
  it("stays silent when the read fails", async () => {
    connectorHealth.mockRejectedValue(new Error("boom"));
    const { container } = render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);
    await waitFor(() => expect(connectorHealth).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  /** An unrecognised condition still renders — naming its keys — but gets
   *  no "Connect Google Chat" button, because offering a specific remedy
   *  for something we cannot describe would be a guess. */
  it("shows an unknown condition without inventing a remedy for it", async () => {
    connectorHealth.mockResolvedValue(
      response({ degradations: [degradation({ provider: "slack", capability: "roster_walk" })] }),
    );
    render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);

    await waitFor(() => {
      expect(screen.getByText(/roster_walk/)).toBeInTheDocument();
    });
    expect(screen.queryByRole("button", { name: /Connect Google Chat/i })).not.toBeInTheDocument();
  });
});

describe("ConnectorHealthNotice — attribution for a member who is still here (CS-448)", () => {
  /** THE test for this item. Item 16 shows the degradation; item 49 shows
   *  "hasn't connected a tool". Neither said they were the same fact. This
   *  asserts the notice now names the member, dates the disconnect, and
   *  attaches the remedy — with NO durable degradation in the response at
   *  all, which is the case that previously rendered nothing whatsoever. */
  it("names the member who disconnected, even with no recorded degradation", async () => {
    connectorHealth.mockResolvedValue(response({ directory_attribution: attribution() }));
    render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);

    await waitFor(() => {
      expect(screen.getByText(/Google Workspace directory/i)).toBeInTheDocument();
    });
    // One row per named member, carrying the address AND the date — an
    // admin needs the date to recognise the change they are being told
    // about. Asserted on the row rather than on loose text because the
    // word "disconnected" also appears in the lead sentence.
    const named = screen.getAllByRole("listitem");
    expect(named).toHaveLength(1);
    expect(named[0].textContent).toContain("person@example.com");
    expect(named[0].textContent).toMatch(/disconnected/i);
    expect(named[0].textContent).toContain(
      new Date("2026-09-18T09:00:00Z").toLocaleDateString(),
    );
    expect(screen.getByText(/weaker check/i)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Connect Google Chat/i })).toBeInTheDocument();
  });

  /** FAILS WITHOUT THE FIX, and is the negative half of the item: a member
   *  who disconnected but was not what the directory rested on produces no
   *  attribution at all. The backend decides that — it answers `Backed`
   *  and sends `null` — and this pins that the surface adds nothing of its
   *  own on top, rendering completely empty. */
  it("renders nothing when the server attributed nothing", async () => {
    connectorHealth.mockResolvedValue(response({ directory_attribution: null }));
    const { container } = render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);
    await waitFor(() => expect(connectorHealth).toHaveBeenCalled());
    expect(container).toBeEmptyDOMElement();
  });

  /** Both at once, and deliberately: a departed backer and a still-present
   *  member who disconnected are two people with two different remedies —
   *  only one of them can be asked to press a button. Collapsing them would
   *  lose exactly the distinction migration 0243 exists to preserve. */
  it("shows a recorded degradation and a live attribution side by side", async () => {
    connectorHealth.mockResolvedValue(
      response({
        degradations: [degradation({ backing_email: "person@example.com" })],
        directory_attribution: attribution(),
      }),
    );
    render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);

    await waitFor(() => {
      expect(screen.getByText("person@example.com")).toBeInTheDocument();
    });
    // The departed colleague, and the one who is still here to fix it.
    expect(screen.getByText("person@example.com")).toBeInTheDocument();
    expect(screen.getByText(/no longer in this organization/i)).toBeInTheDocument();
    expect(screen.getByText(/still here/i)).toBeInTheDocument();
  });

  it("points the attribution's remedy at the same connect handler as the tile", async () => {
    const onConnect = vi.fn();
    connectorHealth.mockResolvedValue(response({ directory_attribution: attribution() }));
    render(<ConnectorHealthNotice onConnectGoogleChat={onConnect} />);

    fireEvent.click(await screen.findByRole("button", { name: /Connect Google Chat/i }));
    expect(onConnect).toHaveBeenCalledTimes(1);
  });

  it("stays admin-only — a member never learns who disconnected", async () => {
    orgAdmin.state = { ...orgAdmin.state, isAdmin: false };
    const { container } = render(<ConnectorHealthNotice onConnectGoogleChat={() => {}} />);
    expect(container).toBeEmptyDOMElement();
    expect(connectorHealth).not.toHaveBeenCalled();
  });
});
