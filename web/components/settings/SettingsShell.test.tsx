// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SettingsShell } from "./SettingsShell";
import { SettingsRedirect } from "./SettingsRedirect";
import { OrgSettingsShell } from "@/components/org/OrgSettingsShell";

const state = vi.hoisted(() => ({
  org: { loaded: true, hasOrg: false, isAdmin: false, orgId: null as string | null, orgName: null as string | null, orgSlug: null as string | null },
  pathname: "/settings",
  replace: vi.fn(),
}));

vi.mock("@/components/org/useOrgAdmin", () => ({ useOrgAdmin: () => state.org }));
vi.mock("@/components/org/useJoinRequestCount", () => ({ useJoinRequestCount: () => null }));
vi.mock("next/navigation", () => ({
  usePathname: () => state.pathname,
  useRouter: () => ({ replace: state.replace, push: vi.fn() }),
}));

function as(kind: "personal" | "member" | "admin") {
  state.org =
    kind === "personal"
      ? { loaded: true, hasOrg: false, isAdmin: false, orgId: null, orgName: null, orgSlug: null }
      : { loaded: true, hasOrg: true, isAdmin: kind === "admin", orgId: "org_1", orgName: "Acme", orgSlug: "acme" };
}

const nav = () => screen.getByRole("navigation", { name: "Settings" });
const hasLink = (name: string) => !!Array.from(nav().querySelectorAll("a")).find((a) => a.textContent?.startsWith(name));

beforeEach(() => {
  state.pathname = "/settings";
  state.replace.mockReset();
});
afterEach(cleanup);

describe("SettingsShell rail by role", () => {
  it("personal user: Account, Connections, Privacy, Usage; no Organization", () => {
    as("personal");
    render(<SettingsShell>body</SettingsShell>);
    for (const l of ["Account", "Connections", "Privacy", "Usage", "Billing"]) expect(hasLink(l)).toBe(true);
    expect(screen.queryByText("Organization")).toBeNull();
    expect(hasLink("Members")).toBe(false);
    expect(nav().querySelector('a[href="/settings"]')).toHaveTextContent("Account");
  });

  it("org member: shared pages only, no Usage, no Organization", () => {
    as("member");
    render(<SettingsShell>body</SettingsShell>);
    for (const l of ["Account", "Connections", "Privacy"]) expect(hasLink(l)).toBe(true);
    expect(hasLink("Usage")).toBe(false);
    expect(hasLink("Billing")).toBe(false);
    expect(screen.queryByText("Organization")).toBeNull();
    for (const l of ["Members", "Permissions", "Connectors", "Billing"]) expect(hasLink(l)).toBe(false);
  });

  it("org admin: shared pages plus Organization heading with five pages", () => {
    as("admin");
    render(<SettingsShell>body</SettingsShell>);
    expect(screen.getByText("Organization")).toBeInTheDocument();
    for (const l of ["Account", "Connections", "Privacy", "Members", "Permissions", "Connectors", "Usage", "Billing"]) {
      expect(hasLink(l)).toBe(true);
    }
    expect(nav().querySelectorAll("a")).toHaveLength(8);
  });
});

describe("OrgSettingsShell gate", () => {
  it("shows an org member 'Managed by your organization admin' instead of the page", () => {
    as("member");
    state.pathname = "/settings/organization/permissions";
    render(<OrgSettingsShell title="Who may do what">secret admin content</OrgSettingsShell>);
    expect(screen.getByText("Managed by your organization admin")).toBeInTheDocument();
    expect(screen.queryByText("secret admin content")).toBeNull();
    expect(hasLink("Permissions")).toBe(false);
  });

  it("renders the page for an admin", () => {
    as("admin");
    state.pathname = "/settings/organization/permissions";
    render(<OrgSettingsShell title="Who may do what">admin content</OrgSettingsShell>);
    expect(screen.getByText("admin content")).toBeInTheDocument();
  });
});

describe("SettingsRedirect stub", () => {
  it("replaces (not pushes) to the new route, keeping query and hash", async () => {
    window.history.replaceState(null, "", "/settings/access?x=1#h");
    render(<SettingsRedirect to="/settings/connections" />);
    await waitFor(() => expect(state.replace).toHaveBeenCalledWith("/settings/connections?x=1#h"));
  });
});
