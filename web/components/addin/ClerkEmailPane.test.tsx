// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ClerkEmailPane } from "./ClerkEmailPane";

const s = vi.hoisted(() => ({
  signedIn: false, orgId: null as string | null, memberships: [] as { organization: { id: string; name: string } }[],
  setActive: vi.fn(), signOut: vi.fn(), getToken: vi.fn(),
}));
vi.mock("@clerk/nextjs", () => ({
  SignIn: () => <div role="region" aria-label="Inline sign-in" />,
  useAuth: () => ({ isLoaded: true, isSignedIn: s.signedIn, getToken: s.getToken, orgId: s.orgId, sessionId: "sess", signOut: s.signOut }),
  useUser: () => ({ user: { primaryEmailAddress: { emailAddress: "ada@acme.test" } } }),
  useOrganization: () => ({ organization: s.orgId ? { name: s.memberships.find((m) => m.organization.id === s.orgId)?.organization.name ?? "Org" } : null }),
  useOrganizationList: () => ({ isLoaded: true, setActive: s.setActive, userMemberships: { data: s.memberships, hasNextPage: false, isFetching: false } }),
}));
vi.mock("./MailContent", () => ({ MailContent: ({ workspaceName }: { workspaceName: string }) => <div role="region" aria-label="Mail content">{workspaceName}</div> }));

const item = { kind: "message", key: "msg:1", itemId: "1", conversationId: "c", subject: "S", from: null } as const;
const acme = { organization: { id: "org-a", name: "Acme ISD" } };
const beta = { organization: { id: "org-b", name: "Beta ISD" } };

beforeEach(() => { vi.clearAllMocks(); s.signedIn = false; s.orgId = null; s.memberships = []; s.setActive.mockResolvedValue(undefined); });
afterEach(cleanup);

it("signed out: Clerk sign-in inside the new surface", () => {
  render(<ClerkEmailPane office={null} item={item} openToday={vi.fn()} />);
  expect(screen.getByRole("region", { name: "Inline sign-in" })).toBeVisible();
  expect(screen.queryByRole("region", { name: "Mail content" })).not.toBeInTheDocument();
});

it("a single organization with none active is chosen for the user", async () => {
  s.signedIn = true; s.memberships = [acme];
  render(<ClerkEmailPane office={null} item={item} openToday={vi.fn()} />);
  await waitFor(() => expect(s.setActive).toHaveBeenCalledWith({ organization: "org-a" }));
});

it("several organizations and none active: the user chooses, never a guess", async () => {
  s.signedIn = true; s.memberships = [acme, beta];
  render(<ClerkEmailPane office={null} item={item} openToday={vi.fn()} />);
  expect(screen.getByText("Change workspace")).toBeVisible();
  expect(s.setActive).not.toHaveBeenCalled();
  expect(screen.queryByRole("region", { name: "Mail content" })).not.toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: /Beta ISD/ }));
  await waitFor(() => expect(s.setActive).toHaveBeenCalledWith({ organization: "org-b" }));
});

it("a rejected setActive keeps the chooser up: never falls through to a workspace the user did not pick", async () => {
  s.signedIn = true; s.memberships = [acme, beta];
  s.setActive.mockRejectedValue(new Error("nope"));
  render(<ClerkEmailPane office={null} item={item} openToday={vi.fn()} />);
  fireEvent.click(screen.getByRole("button", { name: /Beta ISD/ }));
  await waitFor(() => expect(s.setActive).toHaveBeenCalled());
  expect(await screen.findByText("Could not change workspace. Try again in a moment.")).toBeVisible();
  expect(screen.queryByRole("region", { name: "Mail content" })).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: /Beta ISD/ })).toBeVisible();
});

it("signed in with a workspace: mail content for that workspace, account row, change workspace and sign out", () => {
  s.signedIn = true; s.orgId = "org-a"; s.memberships = [acme, beta];
  render(<ClerkEmailPane office={null} item={item} openToday={vi.fn()} />);
  expect(screen.getByRole("region", { name: "Mail content" })).toHaveTextContent("Acme ISD");
  expect(screen.getByText("ada@acme.test")).toBeVisible();
  fireEvent.click(screen.getByRole("button", { name: "Sign out" }));
  expect(s.signOut).toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Change workspace" }));
  expect(screen.getByRole("button", { name: /Personal/ })).toBeVisible();
});
