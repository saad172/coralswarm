// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { useState, type ReactNode } from "react";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import AppLayout from "@/app/(app)/layout";
const state = vi.hoisted(() => ({ live: { isLoaded: true, isSignedIn: true, userId: "one", orgId: "org" }, app: { ready: true, token: "", getterToken: undefined as string | undefined, authReady: true, userId: "one", accountId: "account", resolvedOrgId: "org", currentGraphId: "graph", graphs: [] }, replace: vi.fn() }));
vi.mock("@clerk/nextjs", () => ({ useAuth: () => state.live }));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ ...state.app, getAuthToken: () => state.app.getterToken ?? state.app.token, retryBoot: vi.fn() }) }));
vi.mock("next/navigation", () => ({ useRouter: () => ({ replace: state.replace }) }));
vi.mock("@/components/AppShell", () => ({ default: ({ children }: { children: ReactNode }) => <div><p>Private shell {state.app.userId}</p>{children}</div> }));
vi.mock("@/lib/engine/provider", () => ({ ENGINE_ENABLED: false, EngineProvider: ({ children }: { children: ReactNode }) => children }));
vi.mock("@/components/EngineGate", () => ({ default: ({ children }: { children: ReactNode }) => children }));
vi.mock("@/lib/promoteJobsProvider", () => ({ PromoteJobsProvider: ({ children }: { children: ReactNode }) => children }));
vi.mock("@/lib/postSignInRedirect", () => ({ setPostSignInRedirect: vi.fn() }));
function Page() { const [title] = useState(`Private page ${state.app.userId}:${state.app.currentGraphId}`); return <p>{title}</p>; }
const token = (sub = "one", org_id = "org", issuer = "issuer", iat = 1) => `h.${btoa(JSON.stringify({ sub, org_id, iss: issuer, iat }))}.s`;
beforeEach(() => { Object.assign(state.live, { isLoaded: true, isSignedIn: true, userId: "one", orgId: "org" }); Object.assign(state.app, { ready: true, authReady: true, token: token(), getterToken: undefined, userId: "one", accountId: "account", resolvedOrgId: "org", currentGraphId: "graph" }); });
afterEach(cleanup);
it.each(["user", "org", "signout"])("hides private shell and disables the bearer getter before resolved %s state catches up", (kind) => {
  const view = render(<AppLayout><Page /></AppLayout>);
  expect(screen.getByText("Private shell one")).toBeInTheDocument();
  if (kind === "user") state.live.userId = "two";
  if (kind === "org") state.live.orgId = "other";
  if (kind === "signout") state.live.isSignedIn = false;
  view.rerender(<AppLayout><Page /></AppLayout>);
  expect(screen.queryByText("Private shell one")).not.toBeInTheDocument();
  expect(screen.queryByText("Private page one:graph")).not.toBeInTheDocument();
});
it("remounts private page state on workspace change without reminting credentials", () => {
  const view = render(<AppLayout><Page /></AppLayout>);
  state.app.currentGraphId = "next";
  view.rerender(<AppLayout><Page /></AppLayout>);
  expect(screen.queryByText("Private page one:graph")).not.toBeInTheDocument();
  expect(screen.getByText("Private page one:next")).toBeInTheDocument();
});

it('waits for a signed-in account switch without redirecting the live session to login', () => {
  state.replace.mockClear();
  const view = render(<AppLayout><Page /></AppLayout>);
  state.live.orgId = 'other'; state.app.authReady = false;
  view.rerender(<AppLayout><Page /></AppLayout>);
  expect(screen.queryByText('Private shell one')).not.toBeInTheDocument();
  expect(screen.getByRole('status')).toHaveTextContent('Verifying the current account');
  expect(state.replace).not.toHaveBeenCalled();
  Object.assign(state.app, { authReady: true, resolvedOrgId: 'other', accountId: 'other-account', token: token('one', 'other') });
  view.rerender(<AppLayout><Page /></AppLayout>);
  expect(screen.getByText('Private shell one')).toBeInTheDocument();
  expect(state.replace).not.toHaveBeenCalled();
});
