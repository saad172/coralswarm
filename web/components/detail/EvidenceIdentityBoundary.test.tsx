// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { useState, type ReactNode } from "react";
import { cleanup, render, renderHook, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { EvidenceIdentityBoundary } from "./EvidenceIdentityBoundary";
import { useEvidenceAdmission } from "@/lib/useEvidenceAdmission";
import { EvidenceAdmissionContext } from "@/lib/evidenceAdmissionContext";

const state = vi.hoisted(() => ({ live: { isLoaded: true, isSignedIn: true, userId: "one", orgId: "org" }, app: { userId: "one", accountId: "account", resolvedOrgId: "org", authReady: true, currentGraphId: "graph", graphs: [], bearer: "" }, params: new URLSearchParams("id=first") }));
vi.mock("@clerk/nextjs", () => ({ useAuth: () => state.live }));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ ...state.app, getAuthToken: () => state.app.bearer }) }));
vi.mock("next/navigation", () => ({ useSearchParams: () => state.params }));
const bearer = (sub = "one", org_id = "org", iss = "issuer", iat = 1) => `h.${btoa(JSON.stringify({ sub, org_id, iss, iat }))}.s`;
let action: () => string;
function PrivateChild() {
  const admission = useEvidenceAdmission();
  const [privateTitle] = useState(`Private ${state.params.get("id")}:${state.app.currentGraphId}:${state.app.accountId}`);
  action = admission.getToken;
  return <p>{privateTitle}</p>;
}
beforeEach(() => {
  Object.assign(state.live, { isLoaded: true, isSignedIn: true, userId: "one", orgId: "org" });
  Object.assign(state.app, { userId: "one", accountId: "account", resolvedOrgId: "org", authReady: true, currentGraphId: "graph", bearer: bearer() });
  state.params = new URLSearchParams("id=first");
});
afterEach(cleanup);

it.each(["user", "org", "signedout", "unloaded", "token"])("hides all private detail state during a live %s transition", (kind) => {
  const view = render(<EvidenceIdentityBoundary><PrivateChild /></EvidenceIdentityBoundary>);
  const priorAction = action;
  expect(screen.getByText("Private first:graph:account")).toBeInTheDocument();
  if (kind === "user") state.live.userId = "two";
  if (kind === "org") state.live.orgId = "other";
  if (kind === "signedout") state.live.isSignedIn = false;
  if (kind === "unloaded") state.live.isLoaded = false;
  if (kind === "token") state.app.bearer = bearer("two");
  view.rerender(<EvidenceIdentityBoundary><PrivateChild /></EvidenceIdentityBoundary>);
  expect(screen.queryByText("Private first:graph:account")).not.toBeInTheDocument();
  expect(priorAction).toThrow();
});
it.each(["workspace", "object", "account"])("remounts state and rejects a pending action after the %s changes", (kind) => {
  const view = render(<EvidenceIdentityBoundary><PrivateChild /></EvidenceIdentityBoundary>);
  const priorAction = action;
  if (kind === "workspace") state.app.currentGraphId = "other-graph";
  if (kind === "account") state.app.accountId = "other-account";
  if (kind === "object") state.params = new URLSearchParams("id=second");
  view.rerender(<EvidenceIdentityBoundary><PrivateChild /></EvidenceIdentityBoundary>);
  expect(screen.queryByText("Private first:graph:account")).not.toBeInTheDocument();
  expect(screen.getByText(`Private ${kind === "object" ? "second" : "first"}:${state.app.currentGraphId}:${state.app.accountId}`)).toBeInTheDocument();
  expect(priorAction).toThrow();
});
it("keeps ordinary token refresh stable but checks live token claims again for every action", () => {
  const hook = renderHook(useEvidenceAdmission);
  const originalKey = hook.result.current.key;
  const getToken = hook.result.current.getToken;
  state.app.bearer = bearer("one", "org", "issuer", 2);
  hook.rerender();
  expect(hook.result.current.key).toBe(originalKey);
  expect(getToken()).toBe(state.app.bearer);
  state.app.bearer = bearer("one", "org", "foreign-issuer", 3);
  // Ref changed before a render is still checked against the admitted issuer.
  expect(getToken).toThrow();
});

it.each(["user", "org", "token", "issuer", "workspace"])("refuses a fresh child %s mismatch beneath a previously admitted parent", (kind) => {
  const parent = { key: "frozen-detail", ready: true, getToken: () => bearer(), graphs: [], currentGraphId: "graph" };
  const wrapper = ({ children }: { children: ReactNode }) => <EvidenceAdmissionContext.Provider value={parent}>{children}</EvidenceAdmissionContext.Provider>;
  const hook = renderHook(useEvidenceAdmission, { wrapper });
  const priorAction = hook.result.current.getToken;
  expect(hook.result.current.ready).toBe(true);
  if (kind === "user") state.live.userId = "two";
  if (kind === "org") state.live.orgId = "other";
  if (kind === "token") state.app.bearer = bearer("two");
  if (kind === "issuer") state.app.bearer = bearer("one", "org", "foreign-issuer");
  if (kind === "workspace") state.app.currentGraphId = "other-graph";
  hook.rerender();
  expect(hook.result.current.ready).toBe(false);
  expect(priorAction).toThrow();
  expect(hook.result.current.getToken).toThrow();
});
it("retains a parent's detail fence and permits only matching credential rotations", () => {
  let parentToken = bearer("one", "org", "issuer", 2);
  const parent = { key: "frozen-detail", ready: true, getToken: vi.fn(() => parentToken), graphs: [], currentGraphId: "graph" };
  const wrapper = ({ children }: { children: ReactNode }) => <EvidenceAdmissionContext.Provider value={parent}>{children}</EvidenceAdmissionContext.Provider>;
  const hook = renderHook(useEvidenceAdmission, { wrapper });
  expect(hook.result.current.getToken()).toBe(parentToken);
  parentToken = bearer("other");
  expect(hook.result.current.getToken).toThrow();
  parent.getToken.mockImplementation(() => { throw new Error("Selected detail changed"); });
  expect(hook.result.current.getToken).toThrow("Selected detail changed");
});
