// @vitest-environment jsdom
import { cleanup, render } from "@testing-library/react";
import { QueryClientProvider } from "@tanstack/react-query";
import { afterEach, expect, it, vi } from "vitest";
import { conversationKey } from "./appQueries";
import { makeQueryClient } from "./queryClient";
import { QueryCacheIdentityBoundary } from "./queryCacheIdentityBoundary";

const clerk = vi.hoisted(() => ({ user: { id: "user_a" } as { id: string } | null, orgId: null as string | null }));
vi.mock("@clerk/nextjs", () => ({ useUser: () => ({ user: clerk.user }), useAuth: () => ({ orgId: clerk.orgId }) }));

afterEach(() => { cleanup(); clerk.user = { id: "user_a" }; clerk.orgId = null; });

const key = conversationKey("scope", "c1");
function mountWithEntry() {
  const qc = makeQueryClient();
  qc.setQueryData(key, { conversation: { id: "c1" }, messages: [] });
  // A new element each time: React skips a subtree whose element is unchanged.
  const tree = () => <QueryClientProvider client={qc}><QueryCacheIdentityBoundary><span>child</span></QueryCacheIdentityBoundary></QueryClientProvider>;
  const view = render(tree());
  expect(qc.getQueryData(key)).toBeDefined();
  return { qc, rerender: () => view.rerender(tree()) };
}

it.each([
  ["another user", () => { clerk.user = { id: "user_b" }; }],
  ["signed out", () => { clerk.user = null; }],
  ["the same user in another org", () => { clerk.orgId = "org_1"; }],
])("drops an opened chat's entry when the identity becomes %s", (_label, change) => {
  const { qc, rerender } = mountWithEntry();
  change();
  rerender();
  expect(qc.getQueryData(key)).toBeUndefined();
});

it("keeps the entry across renders under the same identity", () => {
  const { qc, rerender } = mountWithEntry();
  rerender();
  expect(qc.getQueryData(key)).toBeDefined();
});
