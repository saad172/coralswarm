import { expect, it } from "vitest";
import { pageRedirectDestination } from "./pageRedirectDestination";
import { pageRoutes } from "./pageRoutes";
it("declares only the root anonymous auth-gate outcome without changing product bookmark redirects", () => {
  expect(pageRoutes.filter(entry => "redirect" in entry && "anonymous" in entry.redirect).map(entry => entry.route)).toEqual(["/"]);
  expect(pageRedirectDestination("/", "?discard=1")).toBe("/home");
  expect(pageRedirectDestination("/", "?discard=1", { audience: "authenticated" })).toBe("/home");
  expect(pageRedirectDestination("/", "?discard=1", { audience: "anonymous" })).toBe("/login");
  for (const route of ["/home", "/shared", "/signup", "/login"]) expect(pageRedirectDestination(route, "", { audience: "anonymous" })).toBeNull();
  expect(pageRedirectDestination("/ask", "?q=unchanged", { audience: "anonymous" })).toBe("/chats?q=unchanged");
});
it("resolves retired routes before shell work and retains the established query behavior", () => {
  expect(pageRedirectDestination("/ask", "?q=a+b&tag=x&tag=y")).toBe("/chats?q=a+b&tag=x&tag=y");
  expect(pageRedirectDestination("/atlas", "?old=selector")).toBe("/home");
  expect(pageRedirectDestination("/connectors/google-chat", "?old=selector")).toBe("/connectors/google");
  expect(pageRedirectDestination("/connectors/github/setup", "?installation_id=123&setup_action=install")).toBe("/connectors/github?installation_id=123");
  expect(pageRedirectDestination("/library", "?old=selector")).toBe("/home");
  expect(pageRedirectDestination("/library/history", "")).toBe("/home");
  expect(pageRedirectDestination("/library/people", "")).toBe("/people");
  expect(pageRedirectDestination("/settings/account", "?tab=plan")).toBe("/settings?tab=plan");
  expect(pageRedirectDestination("/connectors/composio", "?toolkit=jira")).toBe("/connectors/jira");
  expect(pageRedirectDestination("/connectors/composio", "?toolkit=../private")).toBeNull();
  expect(pageRedirectDestination("/topics", "?id=node")).toBeNull();
  expect(pageRedirectDestination("/topics", "?graph_id=old")).toBe("/home");
  expect(pageRedirectDestination("/home", "")).toBeNull();
});
