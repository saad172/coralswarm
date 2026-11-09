// @vitest-environment jsdom
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { AppProvider, useApp } from "./providers";
import { homeContinueQueryOptions } from "./homeLaunchpadApi";
import { ApiError } from "./api";

const clerk = vi.hoisted(() => ({ orgId: "org_a", userId: "user_a", tokenOrg: "org_a" }));
const token = () => `e30.${btoa(JSON.stringify({ sub: clerk.userId, org_id: clerk.tokenOrg, iss: "https://clerk.test" }))}.signature`;
vi.mock("@clerk/nextjs", () => ({
  useAuth: () => ({ isLoaded: true, isSignedIn: true, orgId: clerk.orgId, getToken, signOut }),
  useUser: () => ({ user: { id: clerk.userId } }),
}));
const getToken = async () => token();
const signOut = async () => {};
vi.mock("./clientMetrics", () => ({ startClientMetrics: () => () => {} }));
vi.mock("./devBearer", () => ({ readDevBearer: () => null }));
const wire = vi.hoisted(() => ({ read: vi.fn() }));
vi.mock("./api", async (original) => ({ ...await original<typeof import("./api")>(), apiRequest: (...args: unknown[]) => wire.read(...args) }));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const boot = (org: string) => ({ me: { account_id: org, user_id: "internal", is_personal_org: false, active_graph_id: "g" },
  graphs: [{ id: "g", name: "Primary", kind: "org" }], activities_enabled: true, gist_enabled: true });
let client: QueryClient;
function Content() {
  const app = useApp();
  const query = useQuery(homeContinueQueryOptions({ scope: app.queryScope, getAuthToken: app.getAuthToken }));
  return <main>{JSON.stringify(query.data)}</main>;
}
function Gate() {
  const app = useApp();
  client = useQueryClient();
  return app.authReady ? <Content /> : <div>{app.bootError ?? "booting"}</div>;
}
beforeEach(() => {
  window.history.replaceState(null, "", "/home");
  clerk.orgId = clerk.tokenOrg = "org_a";
  wire.read.mockReset();
});
afterEach(() => { cleanup(); client?.clear(); });

it("overlaps real provider boot/factories and joins one Home read only after admission", async () => {
  const held = deferred<ReturnType<typeof boot>>();
  wire.read.mockImplementation((path: string) => path === "/v1/bootstrap?billing_banner=trial_v1" ? held.promise : Promise.resolve({ items: ["current"], generated_at: new Date().toISOString() }));
  const view = render(<AppProvider><Gate /></AppProvider>);
  await waitFor(() => expect(wire.read.mock.calls.map(([p]) => p)).toContain("/v1/home/continue"));
  expect(view.queryByRole("main")).toBeNull();
  expect(client.getQueryState(["homeContinue", "user_a::org_a"])?.fetchStatus).toBe("fetching");
  await act(async () => held.resolve(boot("org_a")));
  await waitFor(() => expect(view.getByRole("main").textContent).toContain("current"));
  expect(wire.read.mock.calls.filter(([p]) => p === "/v1/home/continue")).toHaveLength(1);
});

it("failed bootstrap removes speculative results and keeps content withheld", async () => {
  const held = deferred<ReturnType<typeof boot>>();
  wire.read.mockImplementation((path: string) => path === "/v1/bootstrap?billing_banner=trial_v1" ? held.promise : Promise.resolve({ items: ["private"] }));
  const view = render(<AppProvider><Gate /></AppProvider>);
  await waitFor(() => expect(wire.read.mock.calls.length).toBe(5));
  await act(async () => held.reject(new ApiError("Unavailable", 503)));
  await waitFor(() => expect(client.getQueryState(["homeContinue", "user_a::org_a"])).toBeUndefined());
  expect(view.queryByRole("main")).toBeNull();
});

it("late old-org responses cannot survive an identity change during boot", async () => {
  const oldBoot = deferred<ReturnType<typeof boot>>(), oldRead = deferred<unknown>();
  wire.read.mockImplementation((path: string, bearer: string) => {
    const org = JSON.parse(atob(bearer.split(".")[1])).org_id;
    if (org === "org_a") return path === "/v1/bootstrap?billing_banner=trial_v1" ? oldBoot.promise : oldRead.promise;
    return Promise.resolve(path === "/v1/bootstrap?billing_banner=trial_v1" ? boot(org) : { items: ["new-org"] });
  });
  const view = render(<AppProvider><Gate /></AppProvider>);
  await waitFor(() => expect(wire.read.mock.calls.length).toBe(5));
  clerk.orgId = clerk.tokenOrg = "org_b";
  view.rerender(<AppProvider><Gate /></AppProvider>);
  await waitFor(() => expect(view.getByRole("main").textContent).toContain("new-org"));
  await act(async () => { oldRead.resolve({ items: ["old-org-private"] }); oldBoot.resolve(boot("org_a")); });
  expect(client.getQueryCache().getAll().filter(q => JSON.stringify(q.state.data ?? null).includes("old-org-private"))).toHaveLength(0);
  expect(client.getQueryState(["homeContinue", "user_a::org_a"])).toBeUndefined();
});

it("never speculates with a token for a different org", async () => {
  clerk.tokenOrg = "org_other";
  wire.read.mockImplementation(() => new Promise(() => {}));
  render(<AppProvider><Gate /></AppProvider>);
  await waitFor(() => expect(wire.read).toHaveBeenCalledTimes(1));
  expect(wire.read.mock.calls[0][0]).toBe("/v1/bootstrap?billing_banner=trial_v1");
});

it("keeps an early refusal visible after admission without restoring its payload", async () => {
  const held = deferred<ReturnType<typeof boot>>();
  wire.read.mockImplementation((path: string) => {
    if (path === "/v1/bootstrap?billing_banner=trial_v1") return held.promise;
    if (path === "/v1/home/continue") return Promise.reject(new ApiError("Access revoked", 403));
    return Promise.resolve({ items: [] });
  });
  render(<AppProvider><Gate /></AppProvider>);
  await waitFor(() => expect(client.getQueryState(["homeContinue", "user_a::org_a"])?.status).toBe("error"));
  await act(async () => held.resolve(boot("org_a")));
  await waitFor(() => expect(client.getQueryState(["homeContinue", "user_a::org_a"])?.fetchStatus).toBe("idle"));
  expect(client.getQueryData(["homeContinue", "user_a::org_a"])).toBeUndefined();
  expect(client.getQueryState(["homeContinue", "user_a::org_a"])?.error).toBeInstanceOf(ApiError);
});

it.each(["/today", "/workspaces?id=selected"])("does not guess parameters for %s", async (path) => {
  window.history.replaceState(null, "", path);
  wire.read.mockImplementation(() => new Promise(() => {}));
  render(<AppProvider><Gate /></AppProvider>);
  await waitFor(() => expect(wire.read).toHaveBeenCalledTimes(2));
  expect(wire.read.mock.calls.map(([path]) => path).sort()).toEqual(["/v1/bootstrap?billing_banner=trial_v1", "/v1/conversations"]);
});
