// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render as renderUI, screen, fireEvent, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { QueryClient, QueryClientProvider, QueryObserver } from "@tanstack/react-query";
import { beforeEach, afterEach, expect, it, vi } from "vitest";
const mocks = vi.hoisted(() => { const replace = vi.fn(); return { replace, router: { replace }, getAuthToken: () => "token", get: vi.fn(), search: "provider=slack&result=ok" }; });
vi.mock("next/navigation", () => ({ useRouter: () => mocks.router, useSearchParams: () => new URLSearchParams(mocks.search) }));
vi.mock("@/lib/providers", () => ({ useApp: () => ({ authReady: true, getAuthToken: mocks.getAuthToken, queryScope: "scope" }) }));
vi.mock("@/lib/connectorsApi", () => ({ connectorsApi: {} }));
vi.mock("@/lib/pageSectionRefresh", () => ({ refreshPageSection: mocks.get }));
function render(node: ReactNode) { return renderUI(<QueryClientProvider client={new QueryClient()}>{node}</QueryClientProvider>); }
import Page from "@/app/(app)/connectors/return/page";
import { connectorCallbackDeadline } from "@/lib/connectorActionPolicy";
beforeEach(() => { mocks.search = "provider=slack&result=ok"; });
afterEach(() => { cleanup(); sessionStorage.clear(); vi.clearAllMocks(); });
it("resumes onboarding after verifying the returned grant", async () => {
 sessionStorage.setItem("connector-return:slack", "onboarding");
 mocks.get.mockResolvedValue({ status: "setting_up" });
 render(<Page />);
 await waitFor(() => expect(mocks.replace).toHaveBeenCalledWith("/onboarding/connectors?setup=slack"));
});
it("keeps a failed sign-in recovery in onboarding", async () => {
 sessionStorage.setItem("connector-return:slack", "onboarding");
 mocks.get.mockResolvedValue({ status: "not_connected" });
 render(<Page />);
 fireEvent.click(await screen.findByRole("button", { name: "Back to connectors" }));
 expect(mocks.replace).toHaveBeenCalledWith("/onboarding/connectors");
});
it("defaults ordinary connections to the connectors grid", async () => {
 mocks.get.mockResolvedValue({ status: "setting_up" });
 render(<Page />);
 await waitFor(() => expect(mocks.replace).toHaveBeenCalledWith("/connectors?setup=slack"));
});

it("marks Home's attention read stale once the connection lands", async () => {
 mocks.get.mockResolvedValue({ status: "setting_up" });
 const client = new QueryClient();
 client.setQueryData(["homeAttention", "scope"], { has_source: false });
 renderUI(<QueryClientProvider client={client}><Page /></QueryClientProvider>);
 await waitFor(() => expect(mocks.replace).toHaveBeenCalledWith("/connectors?setup=slack"));
 expect(client.getQueryState(["homeAttention", "scope"])?.isInvalidated).toBe(true);
});

it("notes the callback before a slow refetch of the connector list can outlive the acknowledgement", async () => {
 // The worker acknowledges with a deadline 20 ms out; the connector list is on
 // screen and its refetch takes 120 ms. Awaiting that refetch first let the
 // expiry abort the controller and silently skip the callback note.
 const deadline = Date.now() + 20;
 mocks.get.mockImplementation(async (_token: string, _model: string, _params: unknown, _signal: AbortSignal, opts: { onAcknowledged: (time: number) => void }) => {
  opts.onAcknowledged(deadline);
  return { status: "setting_up" };
 });
 const client = new QueryClient();
 const list = new QueryObserver(client, { queryKey: ["connectors", "scope"], queryFn: () => new Promise((resolve) => setTimeout(() => resolve([]), 120)) });
 const stop = list.subscribe(() => {});
 renderUI(<QueryClientProvider client={client}><Page /></QueryClientProvider>);
 await waitFor(() => expect(mocks.replace).toHaveBeenCalledWith("/connectors?setup=slack"));
 expect(connectorCallbackDeadline(client, "scope", "slack")).toBe(deadline);
 stop();
});

it("marks Home's attention read stale when the connection is still pending", async () => {
 mocks.get.mockResolvedValue({ status: "not_connected" });
 const client = new QueryClient();
 client.setQueryData(["homeAttention", "scope"], { has_source: false });
 renderUI(<QueryClientProvider client={client}><Page /></QueryClientProvider>);
 expect(await screen.findByRole("alert")).toHaveTextContent("Sign-in could not be completed");
 expect(client.getQueryState(["homeAttention", "scope"])?.isInvalidated).toBe(true);
});

it("refuses a missing callback result without starting a refresh or changing scope", async () => {
 mocks.search = "provider=slack";
 render(<Page />);
 expect(await screen.findByRole("alert")).toHaveTextContent("Sign-in could not be completed");
 expect(mocks.get).not.toHaveBeenCalled();
 expect(mocks.replace).not.toHaveBeenCalled();
});
