// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { render } from "@/lib/testing/queryRender";
import { OrgDeletionPage } from "./OrgDeletionPage";
import { ApiError } from "@/lib/api";
import { pageRoutes } from "@/lib/pageRoutes";

const state = vi.hoisted(() => ({ orgId: null as string | null, read: vi.fn() }));
vi.mock("@clerk/nextjs", () => ({
  useAuth: () => ({ isLoaded: true, isSignedIn: true, orgId: state.orgId, userId: "clerk-user", getToken: async () =>
    `e30.${btoa(JSON.stringify({ sub: "clerk-user", iss: "https://clerk.test", org_id: state.orgId }))}.sig` }),
  useClerk: () => ({ setActive: vi.fn() }),
  useReverification: (fn: unknown) => fn,
}));
vi.mock("@/lib/api", async (actual) => ({ ...(await actual<typeof import("@/lib/api")>()), apiRequest: (...args: unknown[]) => state.read(...args) }));
afterEach(() => { cleanup(); state.orgId = null; state.read.mockReset(); });

it("shows no organization selected without constructing a missing address or fetching", () => {
  render(<OrgDeletionPage orgParam={null} />);
  expect(screen.getByRole("heading", { name: "No organization selected" })).toBeInTheDocument();
  expect(state.read).not.toHaveBeenCalled();
});

it.each(["parameter", "active"])("reads a real organization supplied by %s", async (kind) => {
  if (kind === "active") state.orgId = "org_real";
  state.read.mockResolvedValue({ org_name: "Real org", is_owner: false, deletion: null });
  render(<OrgDeletionPage orgParam={kind === "parameter" ? "org_real" : null} />);
  expect(await screen.findByRole("heading", { name: "Delete organization" })).toBeInTheDocument();
  await waitFor(() => expect(state.read).toHaveBeenCalledTimes(1));
  expect(state.read.mock.calls[0][0]).toBe("/v1/orgs/org_real/deletion");
});

it("preserves the disabled endpoint refusal and declares only its exact load path/status", async () => {
  state.orgId = "org_real";
  state.read.mockRejectedValue(new ApiError("not found", 404));
  render(<OrgDeletionPage orgParam={null} />);
  expect(await screen.findByRole("heading", { name: "Not available" })).toBeInTheDocument();
  expect(screen.getByText(/Deleting an organization isn.t available yet/)).toBeInTheDocument();
  expect(state.read).toHaveBeenCalledTimes(1);
  expect(screen.queryByRole("heading", { name: "Delete organization" })).not.toBeInTheDocument();
  const declaration = pageRoutes.find((route) => route.route === "/org/deletion");
  if (!declaration || !("refusals" in declaration)) throw new Error("Missing disabled org-deletion declaration");
  expect(declaration.refusals).toHaveLength(1);
  expect(declaration.refusals[0]).toMatchObject({ path: "/v1/orgs/{org_id}/deletion", status: 404 });
  expect(declaration.refusals[0].reason).toContain("ORG_DELETION_ENABLED");
});
