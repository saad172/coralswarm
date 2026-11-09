// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { render } from "@/lib/testing/queryRender";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FollowedThreads } from "./FollowedThreads";
import { ApiError, api, type WatchedThreadsResponse } from "@/lib/api";

const identity = vi.hoisted(() => ({
  app: { getAuthToken: () => "example_17" as string | null, authReady: true, ready: true,
    accountId: "account-a" as string | null, isPersonalOrg: false, resolvedOrgId: null as string | null },
  auth: { isLoaded: true, orgId: null as string | null, sessionId: "session-a" as string | null, userId: "user-a" as string | null },
}));
vi.mock("@/lib/providers", () => ({ useApp: () => identity.app }));
vi.mock("@clerk/nextjs", () => ({ useAuth: () => identity.auth }));
vi.mock("@/lib/api", async () => ({
  ...await vi.importActual<typeof import("@/lib/api")>("@/lib/api"),
  api: { connectors: { watchedThreads: vi.fn(), setThreadFollow: vi.fn() } },
}));

const list = vi.mocked(api.connectors.watchedThreads);
const stop = vi.mocked(api.connectors.setThreadFollow);
const row = (id: string, following = true) => ({ thread_id: id, title: id, following, saved_at: "2026-09-14T15:00:00Z" });
const empty = { threads: [], next_cursor: null };
const TOKEN = "example_17";
const gmail = () => within(screen.getByRole("region", { name: "Gmail followed threads" }));
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

beforeEach(() => {
  identity.app = { getAuthToken: () => TOKEN, authReady: true, ready: true,
    accountId: "account-a", isPersonalOrg: false, resolvedOrgId: null };
  identity.auth = { isLoaded: true, orgId: null, sessionId: "session-a", userId: "user-a" };
  list.mockImplementation(async (_token, provider) => provider === "google"
    ? { threads: [row("Planning")], next_cursor: null } : empty);
  stop.mockResolvedValue({ following: false });
});
afterEach(() => { cleanup(); vi.resetAllMocks(); });

describe("FollowedThreads", () => {
  it("shows only followed rows, with connected mailbox status", async () => {
    list.mockImplementation(async (_token, provider) => provider === "google"
      ? { threads: [row("Planning"), row("Stopped", false)], next_cursor: null } : empty);
    render(<FollowedThreads connections={[{ provider: "google_gmail", provider_workspace_id: null, scope: "", expires_at: null, connected_at: "2026-09-14", missing_scopes: [], scopes_unknown: false }]} />);
    expect(await screen.findByText("Planning")).toBeInTheDocument();
    expect(screen.queryByText("Stopped")).not.toBeInTheDocument();
    expect(gmail().getByText("connected")).toBeInTheDocument();
    expect(list).toHaveBeenCalledWith(TOKEN, "google", undefined);
    expect(list).toHaveBeenCalledWith(TOKEN, "microsoft", undefined);
  });

  it("does not claim empty when the first page contains only unfollowed rows", async () => {
    list.mockImplementation(async (_token, provider, cursor) => provider !== "google" ? empty : cursor
      ? { threads: [row("Older followed")], next_cursor: null }
      : { threads: [row("Stopped", false)], next_cursor: "opaque-cursor" });
    render(<FollowedThreads connections={[]} />);
    const more = await screen.findByRole("button", { name: "Load more Gmail threads" });
    expect(gmail().queryByText(/No Gmail threads are followed/)).not.toBeInTheDocument();
    expect(gmail().getByText(/Load older saved threads/)).toBeInTheDocument();
    fireEvent.click(more);
    expect(await screen.findByText("Older followed")).toBeInTheDocument();
    expect(list).toHaveBeenLastCalledWith(TOKEN, "google", "opaque-cursor");
    expect(screen.queryByRole("button", { name: "Load more Gmail threads" })).not.toBeInTheDocument();
  });

  it("deduplicates rows across progressive pages", async () => {
    list.mockImplementation(async (_token, provider, cursor) => provider !== "google" ? empty : cursor
      ? { threads: [row("Planning"), row("Older")], next_cursor: null }
      : { threads: [row("Planning")], next_cursor: "next" });
    render(<FollowedThreads connections={[]} />);
    fireEvent.click(await screen.findByRole("button", { name: "Load more Gmail threads" }));
    await screen.findByText("Older");
    expect(screen.getAllByText("Planning")).toHaveLength(1);
  });

  it("keeps errors distinct from empty lists and retries missing routes", async () => {
    list.mockImplementation(async (_token, provider) => {
      if (provider === "google") throw new ApiError("not found", 404);
      return empty;
    });
    render(<FollowedThreads connections={[]} />);
    expect(await screen.findByText(/aren't available on this backend yet/)).toBeInTheDocument();
    expect(gmail().queryByText(/No Gmail threads are followed/)).not.toBeInTheDocument();
    list.mockResolvedValue(empty);
    fireEvent.click(gmail().getByRole("button", { name: "Retry" }));
    expect(await screen.findByText(/No Gmail threads are followed/)).toBeInTheDocument();
  });

  it("rejects a malformed successful list instead of reporting empty", async () => {
    list.mockResolvedValue({ threads: [] } as unknown as WatchedThreadsResponse);
    render(<FollowedThreads connections={[]} />);
    // The two mailboxes settle independently.
    await waitFor(() => expect(screen.getAllByText(/Couldn't load followed threads/)).toHaveLength(2));
    expect(screen.queryByText(/No Gmail threads are followed/)).not.toBeInTheDocument();
  });

  it("stops via PUT false and removes only the confirmed unfollowed row", async () => {
    render(<FollowedThreads connections={[]} />);
    fireEvent.click(await screen.findByRole("button", { name: "Stop following Planning" }));
    await waitFor(() => expect(screen.queryByText("Planning")).not.toBeInTheDocument());
    expect(stop).toHaveBeenCalledWith(TOKEN, "google", "Planning", false);
    expect(gmail().getByText("Stopped following")).toBeInTheDocument();
  });

  it("reflects a returned following=true instead of assuming the stop landed", async () => {
    stop.mockResolvedValue({ following: true });
    render(<FollowedThreads connections={[]} />);
    fireEvent.click(await screen.findByRole("button", { name: "Stop following Planning" }));
    expect(await screen.findByText(/This thread is still followed/)).toBeInTheDocument();
    expect(screen.getByText("Planning")).toBeInTheDocument();
  });

  it("retains rows on failed stop and permits retrying the row action", async () => {
    stop.mockRejectedValueOnce(new ApiError("down", 500));
    render(<FollowedThreads connections={[]} />);
    fireEvent.click(await screen.findByRole("button", { name: "Stop following Planning" }));
    await screen.findByText(/Couldn't stop following this thread/);
    expect(screen.getByText("Planning")).toBeInTheDocument();
    expect(gmail().queryByRole("button", { name: "Retry" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Stop following Planning" }));
    await waitFor(() => expect(screen.queryByText("Planning")).not.toBeInTheDocument());
  });

  it("serializes stop and pagination, including double clicks", async () => {
    list.mockImplementation(async (_token, provider) => provider === "google"
      ? { threads: [row("Planning")], next_cursor: "next" } : empty);
    const pending = deferred<{ following: boolean }>();
    stop.mockReturnValue(pending.promise);
    render(<FollowedThreads connections={[]} />);
    const button = await screen.findByRole("button", { name: "Stop following Planning" });
    fireEvent.click(button); fireEvent.click(button);
    expect(stop).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "Load more Gmail threads" })).toBeDisabled();
    await act(async () => pending.resolve({ following: false }));
  });

  it("hides outgoing rows during account changes and rejects late list answers", async () => {
    const pending = deferred<WatchedThreadsResponse>();
    list.mockImplementation((_token, provider) => provider === "google" ? pending.promise : Promise.resolve(empty));
    const view = render(<FollowedThreads connections={[]} />);
    identity.app.accountId = "account-b";
    list.mockResolvedValue(empty);
    view.rerender(<FollowedThreads connections={[]} />);
    await screen.findByText(/No Gmail threads are followed/);
    await act(async () => pending.resolve({ threads: [row("Account A private")], next_cursor: null }));
    expect(screen.queryByText("Account A private")).not.toBeInTheDocument();
  });

  it("rejects late stop answers from a prior account", async () => {
    const pending = deferred<{ following: boolean }>();
    stop.mockReturnValue(pending.promise);
    const view = render(<FollowedThreads connections={[]} />);
    fireEvent.click(await screen.findByRole("button", { name: "Stop following Planning" }));
    identity.app.accountId = "account-b";
    list.mockResolvedValue({ threads: [row("Account B private")], next_cursor: null });
    view.rerender(<FollowedThreads connections={[]} />);
    await screen.findAllByText("Account B private");
    await act(async () => pending.resolve({ following: false }));
    expect(screen.queryByText("Stopped following")).not.toBeInTheDocument();
    expect(screen.getAllByText("Account B private")).toHaveLength(2);
  });

  it("never mounts lists in real orgs or with stale org metadata; supports personal orgs", async () => {
    identity.auth.orgId = "org-real";
    identity.app.resolvedOrgId = "org-real";
    const view = render(<FollowedThreads connections={[]} />);
    expect(list).not.toHaveBeenCalled();
    identity.app.isPersonalOrg = true;
    identity.app.resolvedOrgId = "old-personal-org";
    view.rerender(<FollowedThreads connections={[]} />);
    expect(list).not.toHaveBeenCalled();
    identity.app.resolvedOrgId = "org-real";
    identity.app.getAuthToken = () => `h.${btoa(JSON.stringify({ sub: "user-a", org_id: "org-real" }))}.s`;
    view.rerender(<FollowedThreads connections={[]} />);
    expect(await screen.findByText("Planning")).toBeInTheDocument();
  });

  it("hides private rows immediately on sign-out", async () => {
    const view = render(<FollowedThreads connections={[]} />);
    await screen.findByText("Planning");
    identity.auth.sessionId = null;
    view.rerender(<FollowedThreads connections={[]} />);
    expect(screen.queryByText("Planning")).not.toBeInTheDocument();
  });

  it("does not mount a new user's list with a bearer from the outgoing user", () => {
    identity.auth.userId = "user-b";
    render(<FollowedThreads connections={[]} />);
    expect(list).not.toHaveBeenCalled();
    expect(screen.queryByText("Followed threads")).not.toBeInTheDocument();
  });
});
