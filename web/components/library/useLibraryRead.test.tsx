// @vitest-environment jsdom
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { QueryClientProvider } from "@tanstack/react-query";
import { createElement, type ReactNode } from "react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ApiError } from "@/lib/api";
import { makeQueryClient } from "@/lib/queryClient";
import { TransportError } from "@/lib/transport";
import { useLibraryRead } from "@/lib/useLibraryRead";

let visibility: DocumentVisibilityState = "visible";
beforeEach(() => {
  visibility = "visible";
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => visibility });
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});
function wrapper() {
  const client = makeQueryClient();
  return function Wrapper({ children }: { children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((r, j) => {
    resolve = r;
    reject = j;
  });
  return { promise, resolve, reject };
}
/** Leave the tab, let the floor pass, and come back: one return. */
function returnToTab() {
  visibility = "hidden";
  document.dispatchEvent(new Event("visibilitychange"));
  const later = Date.now() + 60_000;
  vi.spyOn(Date, "now").mockReturnValue(later);
  visibility = "visible";
  document.dispatchEvent(new Event("visibilitychange"));
  window.dispatchEvent(new Event("focus"));
}

it("never publishes a delayed prior-account response after the identity or graph changes", async () => {
  const old = deferred<string>();
  const load = vi
    .fn()
    .mockImplementationOnce(() => old.promise)
    .mockResolvedValueOnce("current account");
  const hook = renderHook(({ key }) => useLibraryRead(key, true, load), {
    initialProps: { key: "old-account/graph" },
    wrapper: wrapper(),
  });
  hook.rerender({ key: "new-account/graph" });
  expect(hook.result.current.data).toBeUndefined();
  await waitFor(() => expect(hook.result.current.data).toBe("current account"));
  await act(async () => old.resolve("private old evidence"));
  expect(hook.result.current.data).toBe("current account");
  expect(load.mock.calls[0][0].aborted).toBe(true);
});

it("hides the previous key's data in the same render as a key change, even once loaded", async () => {
  const next = deferred<string>();
  const load = vi.fn().mockResolvedValueOnce("account A evidence").mockImplementationOnce(() => next.promise);
  const hook = renderHook(({ key }) => useLibraryRead(key, true, load), {
    initialProps: { key: "account-A" },
    wrapper: wrapper(),
  });
  await waitFor(() => expect(hook.result.current.data).toBe("account A evidence"));
  hook.rerender({ key: "account-B" });
  expect(hook.result.current.data).toBeUndefined();
  expect(hook.result.current.loading).toBe(true);
  await act(async () => next.resolve("account B evidence"));
  await waitFor(() => expect(hook.result.current.data).toBe("account B evidence"));
});

it("keeps data on screen across a tab-return revalidation and replaces it on success", async () => {
  const second = deferred<string>();
  const load = vi.fn().mockResolvedValueOnce("first read").mockImplementationOnce(() => second.promise);
  const hook = renderHook(() => useLibraryRead("scope", true, load), { wrapper: wrapper() });
  await waitFor(() => expect(hook.result.current.data).toBe("first read"));
  act(() => returnToTab());
  await waitFor(() => expect(load).toHaveBeenCalledTimes(2));
  expect(hook.result.current.data).toBe("first read");
  expect(hook.result.current.loading).toBe(false);
  expect(hook.result.current.refreshing).toBe(true);
  await act(async () => second.resolve("fresh read"));
  await waitFor(() => expect(hook.result.current.data).toBe("fresh read"));
  expect(hook.result.current.refreshing).toBe(false);
});

it("a single return (visibilitychange + focus together) issues exactly one read", async () => {
  const load = vi.fn().mockResolvedValue("evidence");
  const hook = renderHook(() => useLibraryRead("scope", true, load), { wrapper: wrapper() });
  await waitFor(() => expect(hook.result.current.data).toBe("evidence"));
  expect(load).toHaveBeenCalledTimes(1);
  act(() => returnToTab());
  await waitFor(() => expect(load).toHaveBeenCalledTimes(2));
  await act(async () => new Promise((r) => setTimeout(r, 50)));
  expect(load).toHaveBeenCalledTimes(2);
});

it.each([
  ["401", 401],
  ["403 revoked", 403],
  ["404 deleted", 404],
] as const)("drops loaded evidence when revalidation is denied: %s", async (message, status) => {
  const load = vi.fn().mockResolvedValueOnce("readable evidence").mockRejectedValueOnce(new ApiError(message, status));
  const hook = renderHook(() => useLibraryRead("one-scope", true, load), { wrapper: wrapper() });
  await waitFor(() => expect(hook.result.current.data).toBe("readable evidence"));
  act(() => hook.result.current.refresh());
  await waitFor(() => expect(hook.result.current.error).toBe(message));
  expect(hook.result.current.data).toBeUndefined();
  expect(hook.result.current.loading).toBe(false);
});

it("fails closed when a revalidated response does not verify", async () => {
  const load = vi.fn().mockResolvedValueOnce("readable evidence").mockRejectedValueOnce(new Error("This saved chat could not be verified."));
  const hook = renderHook(() => useLibraryRead("one-scope", true, load), { wrapper: wrapper() });
  await waitFor(() => expect(hook.result.current.data).toBe("readable evidence"));
  act(() => hook.result.current.refresh());
  await waitFor(() => expect(hook.result.current.error).toBe("This saved chat could not be verified."));
  expect(hook.result.current.data).toBeUndefined();
});

it.each([
  ["timeout", new TransportError("timeout", "/v1/library/search", "request exceeded 15000ms"), "Library did not respond in time. Retry in a moment."],
  ["network", new TransportError("network", "/v1/library/search", "Failed to fetch"), "Library could not be reached. Check the connection and retry."],
  ["503", new ApiError("Service unavailable", 503), "Service unavailable"],
] as const)("keeps loaded evidence and reports a non-blocking note on a transient %s failure", async (_kind, failure, note) => {
  const load = vi.fn().mockResolvedValueOnce("readable evidence").mockRejectedValueOnce(failure).mockResolvedValueOnce("recovered");
  const hook = renderHook(() => useLibraryRead("one-scope", true, load), { wrapper: wrapper() });
  await waitFor(() => expect(hook.result.current.data).toBe("readable evidence"));
  act(() => hook.result.current.refresh());
  await waitFor(() => expect(hook.result.current.refreshError).toBe(note));
  expect(hook.result.current.data).toBe("readable evidence");
  expect(hook.result.current.error).toBeUndefined();
  act(() => hook.result.current.refresh());
  await waitFor(() => expect(hook.result.current.data).toBe("recovered"));
  expect(hook.result.current.refreshError).toBeUndefined();
});

it("shows the blocking loading state again when retrying after a denial", async () => {
  const retry = deferred<string>();
  const load = vi.fn().mockRejectedValueOnce(new ApiError("Forbidden", 403)).mockImplementationOnce(() => retry.promise);
  const hook = renderHook(() => useLibraryRead("scope", true, load), { wrapper: wrapper() });
  await waitFor(() => expect(hook.result.current.error).toBe("Forbidden"));
  act(() => hook.result.current.refresh());
  await waitFor(() => expect(hook.result.current.loading).toBe(true));
  expect(hook.result.current.error).toBeUndefined();
  await act(async () => retry.resolve("granted"));
  await waitFor(() => expect(hook.result.current.data).toBe("granted"));
});

it("hides results immediately on sign-out and performs a fresh read on return", async () => {
  const second = deferred<string>();
  const load = vi.fn().mockResolvedValueOnce("readable").mockImplementationOnce(() => second.promise);
  const hook = renderHook(({ enabled }) => useLibraryRead("scope", enabled, load), {
    initialProps: { enabled: true },
    wrapper: wrapper(),
  });
  await waitFor(() => expect(hook.result.current.data).toBe("readable"));
  hook.rerender({ enabled: false });
  expect(hook.result.current.data).toBeUndefined();
  hook.rerender({ enabled: true });
  // The pre-sign-out result is not revived while the fresh read runs.
  expect(hook.result.current.data).toBeUndefined();
  expect(hook.result.current.loading).toBe(true);
  await waitFor(() => expect(load).toHaveBeenCalledTimes(2));
  await act(async () => second.resolve("readable again"));
  await waitFor(() => expect(hook.result.current.data).toBe("readable again"));
});

it("does not revalidate while the tab is hidden or the read is disabled", async () => {
  const load = vi.fn().mockResolvedValue("readable");
  const hook = renderHook(({ enabled }) => useLibraryRead("scope", enabled, load), {
    initialProps: { enabled: false },
    wrapper: wrapper(),
  });
  act(() => returnToTab());
  expect(load).not.toHaveBeenCalled();
  hook.rerender({ enabled: true });
  await waitFor(() => expect(load).toHaveBeenCalledTimes(1));
  visibility = "hidden";
  act(() => {
    window.dispatchEvent(new Event("online"));
  });
  expect(load).toHaveBeenCalledTimes(1);
});

it.each([
  ["timeout", "request exceeded 15000ms", "Library did not respond in time. Retry in a moment."],
  ["network", "Failed to fetch", "Library could not be reached. Check the connection and retry."],
  ["breaker-open", "circuit open for https://api.example; not dispatching", "Library could not be reached. Check the connection and retry."],
] as const)("shows readable copy for a %s transport failure instead of its internal message", async (kind, internal, shown) => {
  const load = vi.fn().mockRejectedValue(new TransportError(kind, "/v1/library/search", internal));
  const hook = renderHook(() => useLibraryRead("scope", true, load), { wrapper: wrapper() });
  await waitFor(() => expect(hook.result.current.error).toBe(shown));
  expect(hook.result.current.data).toBeUndefined();
});
