import { QueryClientProvider } from "@tanstack/react-query";
import { render as rtlRender, renderHook as rtlRenderHook, type RenderOptions } from "@testing-library/react";
import { createElement, type ReactElement, type ReactNode } from "react";
import { vi } from "vitest";
import { makeQueryClient } from "../queryClient";

/* Test-only. Library reads run on TanStack Query (lib/useLibraryRead.ts), so
 * a component that reads Library material needs the app's QueryClientProvider
 * above it. Each render gets a fresh client with the app's defaults, so no
 * result can leak between tests; the wrapper persists across `rerender`. */
function wrapper() {
  const client = makeQueryClient();
  return function QueryWrapper({ children }: { children: ReactNode }) {
    return createElement(QueryClientProvider, { client }, children);
  };
}

export function render(ui: ReactElement, options?: Omit<RenderOptions, "wrapper">): ReturnType<typeof rtlRender> {
  return rtlRender(ui, { ...options, wrapper: wrapper() });
}

export function renderHook<Result, Props>(hook: (props: Props) => Result, options?: { initialProps?: Props }) {
  return rtlRenderHook(hook, { ...options, wrapper: wrapper() });
}

/** One tab return, past the revalidation floor (lib/useReturnRefetch.ts):
 *  hide, then show the document and fire the window `focus` that a real
 *  return also delivers. Pins `Date.now` forward; `vi.restoreAllMocks()`
 *  undoes it. Wrap in `act`. */
export function returnToTab() {
  const later = Date.now() + 60_000;
  let state: DocumentVisibilityState = "hidden";
  Object.defineProperty(document, "visibilityState", { configurable: true, get: () => state });
  document.dispatchEvent(new Event("visibilitychange"));
  vi.spyOn(Date, "now").mockReturnValue(later);
  state = "visible";
  document.dispatchEvent(new Event("visibilitychange"));
  window.dispatchEvent(new Event("focus"));
}
