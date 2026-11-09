import { afterEach } from "vitest";
import { cleanup } from "@testing-library/preact";
import { uninstallFakeOpenAi } from "./fakeOpenAi";

// jsdom has no matchMedia implementation at all (calling it throws
// "not a function") — `useHostLayout`'s browser fallback (touch/hover via
// `(pointer: coarse)` / `(hover: hover)`) needs SOME implementation to
// exercise that code path. Default to "no" for every query — an individual
// test can still override `window.matchMedia` for a specific query.
if (typeof window !== "undefined" && typeof window.matchMedia !== "function") {
  window.matchMedia = (query: string): MediaQueryList => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: () => {},
    removeListener: () => {},
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => false,
  });
}

afterEach(() => {
  cleanup();
  uninstallFakeOpenAi();
});
