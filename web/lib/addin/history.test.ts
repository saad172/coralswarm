import { describe, expect, it } from "vitest";
import { guardHistory, HISTORY_GUARD_SCRIPT } from "./history";

function fakeWin(nulled: boolean) {
  const push = () => "push", replace = () => "replace";
  class History { pushState = push; replaceState = replace; }
  const proto = { pushState: push, replaceState: replace };
  const history: Record<string, unknown> = Object.create(proto);
  if (nulled) { history.pushState = null; history.replaceState = null; }
  return { win: { history, History: { prototype: proto }, document: {} } as unknown as Window, push, replace, History };
}

describe("guardHistory", () => {
  it("restores functions Office nulled from History.prototype", () => {
    const { win, push, replace } = fakeWin(true);
    guardHistory(win);
    expect(win.history.pushState).toBe(push);
    expect(win.history.replaceState).toBe(replace);
  });
  it("ignores a later null assignment but accepts a replacement function", () => {
    const { win, push } = fakeWin(false);
    guardHistory(win);
    (win.history as unknown as Record<string, unknown>).pushState = null;
    expect(win.history.pushState).toBe(push);
    const next = () => {};
    (win.history as unknown as Record<string, unknown>).pushState = next;
    expect(win.history.pushState).toBe(next);
  });
  it("is idempotent", () => {
    const { win, replace } = fakeWin(true);
    guardHistory(win); guardHistory(win);
    expect(win.history.replaceState).toBe(replace);
  });
  it("serializes to a self-contained inline script gated to Office routes", () => {
    expect(HISTORY_GUARD_SCRIPT).toContain('"/outlook"');
    expect(HISTORY_GUARD_SCRIPT).toContain('"/addin"');
    expect(() => new Function(HISTORY_GUARD_SCRIPT)).not.toThrow();
  });
});
