// @vitest-environment jsdom
import { afterEach, describe, expect, it } from "vitest";
import { mark, timings } from "./perf";

afterEach(() => { performance.clearMarks(); });

describe("startup timing marks", () => {
  it("records each phase once, in order, and exposes it through window.__csTimings", () => {
    mark("office-ready"); mark("office-ready"); mark("connected");
    const rows = timings();
    expect(rows.map((r) => r.phase)).toEqual(["office-ready", "connected"]);
    expect(rows[1].ms).toBeGreaterThanOrEqual(rows[0].ms);
    expect(window.__csTimings?.().map((r) => r.phase)).toEqual(["office-ready", "connected"]);
  });

  it("ignores marks that are not ours", () => {
    performance.mark("somebody-else");
    expect(timings()).toEqual([]);
    performance.clearMarks("somebody-else");
  });

  it("never throws when User Timing is missing", () => {
    const original = performance.mark;
    (performance as { mark: unknown }).mark = undefined;
    expect(() => mark("connected")).not.toThrow();
    (performance as { mark: unknown }).mark = original;
  });
});
