import { expect, it } from "vitest";
import { PageRefreshExpiredError, StoredRefreshCycle, refreshRecoveryMs } from "./pageRefreshPolicy";
import type { Stored } from "./pageSectionWire";
// Factual server limits are constructor inputs here; runtime obtains them
// solely from the Rust-generated manifest, with no model list in this test.
const limits = { max_delay_seconds: 60, compute_timeout_ms: 30_000, compute_total_timeout_ms: 35_000, worker_base_poll_ms: 5_000 };
const pending: Stored<unknown> = { model: "conversation_list", version: 1, payload: {}, computed_at: null, stale: true, refresh_eta: "2026-10-31T00:00:02Z" };
it("uses a finite recovery policy derived from actual limits and two extra scheduler cadences", () => {
  expect(refreshRecoveryMs(limits)).toBe(110_000);
  const cycle = new StoredRefreshCycle(limits); cycle.observe(pending, 100);
  expect(cycle.deadline).toBe(110_100); expect(cycle.interval(100)).toBe(2_000);
});
it("unchanged pending and ETA movement never postpone expiry", () => {
  const cycle = new StoredRefreshCycle(limits); cycle.observe(pending, 0);
  cycle.observe({ ...pending, refresh_eta: "2026-10-31T01:00:00Z" }, 50_000);
  expect(cycle.deadline).toBe(110_000);
  expect(() => cycle.beforeRead(110_000)).toThrow(PageRefreshExpiredError);
  expect(cycle.interval(110_001)).toBe(false);
});
it("backs off transient errors at bounded cadence without moving the deadline", () => {
  const cycle = new StoredRefreshCycle(limits); cycle.observe(pending, 0);
  cycle.failure(); expect(cycle.interval(2_000)).toBe(4_000);
  cycle.failure(); expect(cycle.interval(6_000)).toBe(8_000);
  cycle.failure(); expect(cycle.interval(14_000)).toBe(8_000);
  expect(cycle.interval(109_000)).toBe(1_000);
  expect(cycle.deadline).toBe(110_000);
});
it("terminal refusal stops immediately; explicit retry starts a new bounded cycle", () => {
  const cycle = new StoredRefreshCycle(limits); cycle.observe(pending, 0); cycle.stop();
  expect(cycle.interval(2_000)).toBe(false);
  cycle.beforeRead(5_000); cycle.observe(pending, 5_000);
  expect(cycle.deadline).toBe(115_000); expect(cycle.interval(5_000)).toBe(2_000);
});
it("only verified forward progress can restart an ongoing cycle, and resolution stops it", () => {
  const cycle = new StoredRefreshCycle(limits); cycle.observe(pending, 0);
  cycle.observe({ ...pending, computed_at: "2026-10-31T00:00:01Z" }, 10_000);
  expect(cycle.deadline).toBe(120_000);
  cycle.observe({ ...pending, computed_at: "2026-10-31T00:00:00Z" }, 20_000);
  expect(cycle.deadline).toBe(120_000);
  cycle.observe({ ...pending, computed_at: "2026-10-31T00:00:02Z", refresh_eta: null, stale: false }, 30_000);
  expect(cycle.interval(30_000)).toBe(false);
});
it("honors a stored answer's ETA, uses a slower floor after ETA, and stops without an expiry error", () => {
  const now = Date.parse('2026-10-31T00:00:00Z');
  const cycle = new StoredRefreshCycle(limits);
  const stored = { ...pending, computed_at: '2026-10-30T23:59:00Z', refresh_eta: '2026-10-31T00:01:00Z' };
  cycle.observe(stored, now);
  expect(cycle.interval(now)).toBe(60_000);
  expect(cycle.interval(now + 60_000)).toBe(30_000);
  cycle.failure(); expect(cycle.interval(now + 60_000)).toBe(50_000); // bounded by recovery deadline
  expect(() => cycle.beforeRead(now + 110_000)).not.toThrow();
  cycle.observe(stored, now + 110_000);
  expect(cycle.interval(now + 110_000)).toBe(false);
});
it("a newly withheld revision loses the old computed certificate and returns to bounded short polling", () => {
  const cycle = new StoredRefreshCycle(limits);
  cycle.observe({ ...pending, computed_at: '2026-10-30T23:59:00Z' }, 0);
  cycle.observe(pending, 30_000);
  expect(cycle.computedAt).toBeNull();
  expect(cycle.interval(30_000)).toBe(2_000);
  expect(() => cycle.beforeRead(cycle.deadline!)).toThrow(PageRefreshExpiredError);
});
it("short action recovery backs off failures and keeps the same finite deadline despite a distant ETA", () => {
  const now = Date.parse('2026-10-31T00:00:00Z');
  const cycle = new StoredRefreshCycle(limits, true);
  const stored = { ...pending, computed_at: '2026-10-30T23:59:00Z', refresh_eta: '2026-10-31T01:00:00Z' };
  cycle.observe(stored, now);
  expect(cycle.interval(now)).toBe(2_000);
  cycle.failure(); expect(cycle.interval(now + 2_000)).toBe(4_000);
  cycle.observe(stored, now + 60_000);
  expect(cycle.deadline).toBe(now + 110_000);
  expect(() => cycle.beforeRead(now + 110_000)).not.toThrow();
  cycle.observe(stored, now + 110_000);
  expect(cycle.interval(now + 110_000)).toBe(false);
});
