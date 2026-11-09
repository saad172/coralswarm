import { describe, expect, it } from "vitest";
import { createRequestGuard } from "./requestGuard";

/* The claim under test is not "a guard exists". It is "the OLDER response
 * loses, even though it arrives last" — the only thing that matters when a
 * user switches oceans mid-fetch.
 *
 * Every test below drives the two requests to resolve OUT OF ORDER on
 * purpose (request A starts first and settles second), because that is the
 * ordering the bug needs and the one a naive `await` in sequence can never
 * produce. The unguarded control runs the identical script and is asserted to
 * LOSE — a guard test that has only ever seen guarded input proves nothing,
 * since it is indistinguishable from one asserting a constant.
 *
 * No `process.env` and no `NEXT_PUBLIC_*`: vitest in this repo has neither,
 * so a module-scope env read turns "expected null" into a vacuous pass. */

/** A promise plus the handle to settle it — lets a test choose the
 *  resolution ORDER independently of the call order. */
function deferred<T>(): { promise: Promise<T>; resolve: (v: T) => void } {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe("createRequestGuard", () => {
  it("drops a stale response that lands after a newer one", async () => {
    const guard = createRequestGuard();
    let sessions = "nothing yet";
    let loading = true;

    // Request A — ocean A. Starts first, settles LAST.
    const isCurrentA = guard.begin();
    const a = deferred<string>();
    const inFlightA = a.promise.then((rows) => {
      if (!isCurrentA()) return;
      sessions = rows;
      loading = false;
    });

    // The user switches to ocean B while A is still in flight.
    const isCurrentB = guard.begin();
    const b = deferred<string>();
    const inFlightB = b.promise.then((rows) => {
      if (!isCurrentB()) return;
      sessions = rows;
      loading = false;
    });

    b.resolve("ocean B rows");
    await inFlightB;
    expect(sessions).toBe("ocean B rows");
    expect(loading).toBe(false);

    a.resolve("ocean A rows");
    await inFlightA;
    expect(sessions).toBe("ocean B rows"); // the stale response must not win
    expect(loading).toBe(false); // nor may it un-finish the newer one
  });

  it("NEGATIVE CONTROL: without the guard the same script lets the stale response win", async () => {
    let sessions = "nothing yet";

    const a = deferred<string>();
    const inFlightA = a.promise.then((rows) => {
      sessions = rows;
    });
    const b = deferred<string>();
    const inFlightB = b.promise.then((rows) => {
      sessions = rows;
    });

    b.resolve("ocean B rows");
    await inFlightB;
    expect(sessions).toBe("ocean B rows");

    a.resolve("ocean A rows");
    await inFlightA;
    // This is the shipped bug, reproduced: the ocean the user left is now on
    // screen under the ocean they switched to.
    expect(sessions).toBe("ocean A rows");
  });

  it("keeps a lone in-flight request current — the guard is not a no-op that drops everything", async () => {
    const guard = createRequestGuard();
    const isCurrent = guard.begin();
    const only = deferred<string>();
    let sessions = "nothing yet";
    const inFlight = only.promise.then((rows) => {
      if (!isCurrent()) return;
      sessions = rows;
    });
    only.resolve("the only rows");
    await inFlight;
    expect(sessions).toBe("the only rows");
  });

  it("supersedes across load and loadMore, which share one guard", async () => {
    // `loadMore` APPENDS. If an ocean switch during a `loadMore` did not
    // invalidate it, the previous ocean's page 2 would be appended to the new
    // ocean's page 1 — a single list holding two oceans' rows.
    const guard = createRequestGuard();
    let sessions: string[] = ["A#1"];

    const isCurrentMore = guard.begin(); // loadMore, ocean A page 2
    const more = deferred<string[]>();
    const inFlightMore = more.promise.then((rows) => {
      if (!isCurrentMore()) return;
      sessions = [...sessions, ...rows];
    });

    const isCurrentLoad = guard.begin(); // ocean switch -> load, ocean B page 1
    const load = deferred<string[]>();
    const inFlightLoad = load.promise.then((rows) => {
      if (!isCurrentLoad()) return;
      sessions = rows;
    });

    load.resolve(["B#1"]);
    await inFlightLoad;
    more.resolve(["A#2"]);
    await inFlightMore;

    expect(sessions).toEqual(["B#1"]);
  });

  it("gives each guard its own generation — one screen's request cannot invalidate another's", async () => {
    const first = createRequestGuard();
    const second = createRequestGuard();
    const isCurrentFirst = first.begin();
    second.begin();
    second.begin();
    expect(isCurrentFirst()).toBe(true);
  });
});

/* WHERE THE LIBRARY LIST PAGES ARE BOUND NOW.
 *
 * This file used to source-read `app/(app)/library/{sessions,
 * agent-sessions}/page.tsx` and assert each held a `createRequestGuard` around
 * its hand-appended "Load more". #1227 (aee6466b) and #1233 (785294e6) rewrote
 * those pages into thin `<Suspense>` wrappers around
 * `components/library/CapturedSessionCollection.tsx`, which keeps no list
 * state of its own: every read goes through `lib/useLibraryRead.ts`, keyed on
 * the admission key, graph and URL (pagination cursors live in the URL, so
 * there is no append path left). `useLibraryRead` aborts the superseded
 * request on a key change and only publishes a result whose generation is
 * still current — `components/library/useLibraryRead.test.tsx` proves that in
 * isolation, and `components/library/CapturedSessionCollection.test.tsx`
 * ("never shows a previous ocean's rows ...") drives each of the three shipped
 * pages through the same out-of-order ocean switch this file scripts above and
 * asserts the older response loses. A source grep for `createRequestGuard` in
 * those pages would now only assert the dead mechanism, not the behaviour.
 *
 * `createRequestGuard` itself is still what `app/(app)/session/page.tsx` and
 * `app/(app)/capture/page.tsx` run, which is why the tests above stay. */
