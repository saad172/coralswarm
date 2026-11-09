import { describe, expect, it } from "vitest";
import { pickActiveOcean, shouldReportActiveOcean, type OceanRef } from "./activeOcean";

const oceans = (...ids: string[]): OceanRef[] => ids.map((id) => ({ id }));

const A = "aaaaaaaa-0000-0000-0000-000000000001";
const B = "bbbbbbbb-0000-0000-0000-000000000002";
const C = "cccccccc-0000-0000-0000-000000000003";
/** Reachable by nobody in these fixtures — a deleted ocean, a revoked one, or
 *  one belonging to a different account entirely. */
const GONE = "99999999-0000-0000-0000-000000000009";

describe("pickActiveOcean — precedence", () => {
  it("the server's pointer wins over the browser's when both are reachable", () => {
    // The whole point of the PR. If this inverts, an org switch on a device
    // that has ever visited another org opens the wrong ocean.
    expect(pickActiveOcean(oceans(A, B, C), B, C)).toEqual({ id: B, source: "server" });
  });

  it("the browser's value is used only when the server has none", () => {
    for (const none of [null, undefined, ""] as const) {
      expect(pickActiveOcean(oceans(A, B, C), none, C)).toEqual({ id: C, source: "local" });
    }
  });

  it("falls back to the first reachable ocean when neither preference is usable", () => {
    expect(pickActiveOcean(oceans(A, B), null, null)).toEqual({ id: A, source: "first" });
  });

  it("a server pointer that names an ocean this user cannot open falls through", () => {
    // Deleted, archived, or access revoked since it was stored. The pointer is
    // advisory (there is no FK behind it); the reachable list is the authority.
    expect(pickActiveOcean(oceans(A, B), GONE, B)).toEqual({ id: B, source: "local" });
    expect(pickActiveOcean(oceans(A, B), GONE, null)).toEqual({ id: A, source: "first" });
  });

  it("an unreachable pointer NEVER selects itself, on either channel", () => {
    // The negative form of the property above, stated separately because it is
    // the security-relevant half: an id that is not in the reachable list must
    // not be openable by having been stored anywhere.
    const picked = pickActiveOcean(oceans(A), GONE, GONE);
    expect(picked.id).not.toBe(GONE);
    expect(picked).toEqual({ id: A, source: "first" });
  });

  it("org A's leftover browser value cannot open anything in org B", () => {
    // The concrete shape of today's defect: `cs_graph_id` is one global key and
    // `resetOcean` never clears it, so org A's id really is present during org
    // B's boot. Nothing but this lookup miss stops it.
    const orgAOcean = GONE;
    const orgBOceans = oceans(A, B);
    expect(pickActiveOcean(orgBOceans, null, orgAOcean)).toEqual({ id: A, source: "first" });
  });

  it("refuses to invent an ocean when there are none", () => {
    // `oceanBootstrap.decideOceanBootstrap` owns the empty case (create, or
    // throw `UnexpectedEmptyOrgError`); reaching here with an empty list is a
    // caller bug and should be loud, not a silent `undefined.id` crash later.
    expect(() => pickActiveOcean([], A, A)).toThrow(/no reachable oceans/);
  });
});

describe("shouldReportActiveOcean", () => {
  it("says nothing when the server already stores exactly this ocean", () => {
    expect(shouldReportActiveOcean(A, A)).toBe(false);
  });

  it("reports when the server has no pointer at all", () => {
    // This is how the table gets populated for every user who existed before
    // migration 0201 — from whatever their browser, or the first-reachable
    // fallback, produced on their next boot.
    expect(shouldReportActiveOcean(A, null)).toBe(true);
    expect(shouldReportActiveOcean(A, undefined)).toBe(true);
  });

  it("reports when the fallback overrode an unreachable server pointer", () => {
    // Without this the stale pointer would survive every future boot on every
    // device, and `pickActiveOcean` would keep discarding it forever.
    expect(shouldReportActiveOcean(A, GONE)).toBe(true);
  });

  it("reports a genuine switch", () => {
    expect(shouldReportActiveOcean(B, A)).toBe(true);
  });
});

describe("the two functions agree on the case that populates the server", () => {
  it("a first-ever boot picks the first ocean AND reports it", () => {
    // Checked together because either half alone is silently useless: a pick
    // that is never reported never leaves the device, and a report of a pick
    // nobody made is meaningless.
    const list = oceans(A, B);
    const picked = pickActiveOcean(list, null, null);
    expect(picked).toEqual({ id: A, source: "first" });
    expect(shouldReportActiveOcean(picked.id, null)).toBe(true);
  });

  it("a steady-state boot picks the server's ocean AND sends no request", () => {
    const list = oceans(A, B);
    const picked = pickActiveOcean(list, B, A);
    expect(picked).toEqual({ id: B, source: "server" });
    expect(shouldReportActiveOcean(picked.id, B)).toBe(false);
  });
});
