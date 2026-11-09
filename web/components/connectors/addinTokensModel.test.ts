import { describe, expect, it } from "vitest";
import type { AddinToken } from "@/lib/api";
import {
  addinHeaderStatus,
  addinListErrorCopy,
  addinRevokeErrorCopy,
  addinSectionState,
  addinTokensAudience,
  formatDay,
  revokeFailureClearsRow,
  toTokenRows,
  withoutToken,
} from "./addinTokensModel";

/* The decisions behind the "Gmail Add-on" section on Connectors
 * (`GmailAddonTokens.tsx`). Each of these, if it drifted, would produce a
 * confident WRONG answer rather than a visible failure — see the model's own
 * module doc. */

const paired: AddinToken = {
  id: "11111111-1111-1111-1111-111111111111",
  name: "Gmail Add-on",
  created_at: "2026-09-03T15:00:00Z",
  last_used_at: "2026-09-10T18:30:00Z",
};

const neverUsed: AddinToken = {
  id: "22222222-2222-2222-2222-222222222222",
  name: "Gmail Add-on",
  created_at: "2026-09-05T15:00:00Z",
  last_used_at: null,
};

describe("addinTokensAudience — who sees the section at all", () => {
  it("shows it on the bare personal plane", () => {
    expect(
      addinTokensAudience({ loaded: true, orgId: null, orgRole: null, isPersonalOrg: false }),
    ).toBe("show");
  });

  /* THE ONE THAT MATTERS. A personal org (control migration 0208) sets
     `orgId` exactly like a real organization, but the backend's gate resolves
     it to `GateOutcome::Personal` — so `GET /v1/addin/tokens` answers
     normally there. Gating on `orgId != null` would hide the ONLY revoke
     surface from a user who can still mint tokens. */
  it("shows it in a PERSONAL org, whose orgId is set exactly like a real org's", () => {
    expect(
      addinTokensAudience({
        loaded: true,
        orgId: "org_personal_123",
        orgRole: "org:admin",
        isPersonalOrg: true,
      }),
    ).toBe("show");
  });

  it("hides it in a real organization, and does not merely disable it", () => {
    expect(
      addinTokensAudience({
        loaded: true,
        orgId: "org_acme",
        orgRole: "org:member",
        isPersonalOrg: false,
      }),
    ).toBe("org");
  });

  /* `wait` and `org` render the same nothing, but they are NOT the same
     fetch decision: `orgId` is `undefined` before Clerk resolves, which is
     indistinguishable from the personal plane, so an optimistic "show" here
     would fire a request that 400s for every org member on every load. */
  it("waits — never optimistically shows — before Clerk has resolved", () => {
    expect(
      addinTokensAudience({ loaded: false, orgId: undefined, orgRole: undefined, isPersonalOrg: false }),
    ).toBe("wait");
  });
});

describe("toTokenRows — what a row actually says", () => {
  it("renders name, paired date and last-used date", () => {
    const [row] = toTokenRows([paired]);
    expect(row).toEqual({
      id: paired.id,
      name: "Gmail Add-on",
      createdLabel: "9/3/2026",
      lastUsedLabel: "9/10/2026",
    });
  });

  /* `last_used_at: null` is a REAL state — approved, then never used — not a
     missing field. It must read as the word "never", never as a blank (which
     looks like a rendering bug) and never as a fallback to `created_at`
     (which would claim a use that never happened). */
  it("says 'never' for a token that has never been presented", () => {
    const [row] = toTokenRows([neverUsed]);
    expect(row.lastUsedLabel).toBe("never");
    expect(row.lastUsedLabel).not.toBe(row.createdLabel);
  });

  it("orders newest first, whatever order the server sent", () => {
    expect(toTokenRows([paired, neverUsed]).map((r) => r.id)).toEqual([neverUsed.id, paired.id]);
    expect(toTokenRows([neverUsed, paired]).map((r) => r.id)).toEqual([neverUsed.id, paired.id]);
  });

  it("renders an empty list as no rows at all (the empty state's whole input)", () => {
    expect(toTokenRows([])).toEqual([]);
  });

  it("says 'unknown' rather than 'Invalid Date' for an unparseable timestamp", () => {
    expect(formatDay("not-a-date")).toBe("unknown");
    const [row] = toTokenRows([{ ...paired, created_at: "not-a-date" }]);
    expect(row.createdLabel).toBe("unknown");
  });
});

describe("addinSectionState — loading / error / empty / loaded are four DIFFERENT answers", () => {
  /* Regression, CodeRabbit round 1 on #906. The header read `rows.length`
     directly, and a count of 0 is true in three unrelated situations — so
     the card announced "no paired add-ons" while it was still asking, and
     again directly above "couldn't load your tokens". On the expected-404
     path (the list endpoint ships in a sibling backend PR) that
     self-contradicting pair was the ONLY thing most people would ever see. */
  const loadingStates = [
    { name: "first load", inputs: { loading: true, loadError: null, rowCount: 0 } },
    // A RETRY: the previous error has been cleared and a request is in
    // flight again. Must not fall back to `empty`.
    { name: "retry in flight", inputs: { loading: true, loadError: null, rowCount: 0 } },
    // A refresh that still holds the previous rows on screen.
    { name: "refresh with rows still held", inputs: { loading: true, loadError: null, rowCount: 2 } },
  ];

  for (const { name, inputs } of loadingStates) {
    it(`is 'loading' — never 'empty' — while a request is in flight (${name})`, () => {
      expect(addinSectionState(inputs)).toBe("loading");
      expect(addinHeaderStatus(addinSectionState(inputs), inputs.rowCount)).not.toBe(
        "no paired add-ons",
      );
    });
  }

  it("is 'error' after a failed load, and says NOTHING in the header there", () => {
    const state = addinSectionState({
      loading: false,
      loadError: addinListErrorCopy(404),
      rowCount: 0,
    });
    expect(state).toBe("error");
    // `null`, not a count and not "no paired add-ons": the ErrorNote below it
    // already says what went wrong, and any status here would contradict it.
    expect(addinHeaderStatus(state, 0)).toBeNull();
  });

  it("in flight OUTRANKS a stale error — a running retry is not a failure", () => {
    expect(
      addinSectionState({ loading: true, loadError: "previous attempt failed", rowCount: 0 }),
    ).toBe("loading");
  });

  it("is 'empty' ONLY after a load that actually succeeded with zero tokens", () => {
    const state = addinSectionState({ loading: false, loadError: null, rowCount: 0 });
    expect(state).toBe("empty");
    expect(addinHeaderStatus(state, 0)).toBe("no paired add-ons");
  });

  it("is 'loaded' with a count once there are rows", () => {
    const state = addinSectionState({ loading: false, loadError: null, rowCount: 2 });
    expect(state).toBe("loaded");
    expect(addinHeaderStatus(state, 2)).toBe("2 paired");
    expect(addinHeaderStatus(addinSectionState({ loading: false, loadError: null, rowCount: 1 }), 1)).toBe(
      "1 paired",
    );
  });

  it("gives every state its own header line, so no two states read alike", () => {
    const lines = [
      addinHeaderStatus("loading", 0),
      addinHeaderStatus("error", 0),
      addinHeaderStatus("empty", 0),
      addinHeaderStatus("loaded", 3),
    ];
    expect(new Set(lines).size).toBe(lines.length);
    expect(lines[0]).toBe("checking…");
  });
});

describe("withoutToken — revoke removes exactly one row", () => {
  it("drops the revoked token and leaves the others", () => {
    expect(withoutToken([paired, neverUsed], paired.id)).toEqual([neverUsed]);
  });

  it("is a no-op for an id that isn't in the list", () => {
    expect(withoutToken([paired], "33333333-3333-3333-3333-333333333333")).toEqual([paired]);
  });
});

describe("failure copy", () => {
  /* Until the sibling backend PR deploys `GET /v1/addin/tokens`, production
     answers 404. The copy must say THAT, because the alternative reading —
     an empty list — is a lie that talks someone out of revoking a live
     token. */
  it("names the not-yet-deployed case on a 404 and never implies zero tokens", () => {
    const copy = addinListErrorCopy(404);
    expect(copy).toContain("isn't available on this backend yet");
    expect(copy.toLowerCase()).not.toContain("no tokens");
  });

  it("explains the plane on a 400 instead of showing a raw backend string", () => {
    expect(addinListErrorCopy(400)).toContain("personal workspace");
  });

  it("still says something on a transport failure with no status", () => {
    expect(addinListErrorCopy(null)).toBe("Couldn't load your Gmail Add-on tokens.");
  });

  /* A DELETE's 404 is NOT the list's 404: `revoke_token` answers 404 for "no
     such id" / "already revoked" / "someone else's" alike, and from this
     screen the live one is "already revoked". */
  it("reads a revoke 404 as already-revoked, and clears that row", () => {
    expect(addinRevokeErrorCopy(404)).toBe("That token was already revoked.");
    expect(revokeFailureClearsRow(404)).toBe(true);
  });

  it("keeps a row visible when the revoke failed for any other reason", () => {
    expect(revokeFailureClearsRow(503)).toBe(false);
    expect(revokeFailureClearsRow(null)).toBe(false);
    expect(addinRevokeErrorCopy(503)).toContain("try again");
  });
});
