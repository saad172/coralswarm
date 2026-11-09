import { describe, expect, it } from "vitest";
import {
  escrowCheckKey,
  vaultDatabaseName,
  vaultIdentityKey,
  type VaultIdentity,
} from "./vaultNamespace";

const USER_A = "user_2aaaaaaaaaaaaaaaaaaaaaaaaa";
const USER_B = "user_2bbbbbbbbbbbbbbbbbbbbbbbbb";
const ORG_X = "org_2xxxxxxxxxxxxxxxxxxxxxxxxx";
const ORG_Y = "org_2yyyyyyyyyyyyyyyyyyyyyyyyy";

/** A loaded, signed-in Clerk session. `orgId: null` is Clerk's spelling of
 *  "personal instance", so it is a real identity, not a missing one. */
const signedIn = (userId: string, orgId: string | null): VaultIdentity => ({
  isLoaded: true,
  userId,
  orgId,
});

/** The name the store used to have for everybody, before this module existed. */
const LEGACY_SHARED_NAME = "coralswarm";

describe("vaultDatabaseName — tenant isolation", () => {
  it("two organizations never share a database name", () => {
    // THE PR. One name is one IndexedDB database, and that database holds the
    // private-ocean master key plus the corals (whose embeddings are stored
    // unencrypted). If this ever collapses to a single name, org Y reads org
    // X's key and corpus out of the same browser profile.
    const x = vaultDatabaseName(signedIn(USER_A, ORG_X));
    const y = vaultDatabaseName(signedIn(USER_A, ORG_Y));
    expect(x).not.toBe(y);
  });

  it("two users never share a database name, even inside one organization", () => {
    // Shared-device case: two people signing into the same org from one
    // browser profile. Each has their OWN private-ocean key; the org is not
    // the unit of custody, the member is.
    const a = vaultDatabaseName(signedIn(USER_A, ORG_X));
    const b = vaultDatabaseName(signedIn(USER_B, ORG_X));
    expect(a).not.toBe(b);
  });

  it("a user's personal instance is a different store from any org they are in", () => {
    const personal = vaultDatabaseName(signedIn(USER_A, null));
    expect(personal).not.toBe(vaultDatabaseName(signedIn(USER_A, ORG_X)));
    expect(personal).not.toBe(vaultDatabaseName(signedIn(USER_A, ORG_Y)));
  });

  it("the personal instance is ONE store, whether Clerk says null or undefined", () => {
    // The other direction of the same knob, and the reason it is not enough to
    // say "distinct inputs, distinct names". `null` and `undefined` both mean
    // "no active org" once loaded; splitting them would strand a user's
    // private ocean in whichever store Clerk happened to report last, which
    // reads to the user as data loss.
    const withNull = vaultDatabaseName({ isLoaded: true, userId: USER_A, orgId: null });
    const withUndefined = vaultDatabaseName({ isLoaded: true, userId: USER_A, orgId: undefined });
    expect(withNull).toBe(withUndefined);
    expect(withNull).not.toBeNull();
  });

  it("is a pure function of identity — the same identity names the same store forever", () => {
    // A store name that drifted between calls (a counter, a timestamp, a
    // random id) would isolate tenants perfectly and lose the user's ocean on
    // every page load. Isolation is only half the requirement; stability is
    // the other half.
    const first = vaultDatabaseName(signedIn(USER_A, ORG_X));
    const second = vaultDatabaseName(signedIn(USER_A, ORG_X));
    const third = vaultDatabaseName({ isLoaded: true, userId: USER_A, orgId: ORG_X });
    expect(second).toBe(first);
    expect(third).toBe(first);
  });

  it("an id containing the separator cannot forge another identity's name", () => {
    // A REAL collision, not a near miss: with the escaping removed, both of
    // these map to the identical string `u:a:o:b:o:c`, so user `a:o:b` in org
    // `c` would open user `a`-in-org-`b:o:c`'s store.
    //
    // Two things about this fixture were established by negative control, not
    // by inspection, and both are easy to get wrong:
    //
    //   * the obvious-looking pair — `a:o:b` on the personal instance vs `a`
    //     in org `b` — does NOT collide even unescaped, because the personal
    //     form still appends a trailing `:o:`. A test built on it stays green
    //     with the escaping deleted and proves nothing. That was the first
    //     version of this test.
    //   * escaping the USER is what carries injectivity: the org is the last
    //     segment, so everything before it is already determined. Escaping the
    //     org too is for reversibility and for the day the format grows a
    //     third segment, not for this. Accordingly this pair only collides
    //     when BOTH escapes are removed.
    //
    // Clerk ids do not contain `:` today, which is precisely why this would go
    // unnoticed until an id format changed or a non-Clerk identity source
    // appeared.
    const forger = vaultDatabaseName(signedIn("a:o:b", "c"));
    const victim = vaultDatabaseName(signedIn("a", "b:o:c"));
    expect(forger).not.toBe(victim);
  });

  it("no identity can ever produce the legacy shared name", () => {
    // There is no migration from the old single `"coralswarm"` database (the
    // engine has never run in production, so no such database exists in a real
    // user's browser). This is what makes that safe: any leftover one is
    // unreachable rather than silently adopted by the first user to sign in.
    for (const id of [
      signedIn(USER_A, null),
      signedIn(USER_A, ORG_X),
      signedIn(USER_B, ORG_Y),
      signedIn(LEGACY_SHARED_NAME, null),
      signedIn(LEGACY_SHARED_NAME, LEGACY_SHARED_NAME),
    ]) {
      expect(vaultDatabaseName(id)).not.toBe(LEGACY_SHARED_NAME);
    }
  });

  it("distinct identities produce distinct names, across the whole matrix", () => {
    // Injectivity stated once over a cross-product, so a future encoding
    // change has to break here rather than in one hand-picked pair. The
    // fixture list is checked for distinctness FIRST — otherwise this would be
    // comparing a set against a list that could itself contain duplicates, and
    // would pass by being vacuous.
    const identities: VaultIdentity[] = [
      signedIn(USER_A, null),
      signedIn(USER_A, ORG_X),
      signedIn(USER_A, ORG_Y),
      signedIn(USER_B, null),
      signedIn(USER_B, ORG_X),
      signedIn(USER_B, ORG_Y),
      // The colliding pair from the forgery test above: distinct identities
      // that the fully unescaped encoding maps to one name.
      signedIn("a:o:b", "c"),
      signedIn("a", "b:o:c"),
    ];
    expect(new Set(identities.map((i) => JSON.stringify(i))).size).toBe(identities.length);

    const names = identities.map((i) => vaultDatabaseName(i));
    expect(names.every((n) => n !== null)).toBe(true);
    expect(new Set(names).size).toBe(identities.length);
  });
});

describe("vaultDatabaseName — an unknown identity gets no store", () => {
  it("names nothing before Clerk has loaded", () => {
    // Not a default, not a placeholder: null, meaning the caller opens no
    // database at all. Anything else would be a real store that a later,
    // known identity could also be routed into.
    expect(vaultDatabaseName({ isLoaded: false, userId: undefined, orgId: undefined })).toBeNull();
  });

  it("names nothing when nobody is signed in", () => {
    expect(vaultDatabaseName({ isLoaded: true, userId: null, orgId: null })).toBeNull();
    expect(vaultDatabaseName({ isLoaded: true, userId: undefined, orgId: ORG_X })).toBeNull();
  });

  it("ignores a user id that arrives before Clerk claims to be loaded", () => {
    // Defensive: Clerk reports `undefined` for everything until `isLoaded`, so
    // this shape should not occur. If it ever did, the values are not yet
    // authoritative and must not name a store.
    expect(vaultDatabaseName({ isLoaded: false, userId: USER_A, orgId: ORG_X })).toBeNull();
  });

  it("no unknown-identity shape produces a name, and no known one is left unnamed", () => {
    // Both directions in one place, because either alone is satisfiable by a
    // useless implementation: "always null" isolates perfectly and never opens
    // a store, "never null" opens one under an identity nobody knows.
    //
    // An earlier version of this test compared real names against `null` and
    // was a tautology — it stayed GREEN under a mutation that handed
    // signed-out users a shared fallback name, which is the exact defect it
    // was written to catch.
    const unknown: VaultIdentity[] = [
      { isLoaded: false, userId: undefined, orgId: undefined },
      { isLoaded: false, userId: USER_A, orgId: ORG_X },
      { isLoaded: true, userId: null, orgId: null },
      { isLoaded: true, userId: undefined, orgId: ORG_X },
      { isLoaded: true, userId: "", orgId: ORG_X },
    ];
    for (const id of unknown) expect(vaultDatabaseName(id)).toBeNull();
    for (const id of [signedIn(USER_A, null), signedIn(USER_A, ORG_X), signedIn(USER_B, ORG_Y)]) {
      expect(vaultDatabaseName(id)).not.toBeNull();
    }
  });
});

describe("escrowCheckKey — the same identity, on sessionStorage", () => {
  it("two organizations never share the escrow latch", () => {
    // The bug being folded in: the latch used to be the bare constant
    // `cs_escrow_checked`. Switching org inside one session found it already
    // set and skipped the re-enrollment entirely, so the incoming org's
    // recovery never learned about this member's key — an admin recovery
    // everyone believed was configured would come up empty.
    expect(escrowCheckKey(signedIn(USER_A, ORG_X))).not.toBe(
      escrowCheckKey(signedIn(USER_A, ORG_Y)),
    );
  });

  it("two users never share the escrow latch", () => {
    expect(escrowCheckKey(signedIn(USER_A, ORG_X))).not.toBe(
      escrowCheckKey(signedIn(USER_B, ORG_X)),
    );
  });

  it("is stable for one identity, so the latch still latches", () => {
    // The point of a latch is to fire once per session per identity. A key
    // that varied per call would re-enroll on every mount — a PUT storm, not
    // a correctness bug, but the reason the constant existed at all.
    expect(escrowCheckKey(signedIn(USER_A, ORG_X))).toBe(escrowCheckKey(signedIn(USER_A, ORG_X)));
  });

  it("names nothing when there is no identity", () => {
    expect(escrowCheckKey({ isLoaded: false, userId: undefined, orgId: undefined })).toBeNull();
    expect(escrowCheckKey({ isLoaded: true, userId: null, orgId: null })).toBeNull();
  });

  it("agrees with the database name about WHEN an identity is known", () => {
    // `provider.tsx` mounts the engine only when both are non-null and treats
    // that as one decision. If they could disagree, the engine would either
    // open a store with no latch or latch with no store.
    for (const id of [
      signedIn(USER_A, ORG_X),
      signedIn(USER_A, null),
      { isLoaded: false, userId: undefined, orgId: undefined },
      { isLoaded: true, userId: null, orgId: null },
      { isLoaded: false, userId: USER_A, orgId: ORG_X },
    ] satisfies VaultIdentity[]) {
      expect(escrowCheckKey(id) === null).toBe(vaultDatabaseName(id) === null);
    }
  });

  it("is not the same string as the database name for the same identity", () => {
    // Different stores (sessionStorage vs IndexedDB), so a collision is
    // harmless — but equal strings would mean one of the two prefixes had been
    // dropped, and the next thing dropped is the identity segment.
    const id = signedIn(USER_A, ORG_X);
    expect(escrowCheckKey(id)).not.toBe(vaultDatabaseName(id));
  });
});

describe("vaultIdentityKey — the segment both names are built from", () => {
  it("carries both halves of the identity", () => {
    // Named explicitly so a change that drops one half fails here with a
    // readable diff, not only as an inequality three tests away.
    expect(vaultIdentityKey(signedIn(USER_A, ORG_X))).toBe(`u:${USER_A}:o:${ORG_X}`);
    expect(vaultIdentityKey(signedIn(USER_A, null))).toBe(`u:${USER_A}:o:`);
  });

  it("escapes the separator out of the components", () => {
    expect(vaultIdentityKey(signedIn("a:b", "c:d"))).toBe("u:a%3Ab:o:c%3Ad");
  });
});
