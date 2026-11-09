import { describe, expect, it } from "vitest";
import { engineSessionFor, tokenIdentityKey } from "./engineSession";
import { vaultDatabaseName, vaultIdentityKey, type VaultIdentity } from "./vaultNamespace";

/* THE SUBJECT IS THE PAIRING, NOT EITHER HALF.
 *
 * `vaultNamespace.test.ts` already proves the store name follows the identity.
 * What it cannot see is the other input: the BEARER. On an org switch Clerk's
 * `orgId` moves synchronously and `useApp()`'s token does not (it is React
 * state written after an `await getToken()`, and for a newly-active org that
 * is a real round trip to Clerk), so there is a committed render where the
 * store is org B's and the token is org A's — and `EngineSession`'s mount
 * effect fires inside it. Tenancy rides the token's `org_id` claim alone, so
 * every request that session makes in that window runs against org A: it wraps
 * org B's on-device key to ORG A's recovery public key and files it in org A's
 * escrow, where A's admins recover it M-of-N.
 *
 * Accordingly the assertions below are never "a session was produced" — a
 * session was produced on the broken version too, just with the wrong bearer.
 * They are always about WHICH TOKEN a session for a given store is holding. */

const USER = "user_2aaaaaaaaaaaaaaaaaaaaaaaaa";
const OTHER_USER = "user_2bbbbbbbbbbbbbbbbbbbbbbbbb";
const ORG_A = "org_2aaaaaaaaaaaaaaaaaaaaaaaaa";
const ORG_B = "org_2bbbbbbbbbbbbbbbbbbbbbbbbb";

const signedIn = (userId: string, orgId: string | null): VaultIdentity => ({
  isLoaded: true,
  userId,
  orgId,
});

/** base64url, the way a JWT actually encodes a segment (no padding, `-`/`_`). */
function b64url(s: string): string {
  return Buffer.from(s, "utf8").toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** A Clerk **v1** session JWT: tenancy in the top-level `org_id` claim.
 *  Signature bytes are junk on purpose — nothing client-side verifies them,
 *  and a test that used a real signature would imply otherwise. */
function tokenV1(sub: string, orgId: string | null): string {
  const claims: Record<string, unknown> = { sub, exp: 9999999999, sid: "sess_1" };
  if (orgId !== null) {
    claims.org_id = orgId;
    claims.org_role = "org:member";
    claims.org_slug = "acme";
  }
  return `${b64url(JSON.stringify({ alg: "RS256", typ: "JWT" }))}.${b64url(JSON.stringify(claims))}.sig`;
}

/** A Clerk **v2** session JWT: tenancy in the nested `o` claim
 *  (`{ id, slg, rol }`). See `backend/src/clerk.rs` — the backend's own
 *  v1-only decoder once served every one of these as a personal-plane
 *  request, which is the same fail-open this module must not repeat. */
function tokenV2(sub: string, orgId: string): string {
  const claims = {
    sub,
    exp: 9999999999,
    sid: "sess_1",
    o: { id: orgId, slg: "acme", rol: "member" },
  };
  return `${b64url(JSON.stringify({ alg: "RS256", typ: "JWT" }))}.${b64url(JSON.stringify(claims))}.sig`;
}

// ── The finding's own scenario ───────────────────────────────────────────────

describe("an A → B org switch with B's token still in flight", () => {
  /** The render sequence React actually goes through on a switch, in order.
   *
   *  Step 2 IS THE VULNERABILITY: Clerk has already published org B on
   *  `useAuth()` (so `EngineProvider` recomputes `dbName` and React mounts a
   *  BRAND-NEW `EngineSession` under the new `key`), while `useApp()` is still
   *  handing down the token minted for org A. `getToken()` cannot shorten this
   *  window — the cached JWT carries A's `org_id`, so B's token is a network
   *  round trip away. */
  const switchAtoB: Array<{ label: string; identity: VaultIdentity; token: string }> = [
    { label: "settled on org A", identity: signedIn(USER, ORG_A), token: tokenV1(USER, ORG_A) },
    { label: "orgId flipped to B, token still A's", identity: signedIn(USER, ORG_B), token: tokenV1(USER, ORG_A) },
    { label: "B's token landed", identity: signedIn(USER, ORG_B), token: tokenV1(USER, ORG_B) },
  ];

  it("hands out NO session while the stale token is the only one available", () => {
    const [, duringSwitch] = switchAtoB;
    // Not "a session for org B with A's token" and not "org A's old session
    // kept alive" — no session. `provider.tsx` renders the null context, the
    // same one it renders before sign-in, and the engine reappears the moment
    // the right bearer arrives.
    expect(engineSessionFor(duringSwitch.identity, duringSwitch.token)).toBeNull();
  });

  it("never lets org B's store be paired with org A's token, at any point in the switch", () => {
    // THE REGRESSION TEST. Stated over the whole sequence rather than the one
    // interesting step, so a future change that moves the window somewhere
    // else in the sequence still fails here.
    const aToken = tokenV1(USER, ORG_A);
    const bStore = vaultDatabaseName(signedIn(USER, ORG_B));

    for (const step of switchAtoB) {
      const session = engineSessionFor(step.identity, step.token);
      if (session === null) continue;
      // Whatever store this session opens, its bearer names that same
      // identity — which is the property, spelled as the code spells it.
      expect(tokenIdentityKey(session.token)).toBe(vaultIdentityKey(step.identity));
      // And the specific pairing the finding names, spelled out so a failure
      // reads as the vulnerability rather than as an abstract mismatch.
      expect([session.dbName, session.token]).not.toEqual([bStore, aToken]);
    }
  });

  it("produces org B's session the instant B's token lands, and not before", () => {
    // The other half: refusing forever would satisfy every assertion above and
    // would simply have deleted the feature. The sequence must both refuse
    // during the window AND recover from it.
    const outcomes = switchAtoB.map((s) => engineSessionFor(s.identity, s.token));
    expect(outcomes.map((o) => o !== null)).toEqual([true, false, true]);
    expect(outcomes[0]?.dbName).toBe(vaultDatabaseName(signedIn(USER, ORG_A)));
    expect(outcomes[2]?.dbName).toBe(vaultDatabaseName(signedIn(USER, ORG_B)));
    expect(outcomes[2]?.token).toBe(tokenV1(USER, ORG_B));
  });

  it("refuses the mirror case too — switching org → personal with the org token held", () => {
    // The reverse direction is the one that fails OPEN if the org claim is
    // misread, because "no org" is itself a real identity: an org token whose
    // claim we failed to see would be accepted for the personal store.
    expect(engineSessionFor(signedIn(USER, null), tokenV1(USER, ORG_A))).toBeNull();
    expect(engineSessionFor(signedIn(USER, null), tokenV2(USER, ORG_A))).toBeNull();
  });
});

// ── The binding itself ───────────────────────────────────────────────────────

describe("engineSessionFor — a session's store and its bearer name one identity", () => {
  it("accepts a token minted for exactly this identity", () => {
    const org = engineSessionFor(signedIn(USER, ORG_A), tokenV1(USER, ORG_A));
    expect(org).not.toBeNull();
    expect(org?.dbName).toBe(vaultDatabaseName(signedIn(USER, ORG_A)));

    const personal = engineSessionFor(signedIn(USER, null), tokenV1(USER, null));
    expect(personal).not.toBeNull();
    expect(personal?.dbName).toBe(vaultDatabaseName(signedIn(USER, null)));
  });

  it("accepts a v2 token for its own organization", () => {
    // Not only "v2 org tokens are refused for the personal instance" — they
    // must also still WORK for the org they name, or recognizing the shape
    // would just be a more elaborate way of switching the engine off.
    const s = engineSessionFor(signedIn(USER, ORG_A), tokenV2(USER, ORG_A));
    expect(s).not.toBeNull();
    expect(s?.dbName).toBe(vaultDatabaseName(signedIn(USER, ORG_A)));
  });

  it("refuses a token for a different organization", () => {
    expect(engineSessionFor(signedIn(USER, ORG_B), tokenV1(USER, ORG_A))).toBeNull();
    expect(engineSessionFor(signedIn(USER, ORG_A), tokenV2(USER, ORG_B))).toBeNull();
  });

  it("refuses a token for a different user, even in the right organization", () => {
    // Shared-device case. The org matching is not enough: the private-ocean
    // key is the MEMBER's, and the escrow row is `(account_id, user_id)`.
    expect(engineSessionFor(signedIn(USER, ORG_A), tokenV1(OTHER_USER, ORG_A))).toBeNull();
    expect(engineSessionFor(signedIn(USER, null), tokenV1(OTHER_USER, null))).toBeNull();
  });

  it("refuses every unknown-identity shape, whatever the token says", () => {
    // Mirrors `vaultNamespace.test.ts`'s unknown-identity set: a valid token
    // does not conjure an identity Clerk has not resolved.
    const unknown: VaultIdentity[] = [
      { isLoaded: false, userId: undefined, orgId: undefined },
      { isLoaded: false, userId: USER, orgId: ORG_A },
      { isLoaded: true, userId: null, orgId: null },
      { isLoaded: true, userId: undefined, orgId: ORG_A },
    ];
    for (const id of unknown) expect(engineSessionFor(id, tokenV1(USER, ORG_A))).toBeNull();
  });

  it("refuses anything that is not a readable bearer", () => {
    const id = signedIn(USER, ORG_A);
    for (const bad of [
      null,
      undefined,
      "",
      "not-a-jwt",
      "only.two",
      "a.b.c.d",
      // Well-formed JWT structure, unreadable payload.
      "aGVhZGVy.!!!!.sig",
      // Valid base64url JSON, but not an object.
      `aGVhZGVy.${b64url('"hello"')}.sig`,
      `aGVhZGVy.${b64url("[1,2,3]")}.sig`,
      // An object with no usable `sub`.
      `aGVhZGVy.${b64url(JSON.stringify({ org_id: ORG_A }))}.sig`,
      `aGVhZGVy.${b64url(JSON.stringify({ sub: "", org_id: ORG_A }))}.sig`,
      `aGVhZGVy.${b64url(JSON.stringify({ sub: 42, org_id: ORG_A }))}.sig`,
    ]) {
      expect(engineSessionFor(id, bad)).toBeNull();
    }
  });

  it("returns a spec whose three fields agree with each other", () => {
    // The type says the fields exist; this says they are about the same
    // person. A spec whose `latchKey` named a different identity than its
    // `dbName` would latch one org's escrow re-check against another's store.
    for (const id of [signedIn(USER, ORG_A), signedIn(USER, null), signedIn(OTHER_USER, ORG_B)]) {
      const token = tokenV1(id.userId as string, (id.orgId ?? null) as string | null);
      const s = engineSessionFor(id, token);
      expect(s).not.toBeNull();
      expect(s?.dbName).toBe(vaultDatabaseName(id));
      expect(s?.latchKey).toContain(vaultIdentityKey(id));
      expect(tokenIdentityKey(s?.token)).toBe(vaultIdentityKey(id));
    }
  });
});

// ── The claim read ───────────────────────────────────────────────────────────

describe("tokenIdentityKey — which identity a bearer actually authorizes", () => {
  it("reads the v1 top-level org_id claim", () => {
    expect(tokenIdentityKey(tokenV1(USER, ORG_A))).toBe(vaultIdentityKey(signedIn(USER, ORG_A)));
  });

  it("reads the v2 nested o.id claim", () => {
    // The specific fail-open this exists to prevent: with `o` unread, this
    // returns the PERSONAL key and an org token is accepted for the personal
    // store. The backend shipped exactly that bug once (clerk.rs).
    expect(tokenIdentityKey(tokenV2(USER, ORG_A))).toBe(vaultIdentityKey(signedIn(USER, ORG_A)));
    expect(tokenIdentityKey(tokenV2(USER, ORG_A))).not.toBe(vaultIdentityKey(signedIn(USER, null)));
  });

  it("gives v1 precedence over v2, as a group, exactly like the backend", () => {
    // `normalize_v2_org_claim` consults `o` only when the top-level `org_id`
    // is absent. A client that resolved this the other way round would decide
    // whose store to open from a different claim than the request runs under.
    const claims = { sub: USER, exp: 9999999999, org_id: ORG_A, o: { id: ORG_B } };
    const token = `aGVhZGVy.${b64url(JSON.stringify(claims))}.sig`;
    expect(tokenIdentityKey(token)).toBe(vaultIdentityKey(signedIn(USER, ORG_A)));
  });

  it("treats an organization claim it cannot read as unusable, NEVER as personal", () => {
    // The generalization of the v2 lesson. Every one of these carries an org
    // claim in a shape this module does not understand; reading any of them as
    // "no org" would hand it the personal store.
    const personalKey = vaultIdentityKey(signedIn(USER, null));
    for (const claims of [
      { sub: USER, o: {} },
      { sub: USER, o: { id: "" } },
      { sub: USER, o: { slg: "acme", rol: "member" } },
      { sub: USER, o: "org_2abc" },
      { sub: USER, o: ["org_2abc"] },
      { sub: USER, org_id: "" },
      { sub: USER, org_id: 7 },
      { sub: USER, org_id: { id: ORG_A } },
    ]) {
      const key = tokenIdentityKey(`aGVhZGVy.${b64url(JSON.stringify(claims))}.sig`);
      expect(key).toBeNull();
      expect(key).not.toBe(personalKey);
    }
  });

  it("still recognizes a genuinely personal token", () => {
    // The other direction of the same knob: `org_id` absent, or explicitly
    // null, is Clerk's spelling of the personal instance and must resolve —
    // otherwise the refusal above would have quietly disabled the engine for
    // every user not in an org.
    expect(tokenIdentityKey(tokenV1(USER, null))).toBe(vaultIdentityKey(signedIn(USER, null)));
    const explicitNull = `aGVhZGVy.${b64url(JSON.stringify({ sub: USER, org_id: null, o: null }))}.sig`;
    expect(tokenIdentityKey(explicitNull)).toBe(vaultIdentityKey(signedIn(USER, null)));
  });

  it("cannot be forged into another identity by a separator in a claim", () => {
    // `vaultNamespace`'s escaping carries over because this routes through the
    // same encoder. Without it, `sub: "a:o:b"` with no org would produce the
    // same string as `sub: "a"` in org `b` — a token that opens someone else's
    // store.
    const forger = `aGVhZGVy.${b64url(JSON.stringify({ sub: "a:o:b", org_id: "c" }))}.sig`;
    const victim = `aGVhZGVy.${b64url(JSON.stringify({ sub: "a", org_id: "b:o:c" }))}.sig`;
    expect(tokenIdentityKey(forger)).not.toBe(tokenIdentityKey(victim));
    expect(engineSessionFor(signedIn("a", "b:o:c"), forger)).toBeNull();
  });

  it("is comparable to a store name's identity by construction", () => {
    // Not an incidental equality — the assertion is that both sides go through
    // `vaultIdentityKey`. If one of them ever grows its own encoder, this goes
    // red, and a drift here fails OPEN (a token accepted for the wrong store).
    for (const [user, org] of [
      [USER, ORG_A],
      [USER, null],
      [OTHER_USER, ORG_B],
      ["a:b", "c:d"],
    ] as Array<[string, string | null]>) {
      expect(tokenIdentityKey(tokenV1(user, org))).toBe(vaultIdentityKey(signedIn(user, org)));
    }
  });
});
