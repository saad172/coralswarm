import { describe, expect, it } from "vitest";
import { decideOceanBootstrap, effectivePlane, UnexpectedEmptyOrgError } from "./oceanBootstrap";

describe("decideOceanBootstrap", () => {
  it.each([
    ["personal", 1, { type: "use" }],
    ["personal", 5, { type: "use" }],
    ["org", 1, { type: "use" }],
    ["org", 5, { type: "use" }],
  ] as const)("plane=%s, oceanCount=%d -> use", (plane, count, expected) => {
    expect(decideOceanBootstrap(plane, count)).toEqual(expected);
  });

  it("empty list on the personal plane falls back to legacy first-run create", () => {
    expect(decideOceanBootstrap("personal", 0)).toEqual({ type: "create-personal" });
  });

  it("empty list on the org plane is NEVER papered over with a personal ocean", () => {
    expect(decideOceanBootstrap("org", 0)).toEqual({ type: "unexpected-empty-org" });
  });

  it("a non-empty list wins regardless of plane, even org", () => {
    // Guards against a future edit accidentally checking plane before count.
    expect(decideOceanBootstrap("org", 1)).toEqual({ type: "use" });
  });
});

describe("effectivePlane", () => {
  it("a personal org pulls an inferred org plane down to personal", () => {
    // The whole bug this function exists to close: Clerk's `orgId` alone
    // cannot tell a personal org (a Clerk org standing for one person's own
    // ocean) from a real one, so the caller infers "org" — this corrects it.
    expect(effectivePlane("org", true)).toBe("personal");
  });

  it("a real organization is left alone", () => {
    expect(effectivePlane("org", false)).toBe("org");
  });

  it("the personal plane is unaffected either way", () => {
    expect(effectivePlane("personal", true)).toBe("personal");
    expect(effectivePlane("personal", false)).toBe("personal");
  });

  it("composes with decideOceanBootstrap to fix the empty-personal-org boot", () => {
    // Before this fix: `decideOceanBootstrap("org", 0)` for a personal org
    // with no ocean yet returned `unexpected-empty-org`, which
    // `providers.tsx` throws as `UnexpectedEmptyOrgError` — a personal-org
    // user with zero oceans could never boot. Composed through
    // `effectivePlane`, the same inputs now take the personal-plane
    // first-run path instead.
    const plane = effectivePlane("org", true);
    expect(decideOceanBootstrap(plane, 0)).toEqual({ type: "create-personal" });
  });

  it("a REAL org with zero oceans is unchanged — still surfaced as unexpected", () => {
    // The regression guard: `effectivePlane` must not blunt the real-org
    // empty-ocean error this module exists to surface.
    const plane = effectivePlane("org", false);
    expect(decideOceanBootstrap(plane, 0)).toEqual({ type: "unexpected-empty-org" });
  });
});

describe("UnexpectedEmptyOrgError", () => {
  it("is a distinguishable Error subclass with an actionable message", () => {
    const err = new UnexpectedEmptyOrgError();
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(UnexpectedEmptyOrgError);
    expect(err.name).toBe("UnexpectedEmptyOrgError");
    expect(err.message.length).toBeGreaterThan(0);
  });

  it("is distinguishable from a plain Error via instanceof (what providers.tsx's catch relies on)", () => {
    const plain = new Error("boom");
    expect(plain instanceof UnexpectedEmptyOrgError).toBe(false);
  });
});
