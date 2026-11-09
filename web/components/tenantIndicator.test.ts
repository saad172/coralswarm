import { describe, expect, it } from "vitest";
import type { Instance, InstanceSwitcher } from "./useInstanceSwitcher";
import {
  tenantIndicator,
  tenantIndicatorAriaLabel,
  tenantSwitcherAriaLabel,
  type TenantIdentity,
  type TenantIndicator,
} from "./tenantIndicator";

/* The subject is "what does the app TELL the user about which tenant they are
 * in", which after this PR is a security signal rather than a nicety: the
 * hostname used to answer that question and no longer does.
 *
 * TESTS ARE WRITTEN TO GO RED ON THE OLD BEHAVIOUR. The deleted expression was
 * `instance.active?.name ?? "Personal"`, so any assertion whose strongest claim
 * is "some label came out" is green on the bug and worth nothing. Every case
 * below either names the exact string, or asserts the ABSENCE of a label — and
 * the absence cases are the ones that matter. */

const personal: Instance = { kind: "personal", id: "personal", name: "Personal" };
const acme: Instance = { kind: "org", id: "org_2abc", name: "Acme Inc", slug: "acme" };
const globex: Instance = { kind: "org", id: "org_9xyz", name: "Globex", slug: "globex" };

/** Narrowing helper for the assertions that are about a label. Throws rather
 *  than returning a default, so a test can never silently assert against a
 *  fallback the production code does not have. */
function labelled(t: TenantIndicator): string {
  if (t.state === "unknown") throw new Error(`expected a labelled state, got ${t.state}`);
  return t.label;
}

describe("tenantIndicator — the loading state", () => {
  it("is unknown while Clerk is loading, and carries NO label to render", () => {
    const t = tenantIndicator({ loaded: false, active: null });

    // THE assertion of this PR. The shipped code answered "Personal" here.
    expect(t).toEqual({ state: "unknown" });
    // Spelled a second way, because `toEqual` on the whole object would still
    // pass if a future `unknown` variant grew a `label` and this one happened
    // to be constructed without it: there must be no name to print, ever.
    expect(t).not.toHaveProperty("label");
  });

  it("stays unknown even when an unloaded hook offers an active instance", () => {
    /* Not defensive noise. The current hook nulls `active` while loading, so
       this case is unreachable TODAY — which is exactly why the ordering could
       be inverted by a later edit without any test noticing. A Clerk that
       retained the previous session's org across a reload while `isLoaded` is
       false would, under the wrong ordering, print the OLD tenant's name over
       the NEW tenant's data. `loaded` is checked first, and this is the test
       that says so. */
    expect(tenantIndicator({ loaded: false, active: acme })).toEqual({ state: "unknown" });
    expect(tenantIndicator({ loaded: false, active: personal })).toEqual({ state: "unknown" });
  });

  it("is unknown — not personal — if a loaded hook reports no active instance", () => {
    /* `loaded: true, active: null` is a self-contradiction the hook cannot
       currently produce. Guessing "Personal" from it is the same wrong answer
       the old coalesce gave, arrived at from a different direction. */
    expect(tenantIndicator({ loaded: true, active: null })).toEqual({ state: "unknown" });
  });
});

describe("tenantIndicator — personal", () => {
  it("names the personal instance once loaded", () => {
    expect(tenantIndicator({ loaded: true, active: personal })).toEqual({
      state: "personal",
      label: "Personal",
    });
  });

  it("carries no hue — personal is not colour-coded", () => {
    /* The hue exists to tell two organizations apart. There is only ever one
       personal instance, so it takes the neutral treatment, and the union
       shape is what stops a render site from reaching for a colour that has no
       meaning. */
    expect(tenantIndicator({ loaded: true, active: personal })).not.toHaveProperty("hue");
  });

  it("falls back to 'Personal' if the instance name is blank", () => {
    const blank: Instance = { kind: "personal", id: "personal", name: "   " };
    expect(labelled(tenantIndicator({ loaded: true, active: blank }))).toBe("Personal");
  });
});

describe("tenantIndicator — organizations", () => {
  it("names the organization once loaded", () => {
    expect(tenantIndicator({ loaded: true, active: acme })).toMatchObject({
      state: "org",
      label: "Acme Inc",
    });
  });

  it("trims the name rather than rendering the padding", () => {
    const padded: Instance = { ...acme, name: "  Acme Inc  " };
    expect(labelled(tenantIndicator({ loaded: true, active: padded }))).toBe("Acme Inc");
  });

  it("falls back name → slug → id, never to a generic word", () => {
    /* A generic "Organization" would be non-empty AND identical for every
       tenant, which satisfies the letter of "the chip has text in it" while
       destroying the only property that matters: that two different tenants
       look different. */
    const noName: Instance = { ...acme, name: "" };
    expect(labelled(tenantIndicator({ loaded: true, active: noName }))).toBe("acme");

    const noNameNoSlug: Instance = { ...acme, name: "  ", slug: null };
    expect(labelled(tenantIndicator({ loaded: true, active: noNameNoSlug }))).toBe("org_2abc");
  });

  it("still produces a non-empty label when nothing identifies the org", () => {
    /* An empty label renders as an empty chip, which reads as "no indicator" —
       the precise state this module exists to prevent. */
    const nothing: Instance = { kind: "org", id: "", name: "", slug: null };
    expect(labelled(tenantIndicator({ loaded: true, active: nothing }))).toBe("Organization");
  });

  it("gives two differently-named orgs two different hues", () => {
    /* The hue is the pre-verbal half of the signal. Named orgs, not ids: every
       Clerk org id starts "org_" and `hueForName` reads only the first code
       point, so keying on the id would paint every organization the same
       colour and the hue would carry nothing. This case is the reason the
       implementation does NOT do that — it is red if it does. */
    const a = tenantIndicator({ loaded: true, active: acme });
    const b = tenantIndicator({ loaded: true, active: globex });
    expect(a).toMatchObject({ state: "org" });
    expect(b).toMatchObject({ state: "org" });
    expect(a).not.toEqual(b);
    if (a.state !== "org" || b.state !== "org") throw new Error("expected two orgs");
    expect(a.hue).not.toBe(b.hue);
  });

  it("gives one org the same hue every time it is asked", () => {
    const first = tenantIndicator({ loaded: true, active: acme });
    // A different object, same tenant — the hue must not depend on identity.
    const again = tenantIndicator({ loaded: true, active: { ...acme } });
    expect(first).toEqual(again);
  });

  it("keys the hue on the label shown, not on the id behind it", () => {
    /* Same organization id, two different displayed labels — the hue follows
       the label. Written this way on purpose: under an id-keyed implementation
       these two are identical, so this case is the one that goes red on it,
       whereas comparing two orgs whose ids merely differ would not (all Clerk
       ids begin "org_", and only the first code point is read). */
    const named = tenantIndicator({ loaded: true, active: acme }); // "Acme Inc"
    const bySlug = tenantIndicator({ loaded: true, active: { ...acme, name: "" } }); // "acme"
    if (named.state !== "org" || bySlug.state !== "org") throw new Error("expected two orgs");
    expect([named.label, bySlug.label]).toEqual(["Acme Inc", "acme"]);
    expect(named.hue).not.toBe(bySlug.hue);
  });
});

describe("accessible names", () => {
  it("does not claim an account while identity is loading", () => {
    const spoken = tenantIndicatorAriaLabel(tenantIndicator({ loaded: false, active: null }));
    expect(spoken).toBe("Loading which account you are in");
    // The failure being guarded is a screen reader announcing a tenant that
    // may not be the one about to load.
    expect(spoken).not.toContain("Personal");
  });

  it("says the word 'organization' out loud for an org", () => {
    /* Sighted users get this from the tinted, hue-coded fill. Somebody hearing
       only "Acme Inc" cannot tell a company from a nickname on a personal
       account — the same missing signal in a different medium. */
    expect(tenantIndicatorAriaLabel(tenantIndicator({ loaded: true, active: acme }))).toBe(
      "Current account: Acme Inc, an organization",
    );
  });

  it("marks the personal account as personal", () => {
    expect(tenantIndicatorAriaLabel(tenantIndicator({ loaded: true, active: personal }))).toBe(
      "Current account: Personal, your personal account",
    );
  });

  it("adds the switcher's affordance without changing the identity claim", () => {
    // Exact strings on both, not `startsWith` against the other function —
    // an assertion compared against the thing it measures proves nothing.
    expect(tenantSwitcherAriaLabel(tenantIndicator({ loaded: true, active: acme }))).toBe(
      "Current account: Acme Inc, an organization. Switch account",
    );
    expect(tenantSwitcherAriaLabel(tenantIndicator({ loaded: true, active: personal }))).toBe(
      "Current account: Personal, your personal account. Switch account",
    );
    expect(tenantSwitcherAriaLabel(tenantIndicator({ loaded: false, active: null }))).toBe(
      "Loading which account you are in. Switch account",
    );
  });
});

describe("the hook's own return value is a valid input", () => {
  it("accepts a whole InstanceSwitcher, extra fields and all", () => {
    /* AppSidebar and AccountIndicator both pass `useInstanceSwitcher()`
       straight through. The value here is TYPED as the hook's full return
       type, so if `TenantIdentity` ever stops accepting that shape it fails
       here — in a file that explains why — rather than as a red squiggle at
       two call sites. `loaded`/`active` are the hook's own field names for
       exactly this reason; they were not renamed on the way in. */
    const fromHook: InstanceSwitcher = {
      loaded: true,
      instances: [personal, acme],
      active: acme,
      soloInstance: false,
      switchTo: async () => {},
    };
    const asIdentity: TenantIdentity = fromHook;
    expect(tenantIndicator(asIdentity)).toMatchObject({ state: "org", label: "Acme Inc" });
  });

  it("reads the same 'loading' shape the hook actually returns", () => {
    /* Copied field-for-field from useInstanceSwitcher's `!isLoaded` early
       return. If that return ever changes — say `active` starts being
       populated — this fixture goes stale silently, so the assertion is
       written against `loaded`, which is the field the model branches on. */
    const whileLoading: InstanceSwitcher = {
      loaded: false,
      instances: [],
      active: null,
      soloInstance: true,
      switchTo: async () => {},
    };
    expect(tenantIndicator(whileLoading)).toEqual({ state: "unknown" });
    // And the thing that used to be drawn from that shape, spelled out:
    // `soloInstance` is true here, so the sidebar's solo branch would have
    // rendered — which is why `loaded` has to be checked before it.
    expect(whileLoading.soloInstance).toBe(true);
  });
});
