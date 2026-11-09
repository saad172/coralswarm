import { describe, expect, it } from "vitest";
import { ORG_BILLING_HREF, PERSONAL_BILLING_HREF, paywallCopy, type RefusalReason } from "./paywall";

const REASONS: RefusalReason[] = [
  "credits_frozen", "free_plan", "extra_usage_off", "member_cap_reached", "monthly_limit_reached", "extra_balance_exhausted",
];

describe("paywallCopy", () => {
  it.each(REASONS)("%s: admins get an action on the org billing page", (reason) => {
    const c = paywallCopy(reason, "admin");
    expect(c?.action?.href).toBe(ORG_BILLING_HREF);
    expect(c?.message).not.toMatch(/ocean|reef|coral/i);
  });

  it.each(REASONS)("%s: personal accounts act on their own billing page", (reason) => {
    expect(paywallCopy(reason, "personal")?.action?.href).toBe(PERSONAL_BILLING_HREF);
  });

  it.each(REASONS)("%s: members have no action and are told to ask an admin", (reason) => {
    const c = paywallCopy(reason, "member");
    expect(c?.action).toBeNull();
    expect(c?.message).toMatch(/Ask an admin to/);
  });

  it("names the specific limit for each reason", () => {
    expect(paywallCopy("credits_frozen", "admin")?.message).toMatch(/subscription has ended/);
    expect(paywallCopy("free_plan", "admin")?.message).toMatch(/Free plan/);
    expect(paywallCopy("extra_usage_off", "admin")?.message).toMatch(/extra usage is off/);
    expect(paywallCopy("member_cap_reached", "member")?.message).toMatch(/your personal share/);
    expect(paywallCopy("monthly_limit_reached", "admin")?.message).toMatch(/monthly extra-usage limit/);
    expect(paywallCopy("extra_balance_exhausted", "admin")?.message).toMatch(/balance has run out/);
  });

  it("returns null when the reason is absent or unknown, so callers keep the old copy", () => {
    expect(paywallCopy(undefined, "admin")).toBeNull();
    expect(paywallCopy(null, "admin")).toBeNull();
    expect(paywallCopy("something_new", "admin")).toBeNull();
    expect(paywallCopy("toString", "admin")).toBeNull();
  });
});
