// Unit cover for the profile-view derivations. These run against the minimal
// structural types in lib/profile.ts (not Clerk's own), which is the point:
// every one of these inputs is a state the profile page really sees while
// Clerk resolves — no user yet, a user with no emails, a name that IS the
// email, an unverified secondary address.

import { describe, expect, it } from "vitest";
import {
  FALLBACK_NAME,
  accountLabel,
  contextLabel,
  displayName,
  formatDateTime,
  formatMonthYear,
  oceanKindLabel,
  orderedEmails,
  primaryEmail,
  providerLabel,
  showEmailUnderName,
  type ProfileUser,
} from "./profile";

const user = (over: Partial<ProfileUser> = {}): ProfileUser => ({
  fullName: "Ada Lovelace",
  username: null,
  primaryEmailAddressId: "e1",
  emails: [{ id: "e1", emailAddress: "ada@example.com", verified: true }],
  ...over,
});

describe("primaryEmail", () => {
  it("picks the address matching primaryEmailAddressId", () => {
    const u = user({
      primaryEmailAddressId: "e2",
      emails: [
        { id: "e1", emailAddress: "old@example.com", verified: true },
        { id: "e2", emailAddress: "new@example.com", verified: true },
      ],
    });
    expect(primaryEmail(u)).toBe("new@example.com");
  });

  it("falls back to the first address when Clerk reports no primary id", () => {
    expect(primaryEmail(user({ primaryEmailAddressId: null }))).toBe("ada@example.com");
  });

  it("is null for no user and for a user with no addresses", () => {
    expect(primaryEmail(null)).toBeNull();
    expect(primaryEmail(user({ emails: [] }))).toBeNull();
  });
});

describe("displayName", () => {
  it("prefers full name, then username, then email", () => {
    expect(displayName(user())).toBe("Ada Lovelace");
    expect(displayName(user({ fullName: null, username: "ada" }))).toBe("ada");
    expect(displayName(user({ fullName: null, username: null }))).toBe("ada@example.com");
  });

  it("treats whitespace-only name/username as absent", () => {
    expect(displayName(user({ fullName: "   ", username: "\t" }))).toBe("ada@example.com");
  });

  it("never returns an empty string (callers index [0] for the initial)", () => {
    const nameless = displayName(user({ fullName: null, username: null, emails: [] }));
    expect(nameless).toBe(FALLBACK_NAME);
    expect(displayName(null)).toBe(FALLBACK_NAME);
    expect(nameless[0]).toBeTruthy();
  });
});

describe("accountLabel", () => {
  it("prefers name, then username, then email, then the caller's fallback", () => {
    expect(accountLabel({ fullName: "Ada Lovelace", username: "ada" }, "user_123")).toBe(
      "Ada Lovelace",
    );
    expect(accountLabel({ username: "ada", primaryEmail: "ada@example.com" }, "user_123")).toBe(
      "ada",
    );
    expect(accountLabel({ primaryEmail: "ada@example.com" }, "user_123")).toBe("ada@example.com");
    // Clerk not loaded yet — the header keeps showing the raw id, not a blank.
    expect(accountLabel({}, "user_123")).toBe("user_123");
    expect(accountLabel(null, "user_123")).toBe("user_123");
    expect(accountLabel(undefined, "You")).toBe("You");
  });

  it("ignores whitespace-only values on both the user and the fallback", () => {
    expect(accountLabel({ fullName: "  ", username: " " }, "user_123")).toBe("user_123");
    expect(accountLabel({}, "   ")).toBe(FALLBACK_NAME);
  });
});

describe("orderedEmails", () => {
  it("hoists the primary address and keeps Clerk's order for the rest", () => {
    const u = user({
      primaryEmailAddressId: "e2",
      emails: [
        { id: "e1", emailAddress: "a@example.com", verified: true },
        { id: "e2", emailAddress: "b@example.com", verified: true },
        { id: "e3", emailAddress: "c@example.com", verified: false },
      ],
    });
    expect(orderedEmails(u).map((e) => e.id)).toEqual(["e2", "e1", "e3"]);
    expect(orderedEmails(u)[2].verified).toBe(false);
  });

  it("drops nothing when the primary id matches no address", () => {
    const u = user({ primaryEmailAddressId: "missing" });
    expect(orderedEmails(u).map((e) => e.id)).toEqual(["e1"]);
  });

  it("is empty for no user", () => {
    expect(orderedEmails(null)).toEqual([]);
  });
});

describe("showEmailUnderName", () => {
  it("suppresses the email when the name is only the email echoed back", () => {
    expect(showEmailUnderName(user({ fullName: null, username: null }))).toBe(false);
  });

  it("shows the email alongside a real name", () => {
    expect(showEmailUnderName(user())).toBe(true);
  });

  it("shows nothing when there is no email at all", () => {
    expect(showEmailUnderName(user({ emails: [] }))).toBe(false);
    expect(showEmailUnderName(null)).toBe(false);
  });
});

describe("contextLabel", () => {
  it("names the active org", () => {
    expect(contextLabel({ name: "Acme Architects" })).toBe("Acme Architects");
  });

  it("calls the no-org case Personal, matching OrgSwitcher", () => {
    expect(contextLabel(null)).toBe("Personal");
    expect(contextLabel(undefined)).toBe("Personal");
    expect(contextLabel({ name: "  " })).toBe("Personal");
  });
});

describe("date formatting", () => {
  it("formats month precision for joined-at", () => {
    expect(formatMonthYear(new Date("2026-03-14T12:00:00Z"))).toMatch(/2026/);
  });

  it("formats an absolute date+time for last sign-in", () => {
    expect(formatDateTime(new Date("2026-03-14T12:00:00Z"))).toMatch(/2026/);
  });

  it("returns null for absent or invalid dates rather than 'Invalid Date'", () => {
    expect(formatMonthYear(null)).toBeNull();
    expect(formatMonthYear(new Date("nope"))).toBeNull();
    expect(formatDateTime(undefined)).toBeNull();
    expect(formatDateTime(new Date("nope"))).toBeNull();
  });
});

describe("providerLabel", () => {
  it("maps known Clerk oauth strategies to product names", () => {
    expect(providerLabel("oauth_google")).toBe("Google");
    expect(providerLabel("oauth_github")).toBe("GitHub");
    expect(providerLabel("google")).toBe("Google");
  });

  it("title-cases an unknown provider instead of hiding it", () => {
    expect(providerLabel("oauth_linear")).toBe("Linear");
    expect(providerLabel("oauth_custom_sso")).toBe("Custom Sso");
  });

  it("passes through a slug it cannot parse", () => {
    expect(providerLabel("oauth_")).toBe("oauth_");
  });
});

describe("oceanKindLabel", () => {
  // A member ocean and a consumer-plane personal ocean deliberately read the
  // same. They are the same thing to a user — an ocean only its owner can
  // reach — and the plane they sit on is not a distinction worth surfacing.
  // The old "member-private" wording additionally implied a cryptographic
  // property the product does not have.
  it("labels both owner-only kinds 'personal'", () => {
    expect(oceanKindLabel("personal")).toBe("personal");
    expect(oceanKindLabel("member")).toBe("personal");
    expect(oceanKindLabel("team")).toBe("shared team");
    expect(oceanKindLabel("org")).toBe("org commons");
  });

  it("shows an unknown kind verbatim", () => {
    expect(oceanKindLabel("channel")).toBe("channel");
  });
});
