import { describe, expect, it } from "vitest";
import {
  ADDON_CONTINUE_BUTTON,
  approvedLine,
  approvingAsLine,
  primaryEmailOf,
} from "./addinApprove";

/* The Gmail Add-on approval page's identity line
 * (`app/(app)/addin/approve/page.tsx`).
 *
 * The bug these pin is silent by construction: approving binds a long-lived
 * bearer to whoever is signed in IN THIS BROWSER, which is routinely not the
 * account Gmail is open in. The page said nothing about which account that
 * was, so a mis-binding looked exactly like a success. These tests assert the
 * account is NAMED both before the button and in the confirmation, and that
 * the unresolved case still warns instead of quietly rendering nothing. */

describe("primaryEmailOf", () => {
  it("reads the primary address", () => {
    expect(primaryEmailOf({ primaryEmailAddress: { emailAddress: "person@example.com" } })).toBe(
      "person@example.com",
    );
  });

  it("falls back to the first address when no primary is set", () => {
    expect(
      primaryEmailOf({
        primaryEmailAddress: null,
        emailAddresses: [{ emailAddress: "first@example.com" }, { emailAddress: "second@example.com" }],
      }),
    ).toBe("first@example.com");
  });

  it("skips blank entries rather than naming an empty account", () => {
    expect(
      primaryEmailOf({
        primaryEmailAddress: { emailAddress: "   " },
        emailAddresses: [{ emailAddress: "" }, { emailAddress: "real@example.com" }],
      }),
    ).toBe("real@example.com");
  });

  it("is null — never a placeholder — while Clerk is still resolving", () => {
    expect(primaryEmailOf(null)).toBeNull();
    expect(primaryEmailOf(undefined)).toBeNull();
    expect(primaryEmailOf({})).toBeNull();
  });
});

describe("approvingAsLine (rendered directly above the Approve button)", () => {
  it("names the signed-in address", () => {
    expect(approvingAsLine("person@example.com")).toBe("Approving as person@example.com");
  });

  /* The failure this prevents: an empty or absent line reads as "nothing to
     check here", and the unresolved case is EXACTLY when someone can't tell
     which account they're about to bind. */
  it("still warns when no address is known yet", () => {
    const line = approvingAsLine(null);
    expect(line).not.toBe("");
    expect(line).toContain("signed in");
    expect(line).toContain("not the Google account Gmail is open in");
  });
});

describe("approvedLine (the confirmation)", () => {
  it("names the same account again and points back at Gmail", () => {
    const line = approvedLine("person@example.com");
    expect(line).toContain("person@example.com");
    expect(line).toContain("Go back to Gmail");
  });

  /* BYTE-FOR-BYTE against the add-on's own button. `extensions/gmail-addin/
     Card.gs` renders `.setText("I've approved — continue")` — em dash, curly
     apostrophe. A paraphrase (or an ASCII hyphen) sends people hunting for a
     button that isn't there, and nothing else in this repo would catch the
     drift, since the two strings live in different languages. */
  it("quotes the add-on's continue button exactly as Card.gs renders it", () => {
    expect(ADDON_CONTINUE_BUTTON).toBe("I've approved — continue");
    expect(approvedLine("person@example.com")).toContain(`“${ADDON_CONTINUE_BUTTON}”`);
  });

  it("degrades to a truthful noun when the address is unknown", () => {
    const line = approvedLine(null);
    expect(line).toContain("your CoralSwarm account");
    expect(line).toContain(ADDON_CONTINUE_BUTTON);
    expect(line).not.toContain("null");
  });
});
