import { describe, expect, it } from "vitest";
import { ApiError } from "@/lib/api";
import { DELETE_ORG_FACTS, nameMatches, orgDeletionErrorCopy } from "./orgDeletionModel";

describe("org deletion copy and rules", () => {
  it("matches the typed name like the server: trimmed, case-insensitive, never empty", () => {
    expect(nameMatches("  acme inc ", "Acme Inc")).toBe(true);
    expect(nameMatches("acme", "Acme Inc")).toBe(false);
    expect(nameMatches("", "")).toBe(false);
    expect(nameMatches("x", null)).toBe(false);
  });

  it("says plainly that access ends now, there is no refund, 7 days to export or undo, and undo does not restore billing", () => {
    const all = DELETE_ORG_FACTS.join(" ");
    expect(all).toMatch(/Access ends now/);
    expect(all).toMatch(/not refunded/);
    expect(all).toMatch(/7 days/);
    expect(all).toMatch(/export/);
    expect(all).toMatch(/undo/i);
    expect(all).toMatch(/subscribe again/);
  });

  it("says plainly that every member's private notes are deleted and members get no copy", () => {
    const all = DELETE_ORG_FACTS.join(" ");
    expect(all).toMatch(/Every member's private notes in this organization will be permanently deleted/);
    expect(all).toMatch(/Members won't get a copy/);
  });

  it("maps every server code to plain copy", () => {
    const copy = (code: string) => orgDeletionErrorCopy(new ApiError("x", 400, code));
    expect(copy("confirmation_mismatch")).toMatch(/doesn't match/);
    expect(copy("not_owner")).toMatch(/owner/);
    expect(copy("not_yet")).toMatch(/few seconds/);
    expect(copy("too_late")).toMatch(/too late/);
    expect(copy("not_self_serve")).toMatch(/support/);
    expect(copy("interactive_session_required")).toMatch(/sign back in/);
    expect(copy("export_too_large")).toMatch(/support/);
    expect(copy("export_in_progress")).toMatch(/already being prepared/);
    expect(orgDeletionErrorCopy(new Error("boom"))).toMatch(/Try again/);
  });
});
