import { describe, expect, it } from "vitest";
import { connectErrorMessage } from "./connectErrorModel";

describe("connectErrorMessage", () => {
  it("maps expired to the calm retry copy", () => {
    expect(connectErrorMessage("expired")).toBe("That connect link expired — try again.");
  });
  it("shows nothing for unknown, missing, or prototype-ish values", () => {
    expect(connectErrorMessage("<script>")).toBeNull();
    expect(connectErrorMessage("toString")).toBeNull();
    expect(connectErrorMessage(null)).toBeNull();
    expect(connectErrorMessage("")).toBeNull();
  });
});
