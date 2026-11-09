import { describe, expect, it } from "vitest";
import { canonicalSourceType, facetLabelFor, hueForSource, iconKindFor, sourceMeta } from "./sourceKind";

/* The GitHub connector's six atom types (docs/connectors/github.md §3.5) —
 * pinning the fifth source-type switch site the connector brief calls out. */

const GITHUB_TYPES = [
  ["github_pr", "Pull request"],
  ["github_issue", "Issue"],
  ["github_release", "Release"],
  ["github_commit", "Commit"],
  ["github_code", "Code"],
  ["github_code_summary", "Code summary"],
] as const;

describe("sourceMeta for github_* types", () => {
  it.each(GITHUB_TYPES)("gives %s a real label, not a raw-token fallback", (type, label) => {
    expect(sourceMeta(type)).toEqual({ label, hue: "pink", icon: "github" });
  });

  it.each(GITHUB_TYPES)("hues %s the same as the GitHub provider tile (pink)", (type) => {
    expect(hueForSource(type)).toBe("pink");
  });

  it.each(GITHUB_TYPES)("icons %s as github, distinct from the coding_session glyph", (type) => {
    expect(iconKindFor(type)).toBe("github");
    expect(iconKindFor(type)).not.toBe(iconKindFor("coding_session"));
  });

  it.each(GITHUB_TYPES)("does not alias %s to anything else", (type) => {
    expect(canonicalSourceType(type)).toBe(type);
  });

  it("collapses all six onto one facet label", () => {
    for (const [type] of GITHUB_TYPES) {
      expect(facetLabelFor(type)).toBe("GitHub");
    }
  });

  it("never falls through to the raw-token default for a github type", () => {
    // `sourceMeta`'s fallback echoes the input token as the label when it
    // doesn't recognise it — asserting the label is NOT that literal string
    // pins that every one of the six is actually registered, not silently
    // falling through and passing this test by coincidence.
    for (const [type, label] of GITHUB_TYPES) {
      expect(sourceMeta(type).label).not.toBe(type);
      expect(sourceMeta(type).label).toBe(label);
    }
  });
});
