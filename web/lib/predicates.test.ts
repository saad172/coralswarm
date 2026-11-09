import { describe, expect, it } from "vitest";
import { predicateShort, predicateVerb } from "./predicates";

describe("predicateVerb", () => {
  it("matches the backend's humanize_predicate phrasing for topical ids", () => {
    expect(predicateVerb("expert_in")).toBe("is the expert in");
    expect(predicateVerb("works_on")).toBe("works on");
    expect(predicateVerb("responsible_for")).toBe("is responsible for");
    expect(predicateVerb("mentioned_by")).toBe("is mentioned by");
  });

  it("falls back to the spaced id for unknown predicates", () => {
    expect(predicateVerb("co_authored_with")).toBe("co authored with");
  });

  it("has a neutral phrase when the predicate is missing", () => {
    expect(predicateVerb(undefined)).toBe("is linked to");
    expect(predicateVerb(null)).toBe("is linked to");
  });
});

describe("predicateShort", () => {
  it("drops the leading copula for chip-sized labels", () => {
    expect(predicateShort("expert_in")).toBe("expert in");
    expect(predicateShort("parent_of")).toBe("parent of");
    expect(predicateShort("spouse_of")).toBe("spouse of");
    expect(predicateShort("works_on")).toBe("works on");
  });
});
