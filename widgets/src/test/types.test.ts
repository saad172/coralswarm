import { describe, it, expect } from "vitest";
import { isAskOceanWorking, isAskOceanTerminal, isAskOceanCompleted, isAskOceanErrorBody } from "../types";

describe("isAskOceanWorking", () => {
  it("is true ONLY for status === 'working'", () => {
    expect(isAskOceanWorking({ status: "working", task_id: "t1" })).toBe(true);
  });

  it("is false for a terminal 'failed' envelope (has an error, will never complete)", () => {
    expect(isAskOceanWorking({ status: "failed", error: { code: "x", message: "boom" } })).toBe(false);
  });

  it("is false for a terminal 'cancelled' envelope", () => {
    expect(isAskOceanWorking({ status: "cancelled" })).toBe(false);
  });

  it("is false for null/undefined", () => {
    expect(isAskOceanWorking(null)).toBe(false);
    expect(isAskOceanWorking(undefined)).toBe(false);
  });
});

describe("isAskOceanTerminal", () => {
  it("is true for 'failed'", () => {
    expect(isAskOceanTerminal({ status: "failed", error: { code: "x", message: "boom" } })).toBe(true);
  });

  it("is true for 'cancelled'", () => {
    expect(isAskOceanTerminal({ status: "cancelled" })).toBe(true);
  });

  it("is false for 'working'", () => {
    expect(isAskOceanTerminal({ status: "working" })).toBe(false);
  });

  it("is false for a completed body (no status key at all)", () => {
    expect(
      isAskOceanTerminal({
        mode: "synthesized",
        answer: "hi",
        citations: [],
        citation_details: [],
        unreachable_oceans: [],
      }),
    ).toBe(false);
  });
});

describe("isAskOceanCompleted / isAskOceanErrorBody stay disjoint from the terminal envelope", () => {
  const completed = {
    mode: "synthesized" as const,
    answer: "hi",
    citations: [],
    citation_details: [],
    unreachable_oceans: [],
  };
  const errorBody = { error: { code: "invalid_params", message: "bad" } };
  const terminal = { status: "failed" as const, error: { code: "x", message: "boom" } };

  it("a completed body is never terminal or an error body", () => {
    expect(isAskOceanCompleted(completed)).toBe(true);
    expect(isAskOceanTerminal(completed)).toBe(false);
    expect(isAskOceanErrorBody(completed)).toBe(false);
  });

  it("an application-error body is never completed or terminal", () => {
    expect(isAskOceanErrorBody(errorBody)).toBe(true);
    expect(isAskOceanCompleted(errorBody)).toBe(false);
    expect(isAskOceanTerminal(errorBody)).toBe(false);
  });

  it("a terminal envelope is never completed or an application-error body", () => {
    expect(isAskOceanTerminal(terminal)).toBe(true);
    expect(isAskOceanCompleted(terminal)).toBe(false);
    expect(isAskOceanErrorBody(terminal)).toBe(false);
  });
});
