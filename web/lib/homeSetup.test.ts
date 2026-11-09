import { describe, expect, it } from "vitest";
import { EMPTY_SETUP, readSetup, writeSetup } from "./homeSetup";

const memory = () => {
  const m = new Map<string, string>();
  return { getItem: (k: string) => m.get(k) ?? null, setItem: (k: string, v: string) => void m.set(k, v), map: m };
};

describe("setup marks", () => {
  it("round-trip per user and do not leak across users", () => {
    const s = memory();
    writeSetup("u1", { ...EMPTY_SETUP, mcp: true }, s);
    expect(readSetup("u1", s)).toEqual({ mcp: true, ask: false });
    expect(readSetup("u2", s)).toEqual(EMPTY_SETUP);
  });

  it("read the previous Home's stored shape, ignoring its retired steps", () => {
    const s = memory();
    s.map.set("cs.home.setup.u1", JSON.stringify({ connect: true, mcp: false, capture: true, ask: true }));
    expect(readSetup("u1", s)).toEqual({ mcp: false, ask: true });
  });

  it("survive junk and blocked storage", () => {
    const s = memory();
    s.map.set("cs.home.setup.u1", "{not json");
    expect(readSetup("u1", s)).toEqual(EMPTY_SETUP);
    const blocked = { getItem: () => { throw new Error("blocked"); }, setItem: () => { throw new Error("blocked"); } };
    expect(readSetup("u1", blocked)).toEqual(EMPTY_SETUP);
    expect(() => writeSetup("u1", EMPTY_SETUP, blocked)).not.toThrow();
  });
});
