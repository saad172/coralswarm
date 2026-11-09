import { expect, it } from "vitest";
import { SHARED_OCEAN_HUES, oceanHue } from "./oceanHue";

it("the private ocean is always pink", () => {
  expect(oceanHue({ graph_id: "a", kind: "member" })).toBe("pink");
  expect(oceanHue({ graph_id: "zzz", kind: "personal" })).toBe("pink");
});

it("other oceans take one of teal / amber / plum / coral, deterministically per id", () => {
  const ids = ["g-1", "g-2", "g-3", "g-4", "g-5", "g-6", "g-7", "g-8"];
  for (const id of ids) {
    const h = oceanHue({ graph_id: id, kind: "org" });
    expect(SHARED_OCEAN_HUES).toContain(h);
    expect(oceanHue({ graph_id: id, kind: "team" })).toBe(h);
  }
  // It does not depend on list position: only on the id.
  expect(new Set(ids.map((id) => oceanHue({ graph_id: id, kind: "org" }))).size).toBeGreaterThan(1);
});

it("a stored hue wins when one ever ships; an unknown one is ignored", () => {
  expect(oceanHue({ graph_id: "a", kind: "org", hue: "plum" })).toBe("plum");
  expect(SHARED_OCEAN_HUES).toContain(oceanHue({ graph_id: "a", kind: "org", hue: "chartreuse" }));
});
