import { expect, it, vi } from "vitest";
const destination = vi.hoisted(() => ({ redirect: vi.fn((path: string) => { throw new Error(path); }) }));
vi.mock("next/navigation", () => ({ redirect: destination.redirect }));
import { redirectPage } from "./pageRedirect";
it("preserves repeated selectors when resolving a retired bookmark before mounting", async () => {
  await expect(redirectPage("/workspaces", Promise.resolve({ id: "reef 1", tag: ["a", "b"], missing: undefined }))).rejects.toThrow("/workspaces?id=reef+1&tag=a&tag=b");
});
it("drops unrelated retired-surface selectors when the destination requires it", async () => {
  await expect(redirectPage("/home", Promise.resolve({ graph: "old" }), false)).rejects.toThrow("/home");
});
