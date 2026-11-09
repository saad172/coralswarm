// @vitest-environment jsdom
import { cleanup, render, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { RevealAnswer } from "./RevealAnswer";

vi.mock("@/lib/motion", async (original) => ({
  ...await original<typeof import("@/lib/motion")>(),
  useReducedMotion: () => false,
}));
afterEach(cleanup);

it("makes delayed controls visibly opaque synchronously on focus with real Motion", async () => {
  const view = render(<RevealAnswer active delay={760}><button type="button">Copy</button></RevealAnswer>);
  const button = view.getByRole("button");
  const root = button.parentElement as HTMLElement;
  await waitFor(() => expect(getComputedStyle(root).opacity).toBe("0"));
  button.focus();
  expect(document.activeElement).toBe(button);
  expect(getComputedStyle(root).opacity).toBe("1");
});

it("reveals the citation and its delayed ancestor in the focus event", async () => {
  const view = render(<RevealAnswer active><div><p>First</p><p><a className="cite" href="/source">Source</a></p></div></RevealAnswer>);
  const link = view.getByRole("link");
  await waitFor(() => expect(getComputedStyle(link).opacity).toBe("0"));
  link.focus();
  expect(getComputedStyle(link).opacity).toBe("1");
  expect(getComputedStyle(link.parentElement as HTMLElement).opacity).toBe("1");
});
