// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { Badge } from "./Badge";
import { Meter } from "./States";
import { Modal } from "./Modal";

afterEach(cleanup);

it("Badge quiet is a line border with ink-3 text and no tone colour", () => {
  render(<Badge quiet tone="bad">Beta</Badge>);
  const el = screen.getByText("Beta");
  expect(el).toHaveClass("border-line", "text-ink-3");
  expect(el.style.color).toBe("");
  expect(el.style.background).toBe("");
});

it("Badge without quiet is unchanged", () => {
  render(<Badge tone="ok" outline>Live</Badge>);
  expect(screen.getByText("Live").style.border).toContain("var(--ok)");
});

it("Meter indeterminate drops the value attributes and uses the sliding segment", () => {
  const { container } = render(<Meter value={40} indeterminate ariaLabel="Importing" />);
  const bar = screen.getByRole("progressbar", { name: "Importing" });
  expect(bar).not.toHaveAttribute("aria-valuenow");
  expect(container.querySelector(".meter-indeterminate")).not.toBeNull();
});

it("Meter determinate is unchanged", () => {
  render(<Meter value={40} />);
  expect(screen.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "40");
});

it("Modal renders the header slot above the title and a display-voice title", () => {
  render(<Modal open onClose={() => {}} title="Connect Slack" header={<span>rail</span>} display>body</Modal>);
  const title = screen.getByRole("heading", { name: "Connect Slack" });
  expect(title).toHaveClass("font-display");
  const rail = screen.getByText("rail");
  expect(rail.compareDocumentPosition(title) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
});

it("Modal keeps the sans title by default and is a bottom sheet below sm", () => {
  render(<Modal open onClose={() => {}} title="Plain">body</Modal>);
  expect(screen.getByRole("heading", { name: "Plain" })).not.toHaveClass("font-display");
  const dialog = screen.getByRole("dialog");
  expect(dialog).toHaveClass("rounded-t-frame", "sm:rounded-frame");
  expect(dialog.parentElement).toHaveClass("items-end", "sm:items-center");
});
