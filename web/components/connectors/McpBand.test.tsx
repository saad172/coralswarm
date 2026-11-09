// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { McpBand } from "./McpBand";

// jsdom has no canvas; SwarmStream draws nothing without a 2d context.
HTMLCanvasElement.prototype.getContext = (() => null) as typeof HTMLCanvasElement.prototype.getContext;

afterEach(cleanup);

it("shows the address and the display title, with one italic brand word", () => {
  const { container } = render(<McpBand />);
  expect(screen.getByRole("heading", { name: "Use CoralSwarm anywhere" })).toBeInTheDocument();
  expect(container.querySelector("i")?.textContent).toBe("anywhere");
  expect(screen.getByText("https://api.coralswarm.example/mcp")).toBeInTheDocument();
  expect(screen.getByText(/no key/)).toBeInTheDocument();
});

it("the pill copies the address and says copied", async () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  Object.assign(navigator, { clipboard: { writeText } });
  render(<McpBand />);
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: /Copy the MCP address/ }));
  });
  expect(writeText).toHaveBeenCalledExactlyOnceWith("https://api.coralswarm.example/mcp");
  expect(screen.getByText("copied ✓")).toBeInTheDocument();
});
