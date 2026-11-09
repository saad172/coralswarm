// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { OptionTile } from "./OptionTile";

afterEach(cleanup);

function Group({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <div role="radiogroup" aria-label="History">
      <OptionTile name="h" value="30d" checked={value === "30d"} onChange={onChange} title="Last 30 days" hint="Recent" />
      <OptionTile name="h" value="all" checked={value === "all"} onChange={onChange} title="Everything" />
    </div>
  );
}

it("exposes radio semantics and marks only the selected tile", () => {
  render(<Group value="30d" onChange={() => {}} />);
  expect(screen.getByRole("radiogroup", { name: "History" })).toBeInTheDocument();
  expect(screen.getByRole("radio", { name: /Last 30 days/ })).toBeChecked();
  expect(screen.getByRole("radio", { name: /Everything/ })).not.toBeChecked();
  expect(screen.getByText("Last 30 days").closest("label")).toHaveClass("border-brand", "bg-brand-soft");
  expect(screen.getByText("Everything").closest("label")).not.toHaveClass("border-brand");
});

it("reports the chosen value", () => {
  const onChange = vi.fn();
  render(<Group value="30d" onChange={onChange} />);
  fireEvent.click(screen.getByRole("radio", { name: /Everything/ }));
  expect(onChange).toHaveBeenCalledExactlyOnceWith("all");
});
