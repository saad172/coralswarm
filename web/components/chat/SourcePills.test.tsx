// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { SourcePills } from "./SourcePills";
import { rankByCitations, type EvidenceSource } from "./evidence";
afterEach(cleanup);
const sources: EvidenceSource[] = Array.from({ length: 5 }, (_, i) => ({
  n: i + 1,
  sourceKey: `g:${i + 1}`,
  title: `Source ${i + 1}`,
  quote: "quote",
  ocean: "Ocean",
  type: "note",
  date: "",
  hue: "teal",
}));
it("aggregates passages, sorts counts then lowest citation, and keeps uncited last", () => {
  expect(
    rankByCitations("[3] [3] [2] [4]", [...sources, { ...sources[0], n: 6 }]).map((s) => s.n),
  ).toEqual([3, 2, 4, 1, 5]);
  expect(rankByCitations("[6] [6] [3]", [...sources, { ...sources[0], n: 6 }])[0].n).toBe(1);
});
it("shows three ranked sources, expands all passage rows, and unmounts collapsed rows", async () => {
  const select = vi.fn();
  render(<SourcePills sources={sources} text="[4] [4] [1]" onSelect={select} />);
  expect(screen.getAllByRole("button", { name: /^Open source/ })).toHaveLength(3);
  expect(screen.queryByRole("button", { name: "Open source 5: Source 5" })).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "+2 more" }));
  fireEvent.click(screen.getByRole("button", { name: "Open source 5: Source 5" }));
  expect(select).toHaveBeenCalledWith(5);
  fireEvent.click(screen.getByRole("button", { name: "show less" }));
  await waitFor(() =>
    expect(screen.queryByRole("button", { name: "Open source 5: Source 5" })).toBeNull(),
  );
});
it("replaces pills with tiles and counts only unmatched sources", () => {
  render(
    <SourcePills
      sources={sources}
      text=""
      onSelect={() => {}}
      tileKeys={["g:1", "g:2", "g:3", "g:4"]}
    >
      <div>Tiles</div>
    </SourcePills>,
  );
  expect(screen.queryByRole("button", { name: /^Open source/ })).toBeNull();
  expect(screen.getByRole("button", { name: "+1 more" })).toBeVisible();
});
it("preserves source one selector for a single source", () => {
  render(<SourcePills sources={sources.slice(0, 1)} text="[1]" onSelect={() => {}} />);
  expect(screen.getByRole("button", { name: /^Open source 1:/ })).toBeVisible();
});

it("shares hover and selected highlighting and includes expanded kind/date", () => {
  const hover = vi.fn();
  render(<SourcePills sources={sources.map(s => ({...s, date:"30 Oct"}))} text="" hot={2} setHot={hover} onSelect={vi.fn()}/>);
  expect(screen.getByRole("button", {name:"Open source 2: Source 2"})).toHaveAttribute("aria-pressed", "true");
  fireEvent.mouseEnter(screen.getByRole("button", {name:"Open source 1: Source 1"}));
  expect(hover).toHaveBeenLastCalledWith(1);
  fireEvent.mouseLeave(screen.getByRole("button", {name:"Open source 1: Source 1"}));
  expect(hover).toHaveBeenLastCalledWith(null);
  fireEvent.click(screen.getByRole("button", {name:"+2 more"}));
  expect(screen.getAllByText("note · 30 Oct")).toHaveLength(5);
});
