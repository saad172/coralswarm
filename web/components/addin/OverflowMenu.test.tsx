// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { OverflowMenu, Section } from "./kit";

afterEach(cleanup);

const click = (el: Element) => { fireEvent.mouseDown(el); fireEvent.click(el); };
/** Browsers turn Enter on a focused <button> into a click; jsdom does not. */
const pressEnter = (el: HTMLElement) => { if (fireEvent.keyDown(el, { key: "Enter" })) fireEvent.click(el); };
const press = (key: string) => fireEvent.keyDown(document.activeElement ?? document.body, { key });

function setup() {
  const onChange = vi.fn();
  const onDisconnect = vi.fn();
  render(
    <div>
      <p>outside</p>
      <Section>
        <OverflowMenu label="Account menu" header="ada@acme.test" subheader="Acme"
          items={[{ label: "Change workspace", onSelect: onChange }, { label: "Disconnect", onSelect: onDisconnect, destructive: true }]} />
      </Section>
    </div>,
  );
  return { onChange, onDisconnect, button: screen.getByRole("button", { name: "Account menu" }) };
}

describe("OverflowMenu", () => {
  it("renders the menu outside the clipping Section, in document.body, and items are clickable", () => {
    const { onDisconnect, button } = setup();
    click(button);
    const menu = screen.getByRole("menu");
    expect(menu).toBeVisible();
    expect(menu.parentElement).toBe(document.body);
    expect(menu.closest("section")).toBeNull();
    click(screen.getByRole("menuitem", { name: "Disconnect" }));
    expect(onDisconnect).toHaveBeenCalledOnce();
    expect(screen.queryByRole("menu")).toBeNull();
  });

  it("opens with Enter, focuses the first item, and arrows move between items", () => {
    const { button } = setup();
    button.focus();
    pressEnter(button);
    expect(button).toHaveAttribute("aria-expanded", "true");
    const [first, second] = screen.getAllByRole("menuitem");
    expect(first).toHaveFocus();
    press("ArrowDown");
    expect(second).toHaveFocus();
    press("ArrowDown");
    expect(first).toHaveFocus();
    press("ArrowUp");
    expect(second).toHaveFocus();
  });

  it("closes on Escape and returns focus to the button", () => {
    const { button } = setup();
    click(button);
    press("Escape");
    expect(screen.queryByRole("menu")).toBeNull();
    expect(button).toHaveAttribute("aria-expanded", "false");
    expect(button).toHaveFocus();
  });

  it("closes on an outside click but not on a click inside the menu", () => {
    const { button } = setup();
    click(button);
    fireEvent.mouseDown(screen.getByRole("menu"));
    expect(screen.getByRole("menu")).toBeInTheDocument();
    click(screen.getByText("outside"));
    expect(screen.queryByRole("menu")).toBeNull();
  });

  it("closes when the pane scrolls", () => {
    const { button } = setup();
    click(button);
    fireEvent.scroll(window);
    expect(screen.queryByRole("menu")).toBeNull();
  });

  it("Tab and Shift+Tab move focus to the trigger (not prevented) and close the menu", () => {
    const { button } = setup();
    for (const shiftKey of [false, true]) {
      click(button);
      const item = screen.getAllByRole("menuitem")[0];
      expect(item).toHaveFocus();
      const notPrevented = fireEvent.keyDown(item, { key: "Tab", shiftKey });
      expect(notPrevented).toBe(true);
      expect(button).toHaveFocus();
      expect(screen.queryByRole("menu")).toBeNull();
    }
  });

  it("caps the menu to the viewport and lets it scroll", () => {
    const { button } = setup();
    Object.defineProperty(window, "innerHeight", { configurable: true, value: 200 });
    const scrollHeight = vi.spyOn(HTMLElement.prototype, "scrollHeight", "get").mockReturnValue(900);
    try {
      click(button);
      const menu = screen.getByRole("menu");
      expect(menu.style.maxHeight).toBe("184px");
      expect(menu).toHaveClass("overflow-y-auto");
      expect(menu.style.top).toBe("8px");
    } finally {
      scrollHeight.mockRestore();
      Object.defineProperty(window, "innerHeight", { configurable: true, value: 768 });
    }
  });
});
