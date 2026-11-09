// @vitest-environment jsdom
import { afterEach, beforeEach, expect, it } from "vitest";
import { onInternalAnchorClick } from "./AppShell";

let anchor: HTMLAnchorElement;
let reached = false;
// Reached only when the capture-phase handler left the click alone (it stops
// propagation when it takes one); also stops jsdom's own navigation attempt.
const verdict = (e: Event) => { reached = true; e.preventDefault(); };
beforeEach(() => {
  anchor = document.createElement("a");
  document.body.appendChild(anchor);
  document.addEventListener("click", onInternalAnchorClick, true);
  document.addEventListener("click", verdict);
});
afterEach(() => {
  document.removeEventListener("click", onInternalAnchorClick, true);
  document.removeEventListener("click", verdict);
  anchor.remove();
  window.history.replaceState(null, "", "/");
});

/** Whether the handler took the click. */
function click(href: string, init: MouseEventInit = {}) {
  anchor.setAttribute("href", href);
  reached = false;
  anchor.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, button: 0, ...init }));
  return !reached;
}

it("a plain click on a chat link while on /chats switches in place and stops the link", () => {
  window.history.replaceState(null, "", "/chats");
  expect(click("/chats?id=abc")).toBe(true);
  expect(window.location.pathname + window.location.search).toBe("/chats?id=abc");
});

it.each([
  ["meta", { metaKey: true }],
  ["ctrl", { ctrlKey: true }],
  ["shift", { shiftKey: true }],
  ["middle button", { button: 1 }],
])("a %s click keeps the anchor's own behaviour", (_label, init) => {
  window.history.replaceState(null, "", "/chats");
  expect(click("/chats?id=abc", init)).toBe(false);
  expect(window.location.search).toBe("");
});

it("leaves other internal links to the router", () => {
  window.history.replaceState(null, "", "/chats");
  expect(click("/home")).toBe(false);
  expect(window.location.pathname).toBe("/chats");
});
