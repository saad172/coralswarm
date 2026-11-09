// @vitest-environment jsdom
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { openChatInPlace } from "./newChat";

let pushState: ReturnType<typeof vi.spyOn>;
beforeEach(() => { pushState = vi.spyOn(window.history, "pushState"); });
afterEach(() => { vi.restoreAllMocks(); window.history.replaceState(null, "", "/"); });

it.each([
  ["/chats", "/chats?id=abc"],
  ["/chats/", "/chats/?id=abc"],
])("on %s pushes the chat's id onto the current pathname", (pathname, pushed) => {
  window.history.replaceState(null, "", pathname);
  expect(openChatInPlace("/chats?id=abc")).toBe(true);
  expect(pushState).toHaveBeenCalledWith(null, "", pushed);
  expect(window.location.pathname + window.location.search).toBe(pushed);
});

it("keeps a trailing-slash href on a trailing-slash pathname", () => {
  window.history.replaceState(null, "", "/chats/");
  expect(openChatInPlace("/chats/?id=abc")).toBe(true);
  expect(window.location.pathname + window.location.search).toBe("/chats/?id=abc");
});

it.each([
  ["another route", "/home", "/chats?id=abc"],
  ["a non-chat href", "/chats", "/home?ws=all"],
  ["the bare chats link", "/chats", "/chats"],
])("leaves navigation alone from %s", (_label, pathname, href) => {
  window.history.replaceState(null, "", pathname);
  expect(openChatInPlace(href)).toBe(false);
  expect(pushState).not.toHaveBeenCalled();
});
