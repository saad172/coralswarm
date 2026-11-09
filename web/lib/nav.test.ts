import { expect, it } from "vitest";
import { IconCurrents } from "@/components/icons";
import { ADD, MAIN, TITLES, UTILITIES, isActive } from "./nav";

it("the rail is Home, Chats, Today, Workspaces, in that order", () => {
  expect(MAIN.map((i) => i.label)).toEqual(["Home", "Chats", "Today", "Workspaces"]);
  expect(MAIN.map((i) => i.href)).toEqual(["/home", "/chats", "/today", "/workspaces"]);
});

it("Home uses the currents glyph (the design system has no home icon)", () => {
  expect(MAIN[0].Icon).toBe(IconCurrents);
});

it("Library is gone from every nav list", () => {
  const all = [...MAIN, ...UTILITIES, ADD];
  expect(all.some((i) => i.href.startsWith("/library") || /library/i.test(i.label))).toBe(false);
  expect(Object.keys(TITLES)).not.toContain("library");
});

it("utilities are Connectors and Settings", () => {
  expect(UTILITIES.map((i) => i.href)).toEqual(["/connectors", "/settings"]);
});

it("header titles derive from the nav, so Home is titled", () => {
  expect(TITLES.home).toBe("Home");
  expect(TITLES.chats).toBe("Chats");
});

it("isActive compares whole segments and tolerates trailing slashes", () => {
  expect(isActive("/home", "/home")).toBe(true);
  expect(isActive("/home/", "/home")).toBe(true);
  expect(isActive("/homework", "/home")).toBe(false);
  expect(isActive("/workspaces/manage", "/workspaces")).toBe(true);
  expect(isActive(null, "/home")).toBe(false);
});
