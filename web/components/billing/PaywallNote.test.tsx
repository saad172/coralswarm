// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";

let role: "personal" | "org-member" | "org-admin" = "org-admin";
let loaded = true;
vi.mock("@/components/settings/useSettingsRole", () => ({ useSettingsRole: () => ({ role, loaded }) }));
vi.mock("next/link", () => ({
  default: ({ children, href }: { children: React.ReactNode; href: string }) => <a href={href}>{children}</a>,
}));

import { PaywallNote } from "./PaywallNote";

afterEach(() => {
  cleanup();
  role = "org-admin";
  loaded = true;
});

describe("PaywallNote", () => {
  it("keeps the original generic copy when the backend sends no reason", () => {
    render(<PaywallNote />);
    expect(screen.getByRole("alert")).toHaveTextContent("You're out of credits for this billing period. Email person@example.com to get more.");
    render(<PaywallNote reason="from_the_future" />);
    expect(screen.getAllByRole("alert")).toHaveLength(2);
  });

  it("shows an admin the reason and a link to org billing", () => {
    render(<PaywallNote reason="extra_usage_off" />);
    expect(screen.getByRole("alert")).toHaveTextContent("extra usage is off");
    expect(screen.getByRole("link", { name: "Turn on extra usage" })).toHaveAttribute("href", "/settings/organization/billing");
  });

  it("shows a personal account a link to its own billing page", () => {
    role = "personal";
    render(<PaywallNote reason="free_plan" />);
    expect(screen.getByRole("link", { name: "Upgrade" })).toHaveAttribute("href", "/settings/billing");
  });

  it("tells a member to ask an admin, with no link", () => {
    role = "org-member";
    render(<PaywallNote reason="monthly_limit_reached" />);
    expect(screen.getByRole("alert")).toHaveTextContent("Ask an admin to raise the monthly limit.");
    expect(screen.queryByRole("link")).toBeNull();
  });

  it("uses the member wording until the role has loaded", () => {
    loaded = false;
    render(<PaywallNote reason="extra_balance_exhausted" />);
    expect(screen.queryByRole("link")).toBeNull();
    expect(screen.getByRole("alert")).toHaveTextContent("Ask an admin to add funds.");
  });
});
