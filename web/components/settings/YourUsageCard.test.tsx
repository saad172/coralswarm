// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import type { BillingMe } from "@/lib/api";
import { YourUsageView } from "./YourUsageCard";

vi.mock("next/link", () => ({
  default: ({ children, href }: { children: React.ReactNode; href: string }) => <a href={href}>{children}</a>,
}));
vi.mock("@/lib/providers", () => ({ useApp: () => ({}) }));

afterEach(cleanup);

function data(over: Partial<BillingMe> = {}): BillingMe {
  return {
    account_kind: "personal",
    plan: { code: "pro", is_free: false },
    plan_credits: { used: 1200, total: 5000, remaining: 3800, period_start: "2026-10-01T00:00:00Z", resets_at: "2026-11-01T00:00:00Z" },
    extra_usage: { status: "unavailable", scope: null, used_credits: 0, limit_credits: null, percent_used: null },
    blocked: { blocked: false, reason: null },
    ...over,
  };
}

const ALL_REASONS = [
  "free_plan",
  "credits_frozen",
  "extra_usage_off",
  "member_cap_reached",
  "monthly_limit_reached",
  "extra_balance_exhausted",
] as const;

describe("YourUsageView", () => {
  it("team member: credits meter, reset date, member cap meter", () => {
    const { container } = render(
      <YourUsageView
        data={data({
          account_kind: "team_org",
          extra_usage: { status: "on", scope: "member_cap", used_credits: 100, limit_credits: 400, percent_used: 25 },
        })}
      />,
    );
    expect(screen.getByText("1,200 of 5,000 credits used")).toBeInTheDocument();
    expect(screen.getByText("Resets November 1, 2026")).toBeInTheDocument();
    expect(screen.getByRole("progressbar", { name: "Plan credits used" })).toHaveAttribute("aria-valuenow", "1200");
    expect(screen.getByRole("progressbar", { name: "Your extra usage cap" })).toHaveAttribute("aria-valuemax", "400");
    expect(screen.queryByText(/New work is paused/)).toBeNull();
    expect(container.textContent).not.toContain("$");
  });

  it("personal Pro with a monthly limit", () => {
    render(
      <YourUsageView
        data={data({ extra_usage: { status: "on", scope: "monthly_limit", used_credits: 50, limit_credits: 1000, percent_used: 5 } })}
      />,
    );
    expect(screen.getByRole("progressbar", { name: "Monthly extra usage limit" })).toBeInTheDocument();
  });

  it("extra usage unavailable renders nothing about it; off renders one line", () => {
    const { rerender } = render(<YourUsageView data={data()} />);
    expect(screen.queryByText(/extra usage/i)).toBeNull();
    rerender(<YourUsageView data={data({ extra_usage: { status: "off", scope: null, used_credits: 0, limit_credits: null, percent_used: null } })} />);
    expect(screen.getByText(/Extra usage is off/)).toBeInTheDocument();
  });

  it("Free plan with credits", () => {
    render(<YourUsageView data={data({ plan: { code: "free", is_free: true }, plan_credits: { used: 10, total: 100, remaining: 90, period_start: "2026-10-01T00:00:00Z", resets_at: "2026-11-01T00:00:00Z" } })} />);
    expect(screen.getByText("10 of 100 credits used")).toBeInTheDocument();
  });

  it("null plan_credits shows plain copy, not zeros", () => {
    const { container } = render(<YourUsageView data={data({ plan_credits: null })} />);
    expect(screen.getByText("Your allowance starts with your first billing period.")).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).toBeNull();
    expect(container.textContent).not.toMatch(/\b0\b/);
  });

  for (const reason of ALL_REASONS) {
    it(`blocked ${reason}: team member is told to ask the admin`, () => {
      const { container } = render(<YourUsageView data={data({ account_kind: "team_org", blocked: { blocked: true, reason } })} />);
      expect(screen.getByText("New work is paused.")).toBeInTheDocument();
      expect(container.textContent).toContain("Ask your organization admin");
      expect(screen.queryByRole("link")).toBeNull();
      expect(container.textContent).not.toContain("$");
    });

    it(`blocked ${reason}: personal user gets a Billing link`, () => {
      const { container } = render(<YourUsageView data={data({ blocked: { blocked: true, reason } })} />);
      expect(screen.getByRole("link", { name: "Go to Billing" })).toHaveAttribute("href", "/settings/billing");
      expect(container.textContent).not.toContain("$");
    });
  }
});
