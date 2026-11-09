// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ConnectorStatus, ConnectorSummary } from "@/lib/connectorsApi";
import { ConnectorCard } from "./ConnectorCard";

vi.mock("next/link", () => ({
  default: ({ href, children, ...rest }: { href: string; children: React.ReactNode }) => (
    <a href={href} {...rest}>
      {children}
    </a>
  ),
}));

afterEach(cleanup);

const NOW = new Date("2026-10-29T12:00:00Z");

function connector(over: Partial<ConnectorSummary> = {}): ConnectorSummary {
  return {
    key: "microsoft",
    name: "Microsoft",
    beta: false,
    status: "active",
    sources_on: 3,
    sources_total: 5,
    last_captured_at: "2026-10-29T11:57:00Z",
    importing: null,
    problem: null,
    resume_step: null,
    sources: [],
    ...over,
  };
}

function renderCard(c: ConnectorSummary, extra: { recommended?: boolean } = {}) {
  const onConnect = vi.fn();
  const view = render(<ConnectorCard connector={c} onConnect={onConnect} now={NOW} {...extra} />);
  return { onConnect, ...view };
}

describe("per status", () => {
  it("not_connected: what it takes, no freshness, Connect calls onConnect", () => {
    const { onConnect } = renderCard(connector({ key: "slack", name: "Slack", status: "not_connected", sources_on: 0, sources_total: 1 }));
    expect(screen.getByText("Not connected")).toBeInTheDocument();
    expect(screen.getByText("Messages and threads in channels you pick. Never DMs.")).toBeInTheDocument();
    expect(screen.queryByRole("progressbar")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "Connect Slack" }));
    expect(onConnect).toHaveBeenCalledExactlyOnceWith("slack");
  });

  it("setting_up: where it stopped; Continue setup is primary and calls onConnect", () => {
    const { onConnect } = renderCard(connector({ status: "setting_up", sources_on: 2, resume_step: "scope" }));
    expect(screen.getByText("Setting up")).toBeInTheDocument();
    expect(screen.getByText("2 of 5 sources chosen · scope not set yet")).toBeInTheDocument();
    const btn = screen.getByRole("button", { name: "Continue setup Microsoft" });
    expect(btn.className).toContain("bg-ink");
    fireEvent.click(btn);
    expect(onConnect).toHaveBeenCalledExactlyOnceWith("microsoft");
  });

  it("active: freshness line, and Manage is a link to the detail page", () => {
    renderCard(connector());
    expect(screen.getByText("Active")).toBeInTheDocument();
    expect(screen.getByText("3 of 5 sources · last captured 3 min ago")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Manage Microsoft" })).toHaveAttribute("href", "/connectors/microsoft");
    expect(screen.queryByRole("button")).toBeNull();
  });

  it("importing: a determinate Meter and Manage", () => {
    renderCard(connector({ status: "importing", importing: { source: "mail", window_days: 30, percent: 18 } }));
    expect(screen.getByText("Importing history")).toBeInTheDocument();
    expect(screen.getByText("3 of 5 sources · importing 30 days · 18%")).toBeInTheDocument();
    const bar = screen.getByRole("progressbar", { name: "Importing Microsoft history" });
    expect(bar).toHaveAttribute("aria-valuenow", "18");
    expect(screen.getByRole("link", { name: "Manage Microsoft" })).toBeInTheDocument();
  });

  it("importing with no percent: an indeterminate Meter", () => {
    renderCard(connector({ status: "importing", importing: { source: "mail", window_days: 30, percent: null } }));
    expect(screen.getByRole("progressbar")).not.toHaveAttribute("aria-valuenow");
  });

  it("needs_attention: the problem sentence replaces the freshness line; Manage", () => {
    renderCard(
      connector({
        status: "needs_attention",
        problem: { code: "tenant_consent_required", source: null, since: "2026-10-27T12:00:00Z", params: {} },
      }),
    );
    expect(screen.getByText("Needs attention")).toBeInTheDocument();
    expect(screen.getByText(/hasn't approved CoralSwarm/)).toBeInTheDocument();
    expect(screen.queryByText(/last captured/)).toBeNull();
    expect(screen.getByRole("link", { name: "Manage Microsoft" })).toBeInTheDocument();
  });

  it("reconnect_needed: the problem sentence; Reconnect is primary and calls onConnect", () => {
    const { onConnect } = renderCard(
      connector({
        status: "reconnect_needed",
        problem: { code: "credential_invalid", source: null, since: "2026-10-27T12:00:00Z", params: { reason: "ms_grant_expired" } },
      }),
    );
    expect(screen.getByText("Reconnect needed")).toBeInTheDocument();
    expect(screen.getByText("Your Microsoft sign-in expired on Oct 27. Reconnect to resume.")).toBeInTheDocument();
    const btn = screen.getByRole("button", { name: "Reconnect Microsoft" });
    expect(btn.className).toContain("bg-ink");
    fireEvent.click(btn);
    expect(onConnect).toHaveBeenCalledExactlyOnceWith("microsoft");
  });

  it("the pill carries a literal tone class per status (an interpolated one is purged from the CSS)", () => {
    const tones: Record<ConnectorStatus, string> = {
      not_connected: "verdict-muted",
      setting_up: "verdict-muted",
      active: "verdict-ok",
      importing: "verdict-ok",
      needs_attention: "verdict-warn",
      reconnect_needed: "verdict-bad",
    };
    for (const [status, cls] of Object.entries(tones)) {
      const { container, unmount } = renderCard(connector({ status: status as ConnectorStatus }));
      expect(container.querySelector(".verdict")).toHaveClass(cls);
      unmount();
    }
  });

  it("every status renders its pill word", () => {
    const words: Record<ConnectorStatus, string> = {
      not_connected: "Not connected",
      setting_up: "Setting up",
      active: "Active",
      importing: "Importing history",
      needs_attention: "Needs attention",
      reconnect_needed: "Reconnect needed",
    };
    for (const [status, word] of Object.entries(words)) {
      const { unmount } = renderCard(connector({ status: status as ConnectorStatus }));
      expect(screen.getByText(word)).toBeInTheDocument();
      unmount();
    }
  });
});

describe("Beta, recommended, naming", () => {
  it("a Beta toolkit carries the Beta tag and never says Composio", () => {
    const { container } = renderCard(connector({ key: "jira", name: "Jira", beta: true, status: "not_connected", sources_on: 0, sources_total: 1 }));
    expect(screen.getByText("Beta")).toBeInTheDocument();
    expect(container.textContent?.toLowerCase()).not.toContain("composio");
  });

  it("a non-Beta connector has no Beta tag", () => {
    renderCard(connector());
    expect(screen.queryByText("Beta")).toBeNull();
  });

  it("recommended: Start here tag, ink border, brand button", () => {
    const { container } = renderCard(connector({ status: "not_connected", sources_on: 0 }), { recommended: true });
    expect(screen.getByText("Start here")).toBeInTheDocument();
    expect(container.querySelector("[data-connector]")?.className).toContain("border-ink");
    expect(screen.getByRole("button", { name: "Connect Microsoft" }).className).toContain("bg-brand");
  });

  it("not recommended: no tag, line border, secondary button", () => {
    const { container } = renderCard(connector({ status: "not_connected", sources_on: 0 }));
    expect(screen.queryByText("Start here")).toBeNull();
    expect(container.querySelector("[data-connector]")?.className).toContain("border-line");
    expect(screen.getByRole("button", { name: "Connect Microsoft" }).className).not.toContain("bg-brand");
  });

  it("an unknown connector key still renders, with its name and a letter mark", () => {
    const { container } = renderCard(connector({ key: "newtool", name: "NewTool", status: "not_connected", sources_on: 0 }));
    expect(within(container).getByText("NewTool")).toBeInTheDocument();
    expect(container.textContent).toContain("N");
  });
});
