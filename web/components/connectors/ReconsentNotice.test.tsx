// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { OAuthConnection } from "@/lib/api";
import { ReconsentNotice } from "./ReconsentNotice";

afterEach(() => {
  cleanup();
});

function connection(overrides: Partial<OAuthConnection> = {}): OAuthConnection {
  return {
    provider: "slack",
    provider_workspace_id: "T1",
    scope: "channels:read users:read users:read.email",
    expires_at: null,
    connected_at: "2026-09-01T00:00:00Z",
    missing_scopes: [],
    scopes_unknown: false,
    ...overrides,
  };
}

describe("ReconsentNotice — shown / hidden / unknown (CS-431)", () => {
  it("renders nothing for a null connection", () => {
    const { container } = render(
      <ReconsentNotice connection={null} onReconnect={() => {}} reconnecting={false} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("renders nothing when nothing is missing", () => {
    const { container } = render(
      <ReconsentNotice connection={connection()} onReconnect={() => {}} reconnecting={false} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  /// FAILS WITHOUT THE FIX: "never guess" means an unknown scope set must
  /// never render a prompt, even if the backend ever sent a non-empty
  /// `missing_scopes` alongside `scopes_unknown: true`.
  it("renders nothing when the stored scope set is unknown", () => {
    const { container } = render(
      <ReconsentNotice
        connection={connection({ scopes_unknown: true, missing_scopes: ["users:read.email"] })}
        onReconnect={() => {}}
        reconnecting={false}
      />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("names the missing scope and what it unlocks, with a working Reconnect button", () => {
    const onReconnect = vi.fn();
    render(
      <ReconsentNotice
        connection={connection({ missing_scopes: ["users:read.email"] })}
        onReconnect={onReconnect}
        reconnecting={false}
      />,
    );
    expect(screen.getByText("users:read.email")).toBeInTheDocument();
    expect(screen.getByText(/match colleagues by email/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Reconnect" }));
    expect(onReconnect).toHaveBeenCalledOnce();
  });

  it("names Google Chat's scope by its last path segment, not the full URL", () => {
    render(
      <ReconsentNotice
        connection={connection({
          provider: "google_chat",
          missing_scopes: ["https://www.googleapis.com/auth/directory.readonly"],
        })}
        onReconnect={() => {}}
        reconnecting={false}
      />,
    );
    expect(screen.getByText("directory.readonly")).toBeInTheDocument();
    expect(screen.queryByText(/googleapis\.com/)).not.toBeInTheDocument();
  });

  /// Reconnecting a Slack workspace-wide install is the same workspace-level
  /// act Connect/Disconnect already gate on `adminOnly` for
  /// (`ProviderTile`'s own "admins only" states) — a plain member must see
  /// WHY the connection is stale without a button that would bounce off
  /// Slack's own permission gate.
  it("replaces the Reconnect button with 'admins only' when adminOnly is set", () => {
    render(
      <ReconsentNotice
        connection={connection({ missing_scopes: ["users:read.email"] })}
        onReconnect={() => {}}
        reconnecting={false}
        adminOnly
      />,
    );
    expect(screen.queryByRole("button", { name: "Reconnect" })).not.toBeInTheDocument();
    expect(screen.getByText("admins only")).toBeInTheDocument();
  });

  it("shows a redirecting label while reconnecting", () => {
    render(
      <ReconsentNotice
        connection={connection({ missing_scopes: ["users:read.email"] })}
        onReconnect={() => {}}
        reconnecting
      />,
    );
    expect(screen.getByRole("button", { name: "Redirecting…" })).toBeDisabled();
  });
});

describe("ReconsentNotice — a revoked credential", () => {
  /// A sync pass recorded a definite revocation on the row
  /// (`credential_invalid_at`): the connection is dead until a reconnect,
  /// even though no scope is missing. Personal Google Calendar has no other
  /// surface for this — the admin `connector_health` record is for shared
  /// capabilities only.
  it("tells the user to reconnect, with a working Reconnect button", () => {
    const onReconnect = vi.fn();
    render(
      <ReconsentNotice
        connection={connection({
          provider: "google",
          scope: "openid email https://www.googleapis.com/auth/calendar.readonly",
          credential_invalid_at: "2026-10-07T09:00:00Z",
          credential_invalid_reason: "google_invalid_grant",
        })}
        onReconnect={onReconnect}
        reconnecting={false}
      />,
    );
    expect(screen.getByText(/stopped working/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Reconnect" }));
    expect(onReconnect).toHaveBeenCalledTimes(1);
  });

  it("renders nothing for a live row with nothing missing", () => {
    const { container } = render(
      <ReconsentNotice
        connection={connection({ credential_invalid_at: null, credential_invalid_reason: null })}
        onReconnect={() => {}}
        reconnecting={false}
      />,
    );
    expect(container).toBeEmptyDOMElement();
  });
});
