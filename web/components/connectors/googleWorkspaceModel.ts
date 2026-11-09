import type { OAuthConnection } from "@/lib/api";
import { calendarConnectionFrom } from "./calendarModel";
import { gmailConnectionFrom } from "./gmailModel";
import { calendarConnectError } from "./calendarModel";
import { gmailConnectError } from "./gmailModel";
import { meetConnectError, meetConnectionFrom } from "./meetModel";

/* Pure decisions behind the Google Workspace card on /connectors, which
 * replaced the four separate Google tiles.
 *
 * One consent screen asks for every enabled Google surface at once; the
 * backend then stores a connection for each surface the member actually
 * ticked (`connectors/google_workspace.rs`). Each surface keeps its own row
 * (`google`, `google_gmail`, `google_chat`, `google_meet`), so the
 * card's rows keep their own status, Disconnect, and Connect. */

export const WORKSPACE_SURFACE = "workspace";

export type GoogleSurface = "calendar" | "gmail" | "chat" | "meet";

/** Stable display order, matching the backend's `Surface::ALL`. */
export const GOOGLE_SURFACES: readonly GoogleSurface[] = ["calendar", "gmail", "chat", "meet"];

export const SURFACE_LABEL: Record<GoogleSurface, string> = {
  calendar: "Google Calendar",
  gmail: "Gmail",
  chat: "Google Chat",
  meet: "Google Meet",
};

const CHAT_PROVIDER = "google_chat";

/** `GET /v1/oauth/google/workspace`: which surfaces this workspace may connect. */
export type WorkspaceSurfaceFlags = { surface: GoogleSurface; enabled: boolean }[];

export type SurfaceStatus = { surface: GoogleSurface; enabled: boolean; connected: boolean };

export function isGoogleSurface(s: string): s is GoogleSurface {
  return (GOOGLE_SURFACES as readonly string[]).includes(s);
}

/** Per-surface status, using each tile's own "is this connected" rule so the
 *  button and the tiles can never disagree. */
export function surfaceStatuses(
  connections: OAuthConnection[],
  flags: WorkspaceSurfaceFlags | null,
  connectionsAccountId: string | null = null,
  accountId: string | null = null,
): SurfaceStatus[] {
  const guarded = connectionsAccountId !== null && connectionsAccountId !== accountId ? [] : connections;
  const connected: Record<GoogleSurface, boolean> = {
    calendar: calendarConnectionFrom(guarded) !== null,
    gmail: gmailConnectionFrom(guarded) !== null,
    chat: guarded.some((c) => c.provider === CHAT_PROVIDER),
    meet: meetConnectionFrom(guarded) !== null,
  };
  return GOOGLE_SURFACES.map((surface) => ({
    surface,
    enabled: flags?.find((f) => f.surface === surface)?.enabled ?? false,
    connected: connected[surface],
  }));
}

/** Show the button only when it could still connect something: at least one
 *  surface is enabled for this workspace AND not yet connected. Unknown flags
 *  (still loading, or the request failed) hide it rather than guess. */
export function shouldShowWorkspaceButton(statuses: SurfaceStatus[]): boolean {
  return statuses.some((s) => s.enabled && !s.connected);
}

/** Friendly text for a failed start. The backend refuses with a 403 BEFORE
 *  any redirect when none of the surfaces is enabled for this workspace. */
export function workspaceConnectError(status: number | null, message: string | null): string {
  if (status === 403) {
    return "Google isn't enabled for this workspace yet. Ask your workspace admin to turn it on, or switch to your personal workspace.";
  }
  return message || "Couldn't start the Google Workspace connection.";
}

export type SkipReason = "unticked" | "disabled";

export type WorkspaceResult = {
  connected: GoogleSurface[];
  skipped: { surface: GoogleSurface; reason: SkipReason }[];
  failed: GoogleSurface[];
};

function splitList(raw: string | null): string[] {
  return (raw ?? "").split(",").map((s) => s.trim()).filter(Boolean);
}

/** The result the backend appends to the `/connectors` redirect
 *  (`google_workspace=1&gws_connected=..&gws_skipped=chat:unticked,..&gws_failed=..`),
 *  or null when this page load is not the return from that flow. Unknown
 *  names are dropped, never guessed. */
export function parseWorkspaceResult(params: { get(name: string): string | null }): WorkspaceResult | null {
  if (params.get("google_workspace") !== "1") return null;
  const connected = splitList(params.get("gws_connected")).filter(isGoogleSurface);
  const failed = splitList(params.get("gws_failed")).filter(isGoogleSurface);
  const skipped: WorkspaceResult["skipped"] = [];
  for (const item of splitList(params.get("gws_skipped"))) {
    const [surface, reason] = item.split(":");
    if (isGoogleSurface(surface) && (reason === "unticked" || reason === "disabled")) {
      skipped.push({ surface, reason });
    }
  }
  return { connected, skipped, failed };
}

export type ResultLine = { tone: "ok" | "muted" | "bad"; text: string };

/** Calm, plain lines for the banner after the redirect. */
export function workspaceResultLines(result: WorkspaceResult): ResultLine[] {
  const names = (list: GoogleSurface[]) => list.map((s) => SURFACE_LABEL[s]).join(", ");
  const lines: ResultLine[] = [];
  if (result.connected.length > 0) {
    lines.push({ tone: "ok", text: `Connected: ${names(result.connected)}.` });
  }
  const unticked = result.skipped.filter((s) => s.reason === "unticked").map((s) => s.surface);
  const disabled = result.skipped.filter((s) => s.reason === "disabled").map((s) => s.surface);
  if (unticked.length > 0) {
    lines.push({
      tone: "muted",
      text: `Skipped, not selected on Google's screen: ${names(unticked)}. Use that row's Connect to add it later.`,
    });
  }
  if (disabled.length > 0) {
    lines.push({
      tone: "muted",
      text: `Not available for this workspace: ${names(disabled)}.`,
    });
  }
  if (result.failed.length > 0) {
    lines.push({
      tone: "bad",
      text: `Couldn't finish: ${names(result.failed)}. Try that row's Connect again.`,
    });
  }
  if (result.connected.length === 0 && result.failed.length === 0) {
    lines.unshift({ tone: "muted", text: "Nothing was connected." });
  }
  return lines;
}

/** Short row names inside the card. */
export const SURFACE_ROW_LABEL: Record<GoogleSurface, string> = {
  calendar: "Calendar",
  gmail: "Gmail",
  chat: "Chat",
  meet: "Meet",
};

/** The `oauth_tokens.provider` literal each surface's row is stored under,
 *  which is what Disconnect keys on. Calendar's is the original `"google"`. */
export const SURFACE_PROVIDER: Record<GoogleSurface, string> = {
  calendar: "google",
  gmail: "google_gmail",
  chat: CHAT_PROVIDER,
  meet: "google_meet",
};

/** Friendly text for a failed single-surface connect. */
export function surfaceConnectError(surface: GoogleSurface, status: number | null, message: string | null): string {
  switch (surface) {
    case "calendar":
      return calendarConnectError(status, message);
    case "gmail":
      return gmailConnectError(status, message);
    case "meet":
      return meetConnectError(status, message);
    case "chat":
      if (status === 403) {
        return "Google Chat isn't enabled for this workspace yet. Ask your workspace admin to turn it on, or switch to your personal workspace to connect Google Chat.";
      }
      return message || "Couldn't start the Google Chat connection.";
  }
}

/** Overall status for the card header. Counts only enabled surfaces; while
 *  the flags are unknown (`flagsKnown` false) every surface counts. */
export function overallStatus(
  statuses: SurfaceStatus[],
  flagsKnown = true,
): { connected: number; total: number; label: string; allConnected: boolean } {
  const counted = flagsKnown ? statuses.filter((s) => s.enabled) : statuses;
  const connected = counted.filter((s) => s.connected).length;
  const total = counted.length;
  if (connected === 0) return { connected, total, label: "not connected", allConnected: false };
  const allConnected = total > 0 && connected === total;
  return {
    connected,
    total,
    label: `${connected} of ${total} connected${allConnected ? " ✓" : ""}`,
    allConnected,
  };
}
