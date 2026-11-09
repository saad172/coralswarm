import type { GoogleChatDestination, GoogleChatMailboxOptin, GoogleChatSpace, Graph } from "@/lib/api";

/* Pure decisions behind the Google Chat spaces view (`/connectors/google-chat`)
 * and its entry point on `/connectors` — kept framework-free for the same
 * reason `slackChannelModel.ts` beside it is: this repo's vitest runs a Node
 * environment with no DOM/testing-library (see `vitest.config.ts`'s own
 * comment), so a component that renders these decisions cannot be exercised
 * by a plain `.test.ts` file at all.
 *
 * Three things this module exists to get right — getting any of them wrong
 * is exactly how this connector would ship confusing or silently broken,
 * per the brief's own named traps:
 *
 *  1. A space the CoralSwarm Chat app has never been added to still shows up
 *     in `spaces.list` (it's live and visible under the caller's OWN token) —
 *     it must never read as "ready to route", and picking a destination for
 *     it 404s server-side. `containerSpaceStatus` is the one place that
 *     turns the wire `destination` shape into a status a screen can safely
 *     switch on, so the table and the picker can never disagree about it.
 *  2. An unrouted (app added, no destination chosen) space is the RESTING
 *     state for something just added, and it ingests nothing until routed —
 *     it must read as unmistakably actionable, never as "nothing to do
 *     here" or "finished".
 *  3. `newly_routed: false` on a destination-pick response means the write
 *     was a NO-OP (the space was already routed — the server-side UPDATE is
 *     scoped to `mode IS NULL`) — a screen must never claim to have
 *     re-routed a space that call didn't actually touch, and this screen
 *     never offers the picker for an already-routed space in the first
 *     place (see `containerSpaceStatus`/`containerSortWeight`).
 */

// ── 1. Status, derived only from the wire shape ─────────────────────────────

/** One container (`space_type === "SPACE"`) space's status, derived ONLY
 *  from its `destination` field — never re-derived from `connected`/`active`
 *  directly, so this and `destination_json`'s own four-way switch
 *  (`routes/google_chat.rs`) can never drift apart. */
export type ContainerSpaceStatus = "needs_app" | "unrouted" | "routed_single" | "routed_fanout";

export function containerSpaceStatus(destination: GoogleChatDestination | undefined): ContainerSpaceStatus {
  switch (destination?.kind) {
    case "single":
      return "routed_single";
    case "fanout":
      return "routed_fanout";
    case "unrouted":
      return "unrouted";
    case "not_connected":
    default:
      return "needs_app";
  }
}

/** Only an `"unrouted"` space may be offered the destination picker — a
 *  `"needs_app"` space 404s server-side (trap #2), and an already-routed
 *  space's pick would silently no-op rather than re-route (trap #3, see
 *  `SetGoogleChatDestinationResponse`'s own doc comment in lib/api.ts). */
export function isPickable(status: ContainerSpaceStatus): boolean {
  return status === "unrouted";
}

/** Sort weight for the container table — the one ACTIONABLE-HERE state
 *  (unrouted: needs a pick, right on this screen) surfaces first, then the
 *  state actionable elsewhere (needs the app added, in Google Chat itself),
 *  then already-routed spaces last, since there is nothing left to do with
 *  them. */
export function containerSortWeight(status: ContainerSpaceStatus): number {
  switch (status) {
    case "unrouted":
      return 0;
    case "needs_app":
      return 1;
    case "routed_single":
    case "routed_fanout":
      return 2;
  }
}

export function sortContainerSpaces(spaces: GoogleChatSpace[]): GoogleChatSpace[] {
  return [...spaces].sort((a, b) => {
    const wa = containerSortWeight(containerSpaceStatus(a.destination));
    const wb = containerSortWeight(containerSpaceStatus(b.destination));
    if (wa !== wb) return wa - wb;
    return (a.display_name ?? "").localeCompare(b.display_name ?? "");
  });
}

/** Split the merged `spaces` list into the two archetypes the picker renders
 *  separately (`spaces_response`'s own per-`space_type` split) — the
 *  container archetype's app-install/destination-pick state vs. the mailbox
 *  archetype's per-member opt-in, which share no field worth a table column. */
export function splitByArchetype(spaces: GoogleChatSpace[]): {
  containers: GoogleChatSpace[];
  mailboxes: GoogleChatSpace[];
} {
  return {
    containers: spaces.filter((s) => s.space_type === "SPACE"),
    mailboxes: spaces.filter((s) => s.space_type !== "SPACE"),
  };
}

// ── 2. Destination copy ──────────────────────────────────────────────────────

/** The routed destination's ocean name, or `null` when there is none to
 *  show — unrouted, needs-the-app, or a `single` pick whose `graph_id` no
 *  longer resolves to an ocean this account can currently see. */
export function singleDestinationOceanName(
  destination: GoogleChatDestination | undefined,
  oceans: Graph[],
): string | null {
  if (destination?.kind !== "single") return null;
  return oceans.find((g) => g.id === destination.graph_id)?.name ?? null;
}

/** The label the table renders for a container row's CURRENT destination —
 *  never claims more than `destination` backs (see `singleDestinationOceanName`
 *  for the one case that has to fall back to a generic name rather than
 *  inventing one). */
export function containerDestinationLabel(
  destination: GoogleChatDestination | undefined,
  oceans: Graph[],
): string {
  switch (containerSpaceStatus(destination)) {
    case "needs_app":
      return "add the app first";
    case "unrouted":
      return "not routed yet";
    case "routed_fanout":
      return "everyone's individual workspaces";
    case "routed_single":
      return singleDestinationOceanName(destination, oceans) ?? "a workspace";
  }
}

// ── 3. The 404-on-pick trap ──────────────────────────────────────────────────

/** Whether a Chat destination-pick failure is a 404 this screen must render
 *  as actionable guidance (brief's trap #2) rather than a generic error
 *  note. `pick_error_response` (`routes/google_chat.rs`) answers 404 for
 *  BOTH `PickError::NotConnected` ("space not connected — add the
 *  CoralSwarm Chat app to it first" — the app was never added, or was
 *  removed since this list loaded) and `PickError::GraphNotOwned` (the
 *  target ocean vanished between load and pick) — this client does not need
 *  to tell the two apart to know the right treatment: render the backend's
 *  own message verbatim in an actionable panel, offer a refresh, and never
 *  claim the pick "failed" generically. Keyed on STATUS alone, never on
 *  message text — the exact wording is a copy detail the backend owns. */
export function pickFailedActionable(status: number | null): boolean {
  return status === 404;
}

/** Whether THIS caller is actively watching a mailbox-archetype space.
 *  `mailbox_optin_json` retains an opted-out row as `{ opted_in: true,
 *  active: false }` — that is inactive, not "in your ocean". Only both
 *  `opted_in` and `active` means ingest is on. */
export function isMailboxOptinActive(optin: GoogleChatMailboxOptin | undefined): boolean {
  return optin?.opted_in === true && optin.active === true;
}

// ── 4. Empty / not-connected copy ────────────────────────────────────────────

/** Copy for "connected, but zero live spaces" — the state a fresh connect
 *  actually lands on today (production has zero watched spaces and the
 *  founder will see this first). Must teach the SAME ordering the connect
 *  card already stated — connect first, then add the app — never just say
 *  "no spaces found", which teaches nothing about what to do next. */
export function emptySpacesCopy(): { title: string; hint: string } {
  return {
    title: "No Google Chat spaces yet",
    hint:
      "Open a space in Google Chat, then use its “Apps” menu to add the CoralSwarm app to " +
      "it. Come back here afterward to choose where that space's messages land — nothing ingests " +
      "until you pick a destination.",
  };
}

/** Copy for "Google Chat isn't connected for this member yet" — the 404
 *  `googleChatSpaces` itself answers before there is any token to list
 *  spaces with. Distinct from `emptySpacesCopy` (that state IS connected,
 *  just empty) so the two can never be confused on screen. */
export function notConnectedCopy(): { title: string; hint: string } {
  return {
    title: "Google Chat isn't connected",
    hint: "Connect Google Chat first — from the Connectors page — then come back here to see your spaces.",
  };
}

/** Workspace-admin instruction for the Chat connect card — plan item 35.
 *
 *  Chat itself never returns an email on any resource. The directory that
 *  does (`people.listDirectoryPeople`) only answers when a Workspace admin
 *  has enabled external contact and profile sharing and the grant includes
 *  `directory.readonly`. A successful response with no entries is ambiguous:
 *  sharing may be disabled, the grant may lack `directory.readonly`, or the
 *  directory may be genuinely empty. A rejected request, including a 403,
 *  is reported as unavailable — it is not an empty directory. Reconnecting
 *  this member's grant will not turn sharing on. So the connect flow has to
 *  say so, in both the connected and the not-connected states. */
export function profileSharingInstruction(): { label: string; body: string } {
  return {
    label: "workspace admin",
    body:
      "Google Chat never returns an email address. Matching people in a space needs a Workspace admin " +
      "to turn on external contact and profile sharing: in the Admin console, open Directory → Directory " +
      "settings → Sharing settings, enable contact sharing, and set External directory sharing to " +
      "organization data (not only the signed-in user's basic profile). Until that is on, CoralSwarm " +
      "cannot look up the directory. Reconnecting here will not turn it on.",
  };
}
