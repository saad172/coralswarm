import type { SlackChannelRow, SlackChannelsResponse } from "@/lib/api";

/* The ONE client derivation of the Slack channel-management payload's prose.
 *
 * Split out of SlackChannelTable.tsx for the same reason
 * slackOnboardingModel.ts is split out of the checklist: these are the
 * sentences that told a founder his org's Slack connector was healthy while
 * it was fanning content into a single person's private ocean (2026-09-09),
 * and a sentence that can be wrong deserves tests more than a `<td>` does.
 * vitest here has no React renderer (see vitest.config.ts), so pure model
 * functions are also the only shape these rules CAN be tested in.
 *
 * ── Two tenses, kept apart ──
 * `destinationView` answers "where does the NEXT message go". `landedView`
 * answers "where are the corals that already arrived". They are separate
 * functions because they are separate facts that legitimately disagree:
 * re-routing a channel never moves already-captured corals, and an org
 * fan-out that reaches nobody has a destination and no landings at all.
 * Collapsing them into one column is the conflation the old single "lands in"
 * cell was.
 *
 * ── Never claim more than the payload backs ──
 * Every branch below is reachable from a field the server actually sends. In
 * particular there is no "your account default" string anywhere: that phrase
 * described the personal plane and was simply false on the org plane, where
 * the default is a live fan-out to each MAPPED member's private ocean and the
 * shared org ocean receives nothing. When the server declines to say how far
 * a fan-out reaches (an org member, who may not be told roster counts), this
 * says less — it never fills the gap with a guess. */

export interface DestinationView {
  /** The "lands in" line: where the NEXT message in this channel goes. */
  label: string;
  /** A second line spelling out the consequence — `""` when the label
   *  already says everything and the caller should render its own
   *  provenance ("chosen 5d ago") instead. */
  note: string;
  /** Nothing at all receives this channel's messages: they are being
   *  ingested, fanned out to an empty set, and dropped. Render as a warning,
   *  not as a neutral state — no error is raised anywhere else. */
  dropping: boolean;
}

const plural = (n: number, one: string, many: string) => (n === 1 ? one : many);

/** "1 of 3 members' private oceans" / "1 of 1 member's private ocean".
 *
 *  Exported because the re-route/stop-watching confirmation dialog
 *  (`SlackChannelActions.tsx`) renders the SAME destination from a different
 *  endpoint's payload, and used to flatten every fan-out to "everyone's
 *  individual oceans" — so a red "nobody mapped · messages dropped" row
 *  opened a dialog calmly asserting corals stay in everyone's oceans. Both
 *  now go through here.
 *
 *  `mapped` is `SlackFanoutReach.mapped_members` on this endpoint and
 *  `seated_oceans` on the impact endpoint; see `SlackDestinationImpact` in
 *  lib/api.ts for why the wire names differ and the wording must not. */
export const fanoutDestinationLabel = (mapped: number, total: number) =>
  total === 1
    ? `${mapped} of 1 member's private workspace`
    : `${mapped} of ${total} members' private workspaces`;

/** The one phrase for "this fan-out reaches nobody", shared by the table row
 *  and the dialog. `total === 0` is a real state (an account with no member
 *  roster at all), so it gets its own wording rather than "0 of 0". */
export const nobodyMappedNote = (total: number) =>
  total === 0
    ? "nobody mapped · messages dropped"
    : `0 of ${total} ${plural(total, "member", "members")} mapped · messages dropped`;

/** Where the next message lands, in words, derived ONLY from what the payload
 *  says. `plane` disambiguates the one case the payload cannot: an org
 *  member's rows carry no `fanout` (roster counts are withheld from them), so
 *  the plane is what distinguishes "the org default is a fan-out" from "the
 *  personal default is one ocean". */
export function destinationView(
  row: SlackChannelRow,
  plane: SlackChannelsResponse["plane"],
): DestinationView {
  const d = row.destination;

  if (d.kind === "single") {
    // A human picked one named ocean. `ocean_name` is null only if that graph
    // no longer resolves, which is worth not pretending about.
    return { label: d.ocean_name ?? "a workspace", note: "", dropping: false };
  }

  const isDefault = d.kind === "default";
  // The account default on the PERSONAL plane is one named ocean, resolved
  // server-side by the same code ingest falls back to. The server guarantees
  // a personal default never also carries a `fanout` (its `LiveRouting`
  // models the account default as one shape per plane), so this branch cannot
  // be reached by a channel that is in fact fanning out.
  if (isDefault && d.fanout === null && d.ocean_name !== null) {
    return { label: d.ocean_name, note: "nobody chose · still ingesting", dropping: false };
  }

  const reach = d.fanout;

  // Fan-out reaching nobody. Every message is ingested, fanned out to an
  // empty set of oceans, and lost — with no error raised anywhere.
  //
  // **Keyed on the server's `dropping`, never on `reach.mapped_members === 0`.**
  // An org member's `fanout` is null (roster counts are withheld from them),
  // so a client-side derivation is FALSE for exactly the viewer most likely
  // to be the one losing messages — which is how a member's broken channel
  // came to read "nobody chose · still ingesting". `dropping` is computed
  // server-side before that masking and is sent to everyone.
  if (d.dropping) {
    const total = reach === null ? 0 : reach.mapped_members + reach.unmapped_members;
    return {
      label: "nobody's workspace",
      // A member gets the fact without the roster size.
      note: reach === null ? "nobody mapped · messages dropped" : nobodyMappedNote(total),
      dropping: true,
    };
  }

  if (reach !== null) {
    const total = reach.mapped_members + reach.unmapped_members;
    const label = fanoutDestinationLabel(reach.mapped_members, total);
    // The shared org ocean receiving nothing is the specific surprise for an
    // admin reading a `default` row, so it is named on that row and not on an
    // explicit fan-out pick (where someone chose exactly this).
    const unmappedNote =
      reach.unmapped_members > 0
        ? `${reach.unmapped_members} unmapped · their messages are dropped`
        : "";
    if (isDefault) {
      return {
        label,
        note: unmappedNote || "nobody chose · the shared org workspace gets nothing",
        dropping: false,
      };
    }
    return { label, note: unmappedNote, dropping: false };
  }

  // No reach to report: an org member (who is not shown roster counts), or a
  // fan-out row on the personal plane, which the picker cannot produce. Say
  // the shape and stop — this is the branch that must never invent a number.
  const label = plane === "org" || !isDefault ? "everyone's own workspaces" : "a workspace";
  return { label, note: isDefault ? "nobody chose · still ingesting" : "", dropping: false };
}

export interface LandedView {
  /** One entry per ocean these corals are actually in, biggest share first
   *  (the server orders them; this preserves that order). */
  entries: { label: string; atomCount: number }[];
  /** Why `entries` is empty — `""` when it isn't. Distinguishes "nothing was
   *  ever captured here" from "none of it is yours to see". */
  empty: string;
}

/** Where this channel's existing corals actually are. For an org MEMBER the
 *  server sends only oceans they own, so an empty list beside a non-zero
 *  `atom_count` means "none of these are yours" — never "nothing landed". */
export function landedView(
  row: SlackChannelRow,
  viewer: Pick<SlackChannelsResponse, "plane" | "viewer_role">,
): LandedView {
  if (row.landed_in.length > 0) {
    return {
      entries: row.landed_in.map((l) => ({ label: l.ocean_name, atomCount: l.atom_count })),
      empty: "",
    };
  }
  if (row.atom_count === 0) return { entries: [], empty: "nothing yet" };
  // Non-zero total, nothing listed: only reachable for a restricted viewer,
  // whose list is filtered to their own oceans.
  const restricted = viewer.plane === "org" && viewer.viewer_role === "member";
  return { entries: [], empty: restricted ? "none in your workspace" : "—" };
}

/** The residue: corals with no channel at all, which no row can show. `null`
 *  when there is nothing to say — including when the server declined to
 *  compute it (`unattributed_atom_count: null`), which must not be rendered
 *  as a zero it never counted.
 *
 *  **Only the stamped half is described as "this workspace's".** The other
 *  half has neither a workspace nor a channel on the row, so on an account
 *  with two connected Slack workspaces both pages would be claiming the same
 *  corals. The sentence says "on this account" for those, which is the most
 *  the data supports. */
export function unattributedNote(data: SlackChannelsResponse): string | null {
  const residue = data.unattributed_atom_count;
  if (residue === null) return null;
  const parts: string[] = [];
  if (residue.this_workspace > 0) {
    const n = residue.this_workspace;
    parts.push(`${n} ${plural(n, "item", "items")} from this workspace`);
  }
  if (residue.account_legacy > 0) {
    const n = residue.account_legacy;
    parts.push(
      `${n} older Slack ${plural(n, "item", "items")} on this account (too old to say which workspace)`,
    );
  }
  if (parts.length === 0) return null;
  // The subject can be a single coral ("1 coral from this workspace"), and
  // "1 corals carry" / "1 coral carry" both read as a bug in the product. Two
  // clauses joined by "plus" are always plural regardless of either count.
  const one =
    parts.length === 1 && residue.this_workspace + residue.account_legacy === 1;
  const verb = one ? "carries" : "carry";
  const isnt = one ? "isn't" : "aren't";
  return `${parts.join(", plus ")} ${verb} no channel and ${isnt} counted in any row above.`;
}

/** A workspace-level warning when at least one visible channel is fanning out
 *  to nobody — the per-row warning is easy to miss on a long table, and this
 *  is the sentence whose absence caused the misdiagnosis. `null` when every
 *  row has somewhere to land. */
export function droppingAlert(data: SlackChannelsResponse): string | null {
  if (data.channels.state !== "ok") return null;
  const dropping = data.channels.rows.filter((r) => r.destination.dropping);
  if (dropping.length === 0) return null;
  const names = dropping.map((r) => r.name).join(", ");
  return `Messages from ${names} are being dropped: no member of this workspace has both a linked Slack identity and a private workspace to receive them.`;
}
