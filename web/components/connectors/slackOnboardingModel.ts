import type {
  SlackOnboardingChannelsOk,
  SlackOnboardingConnected,
  SlackOnboardingMember,
  SlackOnboardingStatus,
} from "@/lib/api";

/* Shared derivation for the Slack onboarding steps.
 *
 * There are now THREE renderers of this status payload: the inline checklist
 * on Connectors, the post-OAuth sheet, and the Slack App Home tab (Rust, so
 * necessarily its own code). The two TypeScript ones derive from HERE rather
 * than each computing their own step states — the entire reason the status
 * contract is server-derived (docs/connectors/slack-onboarding.md §3) is that
 * surfaces which disagree about "is this set up" are worse than one surface,
 * and two local derivations drift the first time a rule changes.
 *
 * ── Two journeys, one derivation (§3.1) ──
 * `deriveSteps` branches on the viewer. An org MEMBER gets a different three
 * steps — link your identity · get the bot into YOUR channels · first capture
 * in YOUR ocean — because the admin's three are admin-by-construction (only
 * an admin can authorise the app, so anyone who ever sees them as their own
 * task list is one). The branch lives HERE, in the one derivation, rather
 * than as a second model file: a member view that drifted from the admin view
 * on what "done" means would be exactly the failure §3 exists to prevent. */

export type StepState = "done" | "active" | "pending" | "unknown";

export interface OnboardingStep {
  kind: "step";
  n: 1 | 2 | 3;
  name: string;
  state: StepState;
  /** Short mono status shown beside the step name. */
  status: string;
  /** Whether this step should offer the `/invite` command. */
  showCommand: boolean;
  /** The containers this member and the agent are both in, NAMED.
   *
   *  Present only on step 2's `ok` arm — the same arm `count` is present on,
   *  and always the list that `count` was derived from, so the sentence and
   *  the list can never disagree. Everywhere else it is `undefined`, which is
   *  deliberately distinct from `[]`: an empty list means "we looked and the
   *  agent is in none of yours", and `undefined` means there is no answer to
   *  show (not linked yet, or the live lookup was lost). Rendering both as an
   *  empty list is the absence-as-an-answer bug the three-way state exists to
   *  prevent. */
  containers?: { id: string; name: string }[];
}

/** Org-admin-only: identity coverage, rendered as an ONGOING GAUGE.
 *
 *  It carries no `StepState` at all, and that is the enforcement, not an
 *  oversight. Coverage moves as people join and leave the org, so a gauge
 *  that could be `done` would be a task that un-completes itself — and the
 *  moment it can be `done`, it starts counting toward the 0-3 the reef
 *  mandala's density is bound to. Making the field structurally absent means
 *  `filter((s) => s.state === "done")` over the raw row list does not
 *  compile; you have to narrow to `kind: "step"` first (see `isTaskStep`). */
export interface CoverageGauge {
  kind: "gauge";
  name: string;
  /** e.g. `"8 of 12 linked"`. */
  status: string;
  identified: number;
  total: number;
}

/** What `deriveSteps` returns: the numbered task steps, plus — for an org
 *  admin — the coverage gauge as a final row. */
export type OnboardingRow = OnboardingStep | CoverageGauge;

/** Narrow a row to a real, completable task. Every consumer that counts
 *  progress or maps a row to a step DETAIL must go through this. */
export function isTaskStep(row: OnboardingRow): row is OnboardingStep {
  return row.kind === "step";
}

/** Slack's roster call can fail, and a failed lookup is NOT a zero.
 *  Everything downstream that wants to treat `channels.count` as a real
 *  number has to ask this first — the "1 of 0 channels" bug came from a
 *  denominator that skipped this check.
 *
 *  A TYPE PREDICATE, not a `boolean`. As a plain boolean this enforced
 *  nothing: callers still had a `count` typed `number` on both arms, so
 *  forgetting the check compiled cleanly and produced the bug anyway. Narrowing
 *  the union is what turns "remember to ask" into a compile error. */
export function channelsKnown(
  data: SlackOnboardingConnected,
): data is SlackOnboardingConnected & { channels: SlackOnboardingChannelsOk } {
  return data.channels.state === "ok";
}

/** Is this payload an org MEMBER's view?
 *
 *  A type predicate for the same reason `channelsKnown` is one: it narrows
 *  `member` to non-null, so the member branch cannot read a member field
 *  without having established there is one. Gated on the SECTION's presence
 *  and not on `viewer_role` alone — the server only sends `member` for an org
 *  member, so trusting the section is trusting the narrower fact. A
 *  `viewer_role === "member"` with no section (a personal account, or an
 *  older backend) correctly falls through to the admin view rather than
 *  rendering a member journey with nothing in it. */
export function memberView(
  data: SlackOnboardingConnected,
): data is SlackOnboardingConnected & { member: SlackOnboardingMember } {
  return data.viewer_role === "member" && data.member !== null;
}

/** Can this viewer start the Slack OAuth flow themselves?
 *
 *  An org MEMBER cannot: Slack's install grants workspace-wide scopes and
 *  only an org admin may authorise it. Offering them a Connect button is
 *  offering a dead end — they click it, bounce off Slack's own admin gate,
 *  and learn nothing about who to ask. The connectors card shows the honest
 *  state and a request they can send instead.
 *
 *  Takes the WHOLE union, not the connected arm: this is the one decision
 *  that only exists while `connected === false`, which is exactly why
 *  `viewer_role` rides on the not-connected shape too (docs/connectors/
 *  slack-onboarding.md §3).
 *
 *  `null` data (the endpoint 404'd, 403'd, or hasn't resolved) reads as
 *  "not blocked" — the same degradation `StaticExplainer` takes. A member
 *  who gets a button that bounces is a worse day than an admin who gets no
 *  button at all, and we cannot tell them apart without an answer. */
export function connectIsAdminOnly(data: SlackOnboardingStatus | null): boolean {
  if (data === null || data.connected) return false;
  return workspaceActsAreAdminOnly(data);
}

/** Are the WORKSPACE-level acts (connect AND disconnect) someone else's to
 *  take? [`connectIsAdminOnly`] deliberately answers only the pre-connect
 *  question; this one holds across both connection states, because
 *  disconnect became an admin act too (#531's `reject_org_slack_non_admin`)
 *  and a member's Disconnect button now bounces off the same gate their
 *  Connect button would have. Same null-degradation rule as above. */
export function workspaceActsAreAdminOnly(data: SlackOnboardingStatus | null): boolean {
  if (data === null) return false;
  return data.plane === "org" && data.viewer_role === "member";
}

/** Does this viewer get the coverage gauge? Org plane, real coverage data,
 *  and NOT a member — a member has no business seeing org-wide coverage on
 *  their own setup checklist, and their own link is step 1 there anyway. */
function gaugeFor(data: SlackOnboardingConnected): CoverageGauge | null {
  if (data.plane !== "org" || data.identity === null || memberView(data)) return null;
  const { identified, total } = data.identity;
  return {
    kind: "gauge",
    name: "Team coverage",
    status: `${identified} of ${total} linked`,
    identified,
    total,
  };
}

/** The org MEMBER's three steps (docs/connectors/slack-onboarding.md §3.1).
 *
 *  Step 1 is the funnel: with no confirmed identity, nothing this member says
 *  in Slack can reach their ocean, so every later step is honestly pending
 *  rather than falsely failing. */
function deriveMemberSteps(data: SlackOnboardingConnected & { member: SlackOnboardingMember }): OnboardingRow[] {
  const { linked, my_channels: mine, my_capture: capture } = data.member;

  const step1: OnboardingStep = linked.linked
    ? {
        kind: "step",
        n: 1,
        name: "Link your Slack identity",
        state: "done",
        // An email match is something the server did, not something they
        // did — say so, rather than congratulating them for it.
        status: linked.link_method === "verified_email" ? "linked automatically" : "linked",
        showCommand: false,
      }
    : {
        kind: "step",
        n: 1,
        name: "Link your Slack identity",
        state: "active",
        status: "start here",
        showCommand: false,
      };

  // Members inviting the bot into their own channels IS the coverage model,
  // so the `/invite` affordance stays on this step for every state except
  // "already in all of them" — including "unknown", where the command is
  // still correct even though the count isn't.
  const step2: OnboardingStep = (() => {
    const base = { kind: "step", n: 2, name: "Get the bot into your channels" } as const;
    switch (mine.state) {
      case "unlinked":
        // NOT "in 0 channels": we never looked. Sending someone to invite a
        // bot that may already be sitting in every channel they're in is the
        // exact false-failure the unavailable-vs-zero rule exists to stop,
        // one step further down the page.
        return { ...base, state: "pending", status: "link first", showCommand: false };
      case "unavailable":
        return { ...base, state: "unknown", status: "couldn't check", showCommand: true };
      case "ok":
        return mine.count > 0
          ? {
              ...base,
              state: "done",
              status: `in ${mine.count} of your channel${mine.count === 1 ? "" : "s"}`,
              showCommand: false,
              // Named, not just counted. "in 3 of your channels" tells someone
              // a number about their own workspace that they cannot check;
              // naming them is what makes the disclosure verifiable, and it is
              // the list the backfill offer is made from (plan §4.2).
              containers: mine.containers,
            }
          : { ...base, state: "active", status: "not yet", showCommand: true, containers: [] };
    }
  })();

  const capturing = capture.atom_count > 0;
  const step3: OnboardingStep = {
    kind: "step",
    n: 3,
    name: "First capture in your workspace",
    state: capturing ? "done" : "pending",
    status: capturing ? "capturing" : "nothing yet",
    showCommand: false,
  };

  return [step1, step2, step3];
}

export function deriveSteps(data: SlackOnboardingConnected): OnboardingRow[] {
  if (memberView(data)) return deriveMemberSteps(data);

  const known = channelsKnown(data);
  const inChannels = known && data.channels.count > 0;

  // Step 1 — the roster's own state outranks its count.
  const step1: OnboardingStep = !known
    ? { kind: "step", n: 1, name: "Invite the bot", state: "unknown", status: "couldn't check", showCommand: true }
    : inChannels
      ? {
          kind: "step",
          n: 1,
          name: "Invite the bot",
          state: "done",
          status: `in ${data.channels.count} channel${data.channels.count === 1 ? "" : "s"}`,
          showCommand: false,
        }
      : { kind: "step", n: 1, name: "Invite the bot", state: "active", status: "start here", showCommand: true };

  // Step 2 is never "broken" — an unrouted channel still ingests via the
  // account default, so its two states are "chosen" and "using the default".
  const routed = data.routed.count > 0;
  const step2: OnboardingStep = {
    kind: "step",
    n: 2,
    name: "Choose where it lands",
    state: routed ? "done" : inChannels ? "active" : "pending",
    status: routed ? "chosen" : "using default",
    showCommand: false,
  };

  const capturing = data.capture.atom_count > 0;
  const step3: OnboardingStep = {
    kind: "step",
    n: 3,
    name: "First capture",
    state: capturing ? "done" : "pending",
    status: capturing ? "capturing" : "nothing yet",
    showCommand: false,
  };

  const rows: OnboardingRow[] = [step1, step2, step3];
  const gauge = gaugeFor(data);
  if (gauge) rows.push(gauge);
  return rows;
}

/** How many steps are genuinely complete (0-3). Drives the reef.
 *
 *  `isTaskStep` is load-bearing, not defensive: the coverage gauge is a row
 *  in the same list and must never be one of these. Drop the narrowing and
 *  this stops compiling (`CoverageGauge` has no `state`) — which is the
 *  point; the reef mandala's density is bound to a 0-3 scale, and a fourth
 *  "done" would push it past full while claiming setup progress nobody made. */
export function completedCount(data: SlackOnboardingConnected): number {
  return deriveSteps(data).filter(isTaskStep).filter((s) => s.state === "done").length;
}

/* `DotMandala`'s own `density` prop scales dots-per-ring, which is exactly
 * the knob the first-run flow already ends on ("the mandala at full density,
 * the five hues named" — ui_kits/onboarding). Driving that prop rather than
 * lighting rings one at a time keeps this on the real brand mark instead of
 * forking a second one, and keeps density meaning what it already means.
 *
 * Never starts at 0: the mark must still read as the mark at step zero, and
 * you HAVE connected by the time this renders — that much is real. */
const DENSITY_BY_STEP = [0.28, 0.52, 0.76, 1] as const;

export function reefDensity(completed: number): number {
  return DENSITY_BY_STEP[Math.max(0, Math.min(3, completed))];
}

export function reefCaption(completed: number): string {
  return (
    ["Nothing is growing yet", "Your topic is taking hold", "Your topic is forming", "Your topic is live"][
      Math.max(0, Math.min(3, completed))
    ] ?? "Your topic"
  );
}
