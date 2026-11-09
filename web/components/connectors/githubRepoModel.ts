import {
  GITHUB_ORG_DESTINATION_REQUIRES_ADMIN,
  type GithubDestinationOptions,
  type GithubRepo,
  type GithubRepoDestination,
  type GithubRepoDestinationPick,
  type GithubRepoProgress,
  type GithubRepoSelection,
} from "@/lib/api";
import { ApiError } from "@/lib/api";

/* Pure decisions behind `/connectors/github` — kept framework-free for the
 * same reason `googleChatModel.ts` beside it is: this repo's vitest runs a
 * Node environment with no DOM/testing-library, so a component that renders
 * these decisions cannot be exercised by a plain `.test.ts` file at all.
 *
 * Four things this module exists to get right, per docs/connectors/github.md
 * §3.3–3.4 and §3.6–3.8:
 *
 *  1. A never-selected row (`row_id === null`) must default its active
 *     toggle from `suggested`, never from `active` (which is meaningless
 *     for it) — `defaultRepoActive`.
 *  2. The destination picker must offer exactly what §3.4's role table
 *     allows: personal always; an EXISTING team only where the caller
 *     already admins it; a brand-new repo-mirrored team always (the "or
 *     create a new team" clause); org only when the backend says this
 *     caller may — `adminTeams`/`orgDestinationAllowed`.
 *  3. The org disclosure line must say the numbers, not just "some members
 *     can read this" — `orgDisclosureLine`.
 *  4. A 403 from the save is a REFUSAL of the whole submission (one PUT for
 *     the entire table), not a per-repo failure — `isOrgDestinationRefusal`
 *     plus `rowsNeedingReofferAfterOrgRefusal` name which rows caused it so
 *     the UI can re-offer personal/team for exactly those, and only those.
 */

// ── 1. Active default ────────────────────────────────────────────────────

/** The selection page's starting value for a repo's active toggle.
 *  `suggested` only ever matters for a never-selected row — once `row_id`
 *  is set, `active` is the live, server-authoritative value and `suggested`
 *  is stale (it was computed once, at the moment the grant first appeared). */
export function defaultRepoActive(repo: GithubRepo): boolean {
  return repo.row_id === null ? repo.suggested : repo.active;
}

// ── 2. Destination options ───────────────────────────────────────────────

/** Existing teams the caller may route a repo into directly — `is_admin`
 *  only. A team where the caller is a plain member is not offered here;
 *  they still have the "create a new team" option (always available, see
 *  `GithubRepoDestinationPick`'s doc comment), just not this one. */
export function adminTeams(
  teams: GithubDestinationOptions["teams"],
): GithubDestinationOptions["teams"] {
  return teams.filter((t) => t.is_admin);
}

/** Whether the org-ocean option should be offered at all — never derived
 *  from anything else client-side; the backend is the one place that knows
 *  whether this caller is a CoralSwarm org admin. */
export function orgDestinationAllowed(destinations: GithubDestinationOptions): boolean {
  return destinations.org.allowed;
}

/** The §3.4 disclosure line, in numbers: "N org members, M of whom are not
 *  collaborators [on this repo]" — never a vaguer paraphrase, per the
 *  connector doc's own quoted wording. Only meaningful when the org option
 *  is actually offered; callers should not render it otherwise.
 *  `non_collaborator_count: null` means the roster comparison couldn't be
 *  computed (e.g. a collaborator-list read failed) — rendered as
 *  "unavailable", never silently dropped or treated as zero. */
export function orgDisclosureLine(org: GithubDestinationOptions["org"]): string {
  const members = `${org.member_count} org member${org.member_count === 1 ? "" : "s"}`;
  if (org.non_collaborator_count === null) {
    return `${members}, of whom the number who are not collaborators on this repo is unavailable right now.`;
  }
  return org.non_collaborator_count === 1
    ? `${members}, 1 of whom is not a collaborator on this repo.`
    : `${members}, ${org.non_collaborator_count} of whom are not collaborators on this repo.`;
}

// ── 3. Progress / paused copy ────────────────────────────────────────────

const PROGRESS_LABELS: { key: keyof GithubRepoProgress; label: string }[] = [
  { key: "prs", label: "PRs" },
  { key: "issues", label: "issues" },
  { key: "releases", label: "releases" },
  { key: "commits", label: "commits" },
];

/** "PRs 40%, issues 0%" per §3.6 — one comma-joined segment per object type
 *  that has actually started (non-null), in narrative order. `null` (not
 *  `""`) when nothing has started yet or there is no progress row at all,
 *  so a caller can tell "hasn't begun" apart from "began, all at 0%" (the
 *  latter DOES render, as "PRs 0%, …"). */
export function progressSummary(progress: GithubRepoProgress | null): string | null {
  if (!progress) return null;
  const parts = PROGRESS_LABELS.filter(({ key }) => progress[key] !== null).map(
    ({ key, label }) => `${label} ${progress[key]}%`,
  );
  return parts.length > 0 ? parts.join(", ") : null;
}

/** "sync stopped since {date}" per §3.8 — CoralSwarm never purges on
 *  disconnect/removal, and the copy must say so rather than implying loss.
 *  `null` when the repo isn't paused. */
/** The status badge a repo row shows, in precedence order. Pure so the
 *  precedence is testable: a SYNCED row still carries its final 100%
 *  progress object, so `synced` must win over `backfilling` or a finished
 *  repo reads "backfilling — PRs 100%, …" for ever (first real save,
 *  2026-09-21). Paused wins over everything: it is the state an admin must
 *  act on.
 *
 *  `failed` sits immediately under `paused`, for the same reason, and above
 *  BOTH `synced` and `backfilling`. It exists because nothing else on this
 *  row could say it: for days, two production repos whose backfill jobs were
 *  `dead` rendered as `backfilling` with a reassuring counter line, because
 *  every input that arm reads was consistent with progress — a null
 *  `last_synced_at` on a first backfill is indistinguishable from "still
 *  working". It must beat `synced` too: a repo that synced once and whose
 *  reconcile sweep is now failing IS failing, and "synced as of {old date}"
 *  is precisely the reading that would hide it. */
export type RepoStatusKind = "paused" | "failed" | "synced" | "backfilling" | "not_started";
export function repoStatusKind(
  repo: Pick<GithubRepo, "sync_stopped_at" | "last_synced_at" | "progress" | "sync_error_code">,
): RepoStatusKind {
  if (pausedSinceCopy(repo)) return "paused";
  if (syncErrorCopy(repo)) return "failed";
  if (repo.last_synced_at) return "synced";
  if (progressSummary(repo.progress)) return "backfilling";
  return "not_started";
}

/** Wire code -> the sentence a human reads. The backend sends a
 *  CLASSIFICATION, not its error text (internal detail must not leave
 *  through an API response), so this table is the only place that copy
 *  exists — the same arrangement `GITHUB_ORG_DESTINATION_REQUIRES_ADMIN`
 *  uses for the org-refusal message. */
const SYNC_ERROR_LABELS: Record<string, string> = {
  destination_unreachable:
    "the destination workspace is no longer reachable by whoever chose it — pick a new destination",
  sync_failed: "sync failed — we're looking into it",
};

/** The one-line failure copy for a repo whose last sync tick failed, or
 *  `null` when it is not currently failing — the same `null`-not-`""`
 *  contract {@link progressSummary} and {@link pausedSinceCopy} use.
 *
 *  An UNRECOGNISED code still renders, as generic failure copy. The backend's
 *  set can gain a member without a web release, and the outcome that is not
 *  allowed is for a new code to make a failing repo look healthy again.
 *
 *  A whitespace-only code reads as NO error rather than rendering an empty
 *  red line: "the badge says failed and the reason is blank" is a worse read
 *  than either honest state. */
export function syncErrorCopy(repo: Pick<GithubRepo, "sync_error_code">): string | null {
  const raw = repo.sync_error_code;
  if (typeof raw !== "string") return null;
  const code = raw.trim();
  if (code === "") return null;
  return SYNC_ERROR_LABELS[code] ?? SYNC_ERROR_LABELS.sync_failed;
}

/** Wire `paused_reason` -> the sentence a human reads. The column is a
 *  closed internal vocabulary (`connectors::github_disconnect::
 *  paused_reason`), and the status cell used to render it RAW — fine while
 *  every value was self-explanatory, wrong the moment one of them is the
 *  actionable `destination_unreachable`. Unlisted values fall back to
 *  de-snake-casing ({@link unSnake}), so a new backend reason degrades to
 *  readable rather than vanishing. */
const PAUSED_REASON_LABELS: Record<string, string> = {
  destination_unreachable:
    "whoever chose this repo's destination can no longer reach that workspace — re-route it, or restore their access",
  admin_toggled_off: "switched off here",
  uninstalled: "the GitHub App was uninstalled",
  repo_removed: "removed from the installation's grant",
  repo_unavailable: "the repo could not be reached on GitHub",
  // Written only by the retired code tier (removed 2026-10-27). A row
  // paused this way before then is not blocked by anything any more.
  file_ceiling_exceeded: "paused by the retired code index — switch it back on to sync PRs, issues and commits",
  pending_selection: "granted by GitHub, not yet routed",
};

/** Last-resort label for a reason this build has never heard of.
 *  De-snake-casing the raw key gives something a human can read AND grep
 *  for in the backend. */
function unSnake(key: string): string {
  return key.replace(/_/g, " ");
}

/** Human copy for a repo's `paused_reason`, or `null` when it is not paused
 *  — the same `null`-not-`""` contract the rest of this module uses. */
export function pausedReasonCopy(repo: Pick<GithubRepo, "paused_reason">): string | null {
  const raw = repo.paused_reason;
  if (typeof raw !== "string") return null;
  const reason = raw.trim();
  if (reason === "") return null;
  return PAUSED_REASON_LABELS[reason] ?? unSnake(reason);
}

export function pausedSinceCopy(repo: Pick<GithubRepo, "sync_stopped_at">): string | null {
  if (!repo.sync_stopped_at) return null;
  const d = new Date(repo.sync_stopped_at);
  const stamp = Number.isNaN(d.getTime()) ? repo.sync_stopped_at : d.toLocaleDateString();
  return `sync stopped since ${stamp}`;
}

// ── 4. Table state → wire payload ────────────────────────────────────────

/** UI-side destination choice for one row. Mirrors
 *  {@link GithubRepoDestinationPick} except the brand-new-team case, which
 *  is its own discriminated arm here (`"team_new"`) rather than
 *  `{kind:"team",create:true}` — a picker's `<select>` needs one flat value
 *  per option, and collapsing "existing team X" and "a new team" onto the
 *  same `"team"` kind would make the select's `value` ambiguous between
 *  them. `toWireDestinationPick` below is the one place that re-tags this
 *  back onto the wire shape. */
export type RepoDestinationChoice =
  | { kind: "personal" }
  | { kind: "team"; team_id: string }
  | { kind: "team_new" }
  | { kind: "org" };

/** The picker's starting value for a repo — the server's saved destination
 *  when there is one, else `"personal"` (always available, the least
 *  consequential default, and nothing is written anywhere until Save is
 *  clicked — see the page's own module doc). Never defaults to `"org"` or an
 *  existing team, which would put a repo's content somewhere shared without
 *  an explicit pick ever having been rendered for a human to see and change. */
export function defaultDestinationChoice(
  destination: GithubRepoDestination | null,
): RepoDestinationChoice {
  if (!destination) return { kind: "personal" };
  switch (destination.kind) {
    case "personal":
      return { kind: "personal" };
    case "org":
      return { kind: "org" };
    case "team":
      // A saved team destination always carries a concrete `team_id`
      // (it was created or chosen at the time it was saved) — this can
      // never legitimately resolve to "team_new" on load.
      return destination.team_id ? { kind: "team", team_id: destination.team_id } : { kind: "personal" };
  }
}

function toWireDestinationPick(choice: RepoDestinationChoice): GithubRepoDestinationPick {
  switch (choice.kind) {
    case "personal":
      return { kind: "personal" };
    case "team":
      return { kind: "team", team_id: choice.team_id };
    case "team_new":
      return { kind: "team", create: true };
    case "org":
      return { kind: "org" };
  }
}

/** One row of local table state — what the page keeps per repo between load
 *  and Save. */
export interface RepoRowState {
  repo_id: number;
  active: boolean;
  destination: RepoDestinationChoice;
}

/** Builds the exact PUT body from the table's current visible state — every
 *  row, not just changed ones (the endpoint is "the whole table", per the
 *  contract: one PUT with `selections` for every repo under the
 *  installation). */
export function buildGithubReposPutBody(
  installationId: string,
  rows: RepoRowState[],
): { installation_id: string; selections: GithubRepoSelection[] } {
  return {
    installation_id: installationId,
    selections: rows.map((r) => ({
      repo_id: r.repo_id,
      active: r.active,
      destination: toWireDestinationPick(r.destination),
    })),
  };
}

// ── 5. The org-refusal trap ──────────────────────────────────────────────

/** Whether a save failure is the §3.4 "non-admin picked org" refusal —
 *  keyed on the error CODE (`ApiError.message`, since this endpoint sends a
 *  machine string rather than a sentence — see
 *  `GITHUB_ORG_DESTINATION_REQUIRES_ADMIN`'s own doc comment), never on
 *  status alone: a 403 could mean something else entirely (e.g. the
 *  installation itself isn't this account's). */
export function isOrgDestinationRefusal(e: unknown): boolean {
  return (
    e instanceof ApiError &&
    e.status === 403 &&
    e.message === GITHUB_ORG_DESTINATION_REQUIRES_ADMIN
  );
}

/** Which repo ids to re-offer personal/team for after an org-destination
 *  refusal — every row the submission actually set to `"org"`, and only
 *  those. The whole PUT was rejected (nothing saved), so every other row's
 *  in-progress pick is left exactly as the person set it. */
export function rowsNeedingReofferAfterOrgRefusal(rows: RepoRowState[]): number[] {
  return rows.filter((r) => r.destination.kind === "org").map((r) => r.repo_id);
}

/** Fixed client-side copy for the refusal — the wire error is a machine
 *  code, not prose (see `GITHUB_ORG_DESTINATION_REQUIRES_ADMIN`), so this is
 *  the one place its human sentence lives. */
export function orgDestinationRefusalMessage(): string {
  return "Only a CoralSwarm org admin can route a repo to the org workspace. Choose a personal or team destination instead for the repo(s) marked below.";
}
