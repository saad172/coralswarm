import { describe, expect, it } from "vitest";
import {
  ApiError,
  type GithubDestinationOptions,
  type GithubRepo,
  type GithubRepoProgress,
} from "@/lib/api";
import {
  adminTeams,
  buildGithubReposPutBody,
  defaultDestinationChoice,
  defaultRepoActive,
  isOrgDestinationRefusal,
  orgDestinationAllowed,
  orgDisclosureLine,
  pausedReasonCopy,
  pausedSinceCopy,
  progressSummary,
  rowsNeedingReofferAfterOrgRefusal,
  type RepoRowState,
  repoStatusKind,
  syncErrorCopy,
} from "./githubRepoModel";

function repo(overrides: Partial<GithubRepo>): GithubRepo {
  return {
    row_id: null,
    repo_id: 1,
    owner: "coralswarm",
    name: "coralswarm",
    full_name: "coralswarm/coralswarm",
    default_branch: "main",
    visibility: "private",
    archived: false,
    fork: false,
    size_kb: 1200,
    active: false,
    suggested: true,
    paused_reason: null,
    sync_stopped_at: null,
    destination: null,
    progress: null,
    last_synced_at: null,
    sync_error_code: null,
    sync_error_at: null,
    ...overrides,
  };
}

// ── defaultRepoActive ───────────────────────────────────────────────────

describe("defaultRepoActive", () => {
  it("uses `suggested` for a never-selected row (row_id null)", () => {
    expect(defaultRepoActive(repo({ row_id: null, suggested: true, active: false }))).toBe(true);
    expect(defaultRepoActive(repo({ row_id: null, suggested: false, active: true }))).toBe(false);
  });

  it("uses the live `active` value once a watch row exists", () => {
    expect(defaultRepoActive(repo({ row_id: "row-1", suggested: true, active: false }))).toBe(false);
    expect(defaultRepoActive(repo({ row_id: "row-1", suggested: false, active: true }))).toBe(true);
  });
});

// ── destination options ─────────────────────────────────────────────────

describe("adminTeams", () => {
  it("keeps only teams the caller admins", () => {
    const teams = [
      { id: "t1", name: "Eng", is_admin: true },
      { id: "t2", name: "Sales", is_admin: false },
    ];
    expect(adminTeams(teams)).toEqual([{ id: "t1", name: "Eng", is_admin: true }]);
  });

  it("returns an empty list when the caller admins nothing", () => {
    expect(adminTeams([{ id: "t2", name: "Sales", is_admin: false }])).toEqual([]);
  });
});

describe("orgDestinationAllowed", () => {
  const base: GithubDestinationOptions = {
    personal: true,
    teams: [],
    org: { allowed: false, member_count: 40, non_collaborator_count: 12 },
  };

  it("is false when the backend says the caller may not route to org", () => {
    expect(orgDestinationAllowed(base)).toBe(false);
  });

  it("is true when the backend allows it", () => {
    expect(orgDestinationAllowed({ ...base, org: { ...base.org, allowed: true } })).toBe(true);
  });
});

describe("orgDisclosureLine", () => {
  it("states both numbers, per §3.4's exact wording shape", () => {
    expect(orgDisclosureLine({ allowed: true, member_count: 40, non_collaborator_count: 12 })).toBe(
      "40 org members, 12 of whom are not collaborators on this repo.",
    );
  });

  it("singularizes both counts at 1", () => {
    expect(orgDisclosureLine({ allowed: true, member_count: 1, non_collaborator_count: 1 })).toBe(
      "1 org member, 1 of whom is not a collaborator on this repo.",
    );
  });

  it("reports unavailable, never zero, when the comparison couldn't be computed", () => {
    const line = orgDisclosureLine({ allowed: true, member_count: 40, non_collaborator_count: null });
    expect(line).toContain("unavailable");
    expect(line).not.toContain("0 of whom");
  });
});

// ── progress / paused copy ──────────────────────────────────────────────

describe("progressSummary", () => {
  it("renders the narrative types in order", () => {
    expect(progressSummary({ prs: 40, issues: 0, releases: null, commits: null })).toBe("PRs 40%, issues 0%");
  });

  it("is null when there is no progress row at all", () => {
    expect(progressSummary(null)).toBeNull();
  });

  it("is null when every field is null (crawl hasn't started)", () => {
    expect(progressSummary({ prs: null, issues: null, releases: null, commits: null })).toBeNull();
  });

  it("renders an explicit 0% rather than omitting a started-but-empty type", () => {
    expect(progressSummary({ prs: 0, issues: null, releases: null, commits: null })).toBe("PRs 0%");
  });

  // The connector captures no source code (2026-10-27). A row synced before
  // then still carries the retired code tier's `code` key in its stored
  // progress; it must not render as a "files" line that promises code.
  it("never renders a files/code segment, even from a legacy progress row", () => {
    const legacy = { code: 100, prs: 40, issues: null, releases: null, commits: null } as unknown as GithubRepoProgress;
    expect(progressSummary(legacy)).toBe("PRs 40%");
  });
});


describe("pausedSinceCopy", () => {
  it("is null for an active (non-paused) repo", () => {
    expect(pausedSinceCopy({ sync_stopped_at: null })).toBeNull();
  });

  it("names the stop date, never implying removal", () => {
    const copy = pausedSinceCopy({ sync_stopped_at: "2026-09-01T00:00:00Z" });
    expect(copy).toMatch(/^sync stopped since /);
  });
});

// ── table state → PUT payload ───────────────────────────────────────────

describe("defaultDestinationChoice", () => {
  it("defaults to personal when nothing has been saved yet", () => {
    expect(defaultDestinationChoice(null)).toEqual({ kind: "personal" });
  });

  it("carries over a saved team destination's id", () => {
    expect(defaultDestinationChoice({ kind: "team", team_id: "t1", team_name: "Eng" })).toEqual({
      kind: "team",
      team_id: "t1",
    });
  });

  it("carries over a saved org destination", () => {
    expect(defaultDestinationChoice({ kind: "org" })).toEqual({ kind: "org" });
  });
});

describe("buildGithubReposPutBody", () => {
  it("builds one selection per row, mapping team_new onto the wire's create:true shape", () => {
    const rows: RepoRowState[] = [
      { repo_id: 1, active: true, destination: { kind: "personal" } },
      { repo_id: 2, active: true, destination: { kind: "team", team_id: "t1" } },
      { repo_id: 3, active: false, destination: { kind: "team_new" } },
      { repo_id: 4, active: true, destination: { kind: "org" } },
    ];
    expect(buildGithubReposPutBody("555", rows)).toEqual({
      installation_id: "555",
      selections: [
        { repo_id: 1, active: true, destination: { kind: "personal" } },
        { repo_id: 2, active: true, destination: { kind: "team", team_id: "t1" } },
        { repo_id: 3, active: false, destination: { kind: "team", create: true } },
        { repo_id: 4, active: true, destination: { kind: "org" } },
      ],
    });
  });
});

// ── org-refusal ──────────────────────────────────────────────────────────

describe("isOrgDestinationRefusal", () => {
  it("matches the exact 403 + code combination", () => {
    expect(isOrgDestinationRefusal(new ApiError("org_destination_requires_admin", 403))).toBe(true);
  });

  it("does not match a differently-worded 403", () => {
    expect(isOrgDestinationRefusal(new ApiError("forbidden", 403))).toBe(false);
  });

  it("does not match the same message at a different status", () => {
    expect(isOrgDestinationRefusal(new ApiError("org_destination_requires_admin", 400))).toBe(false);
  });

  it("does not match a non-ApiError", () => {
    expect(isOrgDestinationRefusal(new Error("network down"))).toBe(false);
  });
});

describe("rowsNeedingReofferAfterOrgRefusal", () => {
  it("names only the rows actually set to org", () => {
    const rows: RepoRowState[] = [
      { repo_id: 1, active: true, destination: { kind: "personal" } },
      { repo_id: 2, active: true, destination: { kind: "org" } },
      { repo_id: 3, active: true, destination: { kind: "org" } },
    ];
    expect(rowsNeedingReofferAfterOrgRefusal(rows)).toEqual([2, 3]);
  });

  it("is empty when nothing in the submission was org", () => {
    const rows: RepoRowState[] = [{ repo_id: 1, active: true, destination: { kind: "personal" } }];
    expect(rowsNeedingReofferAfterOrgRefusal(rows)).toEqual([]);
  });
});

describe("repoStatusKind — precedence", () => {
  const done = { prs: 100, issues: 100, releases: 100, commits: 100 };
  it("a synced row wins over its own final 100% progress", () => {
    expect(repoStatusKind({ sync_stopped_at: null, last_synced_at: "2026-09-21T00:00:00Z", progress: done, sync_error_code: null })).toBe("synced");
  });
  it("a paused row wins over everything", () => {
    expect(repoStatusKind({ sync_stopped_at: "2026-09-21T00:00:00Z", last_synced_at: "2026-09-21T00:00:00Z", progress: done, sync_error_code: "sync_failed" })).toBe("paused");
  });
  it("progress without a sync stamp is backfilling", () => {
    expect(repoStatusKind({ sync_stopped_at: null, last_synced_at: null, progress: { prs: 40, issues: 0, releases: null, commits: null }, sync_error_code: null })).toBe("backfilling");
  });
  it("nothing at all is not started", () => {
    expect(repoStatusKind({ sync_stopped_at: null, last_synced_at: null, progress: null, sync_error_code: null })).toBe("not_started");
  });

  // The exact production shape of the D7 incident: a dead backfill whose
  // skip/progress counters had kept climbing during the tree walk. Before
  // `failed` existed, this row rendered "backfilling — PRs 40%" while its
  // job had been `status='dead'` for days.
  it("a failing row with live-looking progress is failed, not backfilling", () => {
    expect(
      repoStatusKind({
        sync_stopped_at: null,
        last_synced_at: null,
        progress: { prs: 40, issues: 0, releases: null, commits: null },
        sync_error_code: "sync_failed",
      }),
    ).toBe("failed");
  });

  // A repo that synced once and is now failing its reconcile sweep is
  // FAILING. "synced as of <old date>" is exactly the reading that hides it.
  it("a failing row that once synced is failed, not synced", () => {
    expect(
      repoStatusKind({
        sync_stopped_at: null,
        last_synced_at: "2026-09-21T00:00:00Z",
        progress: done,
        sync_error_code: "destination_unreachable",
      }),
    ).toBe("failed");
  });

  // Paused is a state an admin CHOSE; it outranks a stale failure so the
  // badge never contradicts the toggle they just flipped.
  it("paused still outranks failed", () => {
    expect(
      repoStatusKind({
        sync_stopped_at: "2026-09-21T00:00:00Z",
        last_synced_at: null,
        progress: null,
        sync_error_code: "sync_failed",
      }),
    ).toBe("paused");
  });

  // A blank message must not promote the row to `failed` with nothing to
  // show for it — the badge and the reason are written together or not at
  // all.
  it("a whitespace-only sync_error is not a failure", () => {
    expect(
      repoStatusKind({ sync_stopped_at: null, last_synced_at: null, progress: null, sync_error_code: "   \n " }),
    ).toBe("not_started");
  });
});

describe("pausedReasonCopy", () => {
  it("turns the actionable reason into a sentence that names the action", () => {
    const copy = pausedReasonCopy({ paused_reason: "destination_unreachable" });
    expect(copy).toContain("can no longer reach");
    expect(copy).toContain("re-route");
    // Never the raw wire token — that is what the cell used to render.
    expect(copy).not.toContain("destination_unreachable");
  });

  it("covers every reason the backend's paused_reason vocabulary defines", () => {
    // Mirrors `connectors::github_disconnect::paused_reason`. A new constant
    // there should add a row here; until it does, the fallback below keeps it
    // readable rather than letting it render as a snake_case token.
    for (const reason of [
      "uninstalled",
      "repo_removed",
      "admin_toggled_off",
      "pending_selection",
      "repo_unavailable",
      "file_ceiling_exceeded",
      "destination_unreachable",
    ]) {
      const copy = pausedReasonCopy({ paused_reason: reason });
      expect(copy).not.toBeNull();
      expect(copy).not.toContain("_");
    }
  });

  it("de-snake-cases an unrecognised reason rather than dropping it", () => {
    expect(pausedReasonCopy({ paused_reason: "some_future_reason" })).toBe("some future reason");
  });

  it("is null when the repo is not paused", () => {
    expect(pausedReasonCopy({ paused_reason: null })).toBeNull();
    expect(pausedReasonCopy({ paused_reason: "  " })).toBeNull();
  });
});

describe("syncErrorCopy", () => {
  it("maps the actionable code to copy that names the action", () => {
    const copy = syncErrorCopy({ sync_error_code: "destination_unreachable" });
    expect(copy).toContain("destination");
    expect(copy).toContain("pick a new destination");
  });
  it("maps the catch-all code to generic copy", () => {
    expect(syncErrorCopy({ sync_error_code: "sync_failed" })).toBe("sync failed — we're looking into it");
  });
  it("is null when there is no error", () => {
    expect(syncErrorCopy({ sync_error_code: null })).toBeNull();
  });
  it("is null (never an empty line) for a blank code", () => {
    expect(syncErrorCopy({ sync_error_code: "" })).toBeNull();
    expect(syncErrorCopy({ sync_error_code: "\t\n  " })).toBeNull();
  });

  // The backend's code set is open — a new variant ships without a web
  // release. It must still read as a failure, never vanish back into
  // "healthy", which is the whole misread this field exists to end.
  it("renders an unrecognised code as generic failure copy, never null", () => {
    const copy = syncErrorCopy({ sync_error_code: "some_future_code" });
    expect(copy).not.toBeNull();
    expect(copy).toBe("sync failed — we're looking into it");
    expect(
      repoStatusKind({
        sync_stopped_at: null,
        last_synced_at: null,
        progress: null,
        sync_error_code: "some_future_code",
      }),
    ).toBe("failed");
  });

  // Whatever the backend sends, this never becomes a channel for server
  // text: the copy always comes from the local table.
  it("never echoes the wire value back as copy", () => {
    for (const code of ["destination_unreachable", "sync_failed", "some_future_code"]) {
      expect(syncErrorCopy({ sync_error_code: code })).not.toContain(code);
    }
  });
});
