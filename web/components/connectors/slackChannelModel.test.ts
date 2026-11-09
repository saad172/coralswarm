import { describe, expect, it } from "vitest";
import type { SlackChannelRow, SlackChannelsResponse } from "@/lib/api";
import { destinationView, droppingAlert, landedView, unattributedNote } from "./slackChannelModel";

/* These pin the sentences that were WRONG in production on 2026-09-09: an org
 * workspace whose every channel read "your account default · nobody chose ·
 * still ingesting" while ingest was fanning content into exactly one of three
 * admins' private oceans, and the shared org ocean held zero Slack corals.
 *
 * So every test here is written as "would this have caught it": each asserts
 * on the reachable-but-misleading shape, not merely on a happy path. */

const row = (over: Partial<SlackChannelRow> = {}): SlackChannelRow => ({
  channel_id: "C1",
  name: "#platform",
  destination: { kind: "default", ocean_name: null, fanout: null, dropping: false, chosen_at: null, stale_name: false },
  last_activity_at: null,
  atom_count: 0,
  landed_in: [],
  huddles: { seen: 0, ingested: 0, skipped: 0, reasons: [] },
  ...over,
});

const payload = (over: Partial<SlackChannelsResponse> = {}): SlackChannelsResponse => ({
  workspace_id: "T1",
  workspace_name: "CoralSwarm",
  plane: "org",
  viewer_role: "owner",
  channels: { state: "ok", error: null, rows: [] },
  unattributed_atom_count: { this_workspace: 0, account_legacy: 0 },
  ...over,
});

describe("destinationView", () => {
  it("names the workspace for an explicit single pick", () => {
    const v = destinationView(row({ destination: { kind: "single", ocean_name: "Engineering", fanout: null, dropping: false, chosen_at: "2026-08-01T00:00:00Z", stale_name: false } }), "org");
    expect(v).toEqual({ label: "Engineering", note: "", dropping: false });
  });

  it("names the account's own workspace for a personal-plane default, never 'your account default'", () => {
    const v = destinationView(
      row({ destination: { kind: "default", ocean_name: "Alex's workspace", fanout: null, dropping: false, chosen_at: null, stale_name: false } }),
      "personal",
    );
    expect(v.label).toBe("Alex's workspace");
    expect(v.note).toBe("nobody chose · still ingesting");
    expect(v.dropping).toBe(false);
  });

  it("spells out the org default as a member fan-out, and says the org workspace gets nothing", () => {
    // THE PRODUCTION BUG. An org account, no watched row: ingest fans out to
    // each mapped member's private ocean. Every one of these assertions fails
    // against the old hardcoded "your account default".
    const v = destinationView(
      row({ destination: { kind: "default", ocean_name: null, fanout: { mapped_members: 1, unmapped_members: 2 }, dropping: false, chosen_at: null, stale_name: false } }),
      "org",
    );
    expect(v.label).toBe("1 of 3 members' private workspaces");
    expect(v.note).toBe("2 unmapped · their messages are dropped");
    expect(v.dropping).toBe(false);
  });

  it("says the shared org workspace gets nothing when the fan-out reaches everyone", () => {
    const v = destinationView(
      row({ destination: { kind: "default", ocean_name: null, fanout: { mapped_members: 3, unmapped_members: 0 }, dropping: false, chosen_at: null, stale_name: false } }),
      "org",
    );
    expect(v.label).toBe("3 of 3 members' private workspaces");
    expect(v.note).toBe("nobody chose · the shared org workspace gets nothing");
  });

  it("calls a fan-out that reaches nobody a drop, not an empty state", () => {
    const v = destinationView(
      row({ destination: { kind: "default", ocean_name: null, fanout: { mapped_members: 0, unmapped_members: 4 }, dropping: true, chosen_at: null, stale_name: false } }),
      "org",
    );
    expect(v.dropping).toBe(true);
    expect(v.note).toBe("0 of 4 members mapped · messages dropped");
  });

  it("handles a zero-member roster without dividing the sentence by zero", () => {
    const v = destinationView(
      row({ destination: { kind: "default", ocean_name: null, fanout: { mapped_members: 0, unmapped_members: 0 }, dropping: true, chosen_at: null, stale_name: false } }),
      "org",
    );
    expect(v.dropping).toBe(true);
    expect(v.note).toBe("nobody mapped · messages dropped");
  });

  it("an explicit fan-out pick that reaches everyone carries no scare note", () => {
    const v = destinationView(
      row({ destination: { kind: "fanout", ocean_name: null, fanout: { mapped_members: 5, unmapped_members: 0 }, dropping: false, chosen_at: "2026-08-01T00:00:00Z", stale_name: false } }),
      "org",
    );
    expect(v.label).toBe("5 of 5 members' private workspaces");
    // Someone chose exactly this; the caller renders "chosen …" instead.
    expect(v.note).toBe("");
  });

  it("invents no roster numbers for a member viewer, whose rows carry no reach", () => {
    // The server withholds `fanout` from org members (seat counts are other
    // people's information). The label must degrade to the SHAPE, never to a
    // fabricated count and never back to "your account default".
    const v = destinationView(
      row({ destination: { kind: "default", ocean_name: null, fanout: null, dropping: false, chosen_at: null, stale_name: false } }),
      "org",
    );
    expect(v.label).toBe("everyone's own workspaces");
    expect(v.label).not.toMatch(/\d/);
    expect(v.dropping).toBe(false);
  });

  it("tells a MEMBER their channel reaches nobody, with the counts still masked", () => {
    // FAILS WITHOUT THE FIX (this test previously asserted the opposite, and
    // was wrong to). A member's `fanout` is null, so a client-side
    // `mapped_members === 0` check is false for exactly the viewer most
    // likely to be losing messages — their row read "nobody chose · still
    // ingesting" while every message they sent was discarded. The server
    // sends `dropping` to everyone; this must key on it.
    const v = destinationView(
      row({ destination: { kind: "default", ocean_name: null, fanout: null, dropping: true, chosen_at: null, stale_name: false } }),
      "org",
    );
    expect(v.dropping).toBe(true);
    expect(v.label).toBe("nobody's workspace");
    expect(v.note).toBe("nobody mapped · messages dropped");
    // …and still no roster size, because the member was not sent one.
    expect(v.note).not.toMatch(/\d/);
  });

  it("keeps a personal default healthy even beside a legacy fan-out row", () => {
    // The server guarantees a personal-plane Default never also carries a
    // `fanout` (its account default is modelled as one shape per plane). This
    // pins the client half of that contract: an ocean-named default is never
    // routed through the fan-out branch.
    const v = destinationView(
      row({ destination: { kind: "default", ocean_name: "Alex's workspace", fanout: null, dropping: false, chosen_at: null, stale_name: false } }),
      "personal",
    );
    expect(v.label).toBe("Alex's workspace");
    expect(v.dropping).toBe(false);
  });

  it("an explicit fan-out with unmapped members keeps BOTH its warning and its provenance", () => {
    // The cell renders `chosen_at` separately whenever it is non-null, so the
    // model must not swallow provenance into the note (nor vice versa): the
    // row needs to say both "someone deliberately chose this" and "part of
    // it is going nowhere".
    const r = row({
      destination: {
        kind: "fanout",
        ocean_name: null,
        fanout: { mapped_members: 2, unmapped_members: 3 },
        dropping: false,
        chosen_at: "2026-08-01T00:00:00Z",
        stale_name: false,
      },
    });
    const v = destinationView(r, "org");
    expect(v.label).toBe("2 of 5 members' private workspaces");
    expect(v.note).toBe("3 unmapped · their messages are dropped");
    expect(r.destination.chosen_at).not.toBeNull();
  });

  it("never renders the phrase that caused the misdiagnosis", () => {
    const cases: SlackChannelRow[] = [
      row(),
      row({ destination: { kind: "default", ocean_name: "Alex's workspace", fanout: null, dropping: false, chosen_at: null, stale_name: false } }),
      row({ destination: { kind: "default", ocean_name: null, fanout: { mapped_members: 1, unmapped_members: 2 }, dropping: false, chosen_at: null, stale_name: false } }),
      row({ destination: { kind: "fanout", ocean_name: null, fanout: null, dropping: false, chosen_at: "x", stale_name: false } }),
    ];
    for (const plane of ["org", "personal"] as const) {
      for (const c of cases) {
        expect(destinationView(c, plane).label).not.toBe("your account default");
      }
    }
  });
});

describe("landedView", () => {
  const admin = { plane: "org", viewer_role: "owner" } as const;
  const member = { plane: "org", viewer_role: "member" } as const;

  it("lists every workspace items actually landed in, in server order", () => {
    const v = landedView(
      row({
        atom_count: 7,
        landed_in: [
          { ocean_name: "My Workspace (Alex)", ocean_kind: "member", atom_count: 5 },
          { ocean_name: "Engineering", ocean_kind: "team", atom_count: 2 },
        ],
      }),
      admin,
    );
    expect(v.entries).toEqual([
      { label: "My Workspace (Alex)", atomCount: 5 },
      { label: "Engineering", atomCount: 2 },
    ]);
    expect(v.empty).toBe("");
  });

  it("says 'nothing yet' only when nothing was captured at all", () => {
    expect(landedView(row({ atom_count: 0, landed_in: [] }), admin).empty).toBe("nothing yet");
  });

  it("tells a member their workspace got none of it, rather than 'nothing yet'", () => {
    // FAILS WITHOUT THE DISTINCTION: a member's `landed_in` is filtered to
    // oceans they own, so an empty list beside a non-zero count means "none
    // of these are yours". Rendering that as "nothing yet" would tell a
    // member the channel is not being captured when it is.
    const v = landedView(row({ atom_count: 12, landed_in: [] }), member);
    expect(v.empty).toBe("none in your workspace");
  });
});

describe("unattributedNote", () => {
  it("says nothing when both halves are zero", () => {
    expect(
      unattributedNote(payload({ unattributed_atom_count: { this_workspace: 0, account_legacy: 0 } })),
    ).toBeNull();
  });

  it("says nothing — rather than '0' — when the server did not compute it", () => {
    // `null` is "not counted" (unavailable list, or a member viewer). Turning
    // that into "0 corals" would be the absence-renders-as-success failure.
    expect(unattributedNote(payload({ unattributed_atom_count: null }))).toBeNull();
  });

  it("claims a workspace only for items actually stamped with one", () => {
    // FAILS WITHOUT THE SPLIT. Corals with neither workspace nor channel are
    // claimed identically by every connected workspace's page, so calling
    // them "from this workspace" is a claim the row does not support.
    const note = unattributedNote(
      payload({ unattributed_atom_count: { this_workspace: 0, account_legacy: 19 } }),
    );
    expect(note).toContain("19 older Slack items on this account");
    expect(note).not.toContain("from this workspace");
  });

  it("states both halves when both exist, and keeps them apart", () => {
    const note = unattributedNote(
      payload({ unattributed_atom_count: { this_workspace: 4, account_legacy: 19 } }),
    );
    expect(note).toContain("4 items from this workspace");
    expect(note).toContain("19 older Slack items on this account");
  });

  it("agrees its verb with a single item, not just its noun", () => {
    // "1 coral ... carry no channel and aren't counted" reads as a bug in the
    // product. The noun was already singular; the VERB was not.
    const note = unattributedNote(
      payload({ unattributed_atom_count: { this_workspace: 1, account_legacy: 0 } }),
    );
    expect(note).toBe(
      "1 item from this workspace carries no channel and isn't counted in any row above.",
    );
  });

  it("agrees its verb with a single LEGACY item too", () => {
    const note = unattributedNote(
      payload({ unattributed_atom_count: { this_workspace: 0, account_legacy: 1 } }),
    );
    expect(note).toContain("carries no channel and isn't counted");
  });

  it("stays plural for a joined subject even when one half is 1", () => {
    // Two clauses joined by "plus" are a plural subject whatever the counts.
    const note = unattributedNote(
      payload({ unattributed_atom_count: { this_workspace: 1, account_legacy: 1 } }),
    );
    expect(note).toContain("carry no channel and aren't counted");
  });

  it("stays plural for a single clause counting more than one", () => {
    const note = unattributedNote(
      payload({ unattributed_atom_count: { this_workspace: 0, account_legacy: 19 } }),
    );
    expect(note).toContain("carry no channel and aren't counted");
  });
});

describe("droppingAlert", () => {
  const dropping = row({
    name: "#platform",
    destination: { kind: "default", ocean_name: null, fanout: { mapped_members: 0, unmapped_members: 3 }, dropping: true, chosen_at: null, stale_name: false },
  });
  const healthy = row({
    channel_id: "C2",
    name: "#design",
    destination: { kind: "single", ocean_name: "Engineering", fanout: null, dropping: false, chosen_at: "x", stale_name: false },
  });

  it("names the channels whose messages are going nowhere", () => {
    const alert = droppingAlert(payload({ channels: { state: "ok", error: null, rows: [dropping, healthy] } }));
    expect(alert).toContain("#platform");
    expect(alert).not.toContain("#design");
  });

  it("is silent when every channel has somewhere to land", () => {
    expect(droppingAlert(payload({ channels: { state: "ok", error: null, rows: [healthy] } }))).toBeNull();
  });

  it("is silent when the channel list could not be established", () => {
    // "couldn't check" must never be rendered as "nothing is landing".
    expect(
      droppingAlert(payload({ channels: { state: "unavailable", error: "ratelimited", rows: [] } })),
    ).toBeNull();
  });
});
