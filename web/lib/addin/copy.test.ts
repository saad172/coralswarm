import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { ADDON_CONTINUE_BUTTON } from "@/lib/addinApprove";
import { copy, CONTINUE_BUTTON, explainMeetingError } from "./copy";

const gmail = (file: string) => readFileSync(path.resolve(__dirname, "../../../extensions/gmail-addin", file), "utf8");
/* The Gmail add-on signs in with Clerk OAuth now (no pairing), so the pairing-only
 * strings (connect button, code, approve page, continue) are no longer shared with it;
 * the Outlook/Teams panes still pair. Strings that remain are still pinned. */
const gmailSource = ["Card.gs", "Auth.gs", "Ingest.gs", "Settings.gs"].map(gmail).join("\n");

describe("strings shared with the Gmail add-on stay byte-identical to the source of truth", () => {
  const shared: Array<[string, string]> = [
    ["revoked toast", copy.disconnect.revoked],
    ["not revoked toast (first half; Gmail concatenates)", copy.disconnect.notRevoked.split("contact support")[0]],
    ["not revoked toast (second half)", "contact support if it keeps happening."],
    ["reconnect toast", copy.reconnect],
    ["save button", copy.email.save],
    ["plus row", copy.email.plusRow],
    ["just this message", copy.email.justThis],
    ["follow switch", copy.email.follow],
    ["already saved", copy.email.alreadySaved],
    ["following again", copy.email.followOn],
    ["stopped following", copy.email.followOff],
    ["thread not in workspace", copy.email.notInOcean],
    ["mailbox switch", copy.mailbox.row],
    ["mailbox on", copy.mailbox.on],
    ["mailbox off", copy.mailbox.off],
    ["mailbox unavailable", copy.mailbox.unavailable],
    ["turn on", copy.mailbox.turnOn],
    ["cancel", copy.mailbox.cancel],
    ["mailbox row 1", copy.mailbox.rows[0].text],
    ["mailbox row 2", copy.mailbox.rows[1].text],
    ["mailbox row 2 detail", copy.mailbox.rows[1].bottom],
    ["mailbox row 3", copy.mailbox.rows[2].text],
    ["email start title hint", copy.start.email.hint],
    ["save any thread", copy.start.email.rows[0].text],
    // Apostrophe-free quote forms: Gmail uses a plain ' inside a double-quoted JS string for the continue button.
  ];
  for (const [name, value] of shared) {
    it(name, () => {
      // Gmail source has JS-escaped apostrophes and typographic quotes in a few places; normalise only those.
      const normalise = (text: string) => text.replace(/\\'/g, "'").replace(/’/g, "'");
      expect(normalise(gmailSource)).toContain(normalise(value));
    });
  }

  it("the approve page quotes the same continue button the panes render", () => {
    expect(CONTINUE_BUTTON).toBe(ADDON_CONTINUE_BUTTON);
  });
});

describe("copy rules", () => {
  const strings = (value: unknown): string[] =>
    typeof value === "string" ? [value] : Array.isArray(value) ? value.flatMap(strings) : value && typeof value === "object" ? Object.values(value).flatMap(strings) : [];
  const all = strings(copy);

  it("never uses the retired vocabulary", () => {
    for (const text of all) expect(text).not.toMatch(/\bGrow\b|\bgrowing\b|Change ocean/i);
  });

  it("never says 'capture' (Gmail says Save)", () => {
    for (const text of all) expect(text).not.toMatch(/\bcapture/i);
  });

  it("meeting status pattern", () => {
    expect(copy.meeting.status.saved.line).toBe("In your workspace · transcript saved");
    expect(copy.meeting.status.waiting.line).toBe("In your workspace · waiting for transcript");
    expect(copy.meeting.save.teams).toBe("Save this meeting");
    expect(copy.meeting.save.outlook).toBe("Save meeting to CoralSwarm");
    expect(copy.meeting.needsOrg).toBe("Choose an organization workspace to save meetings");
  });

  it("email saved line and toasts", () => {
    expect(copy.email.saved(true, 3)).toBe("In your workspace · following · 3 messages saved");
    expect(copy.email.saved(false, 1)).toBe("In your workspace · 1 message saved");
    expect(copy.email.saved(true, 0)).toBe("In your workspace · following");
    expect(copy.email.savedToast(2)).toBe("Saved to CoralSwarm · 2 messages");
    expect(copy.email.savedToast(1)).toBe("Saved to CoralSwarm · 1 message");
    expect(copy.email.wholeThread(4)).toBe("Whole thread · 4 messages");
    expect(copy.email.wholeThread(null)).toBe("Whole thread");
    expect(copy.email.meta(4, "Ada")).toBe("4 messages · Ada");
  });

  it("maps unknown error codes to the generic meeting message", () => {
    expect(explainMeetingError("nope")).toBe(copy.meeting.explain.generic);
    expect(explainMeetingError(null)).toBe(copy.meeting.explain.generic);
    for (const inherited of ["constructor", "toString", "__proto__", "hasOwnProperty"]) expect(explainMeetingError(inherited)).toBe(copy.meeting.explain.generic);
    expect(explainMeetingError("availability_window_ended")).toBe(copy.meeting.explain.availability_window_ended);
  });

  it("explains a consumer Microsoft account as a provider limit without internal detail", () => {
    const text = explainMeetingError("consumer_identity_unsupported");
    expect(text).not.toBe(copy.meeting.explain.generic);
    expect(text).toMatch(/work or school/i);
    expect(text).not.toMatch(/consumer_identity|organization_required|\bapi\b|403|graph|tenant/i);
    expect(copy.teams.personalNote).toMatch(/only you/i);
  });
});

describe("compose and recording-prompt copy", () => {
  const strings = (value: unknown): string[] =>
    typeof value === "string" ? [value] : value && typeof value === "object" ? Object.values(value).flatMap(strings) : [];
  it("never says capture or ingest to the user, and never names a clock time", () => {
    for (const text of [...strings(copy.compose), ...strings(copy.transcribe)]) {
      expect(text).not.toMatch(/captur|ingest/i);
      expect(text).not.toMatch(/\b\d{1,2}(:\d\d)?\s?(am|pm)\b/i);
    }
  });
});

describe("compose waiting-state copy", () => {
  it("covers every error_code the backend documents, for both stopped and error", () => {
    // docs/plans/outlook-compose-capture-v2.md, GET /intents/{id}
    for (const code of ["event_cancelled", "event_deleted", "series_ended", "completed", "not_organizer", "capture_stopped", "superseded"])
      expect(Object.keys(copy.compose.stoppedBecause), code).toContain(code);
    for (const code of ["not_organizer", "ambiguous_nonce", "unsupported_event", "unsupported_join_url", "already_ended"])
      expect(Object.keys(copy.compose.errors), code).toContain(code);
  });
  it("names the expiry only when one is known, and never says capture or ingest", () => {
    expect(copy.compose.waitingForLink.body("tomorrow at 9:00 AM")).toBe("Saved, but this meeting doesn’t have a Teams link yet. Add an attendee or turn on Teams meeting, then save again. CoralSwarm keeps waiting until tomorrow at 9:00 AM.");
    expect(copy.compose.waitingForLink.body(null)).not.toContain("keeps waiting");
    const strings = [copy.compose.waitingForLink.title, copy.compose.waitingForLink.body("x"), copy.compose.expiredNoLink, copy.compose.turnOnAgain,
      ...Object.values(copy.compose.errors), ...Object.values(copy.compose.stoppedBecause)];
    for (const text of strings) expect(text).not.toMatch(/\b(capture|ingest)/i);
  });
});
