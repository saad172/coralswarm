// @vitest-environment jsdom
import { describe, expect, it, vi } from "vitest";
import { appointmentJoinUrl, readAppointmentJoinUrl, supportedJoinUrl } from "./appointment";
import type { OfficeApi, OfficeMailItem } from "./office";

// Ported from extensions/outlook-addin/scripts/test-meeting.js (selector half).
const primary = '<a id="meet_invite_block.action.join_link" href="https://teams.microsoft.com/meet/123?p=secret">Join</a>';
const reference = '<a id="meet_invite_block.action.join_link_compatibility" href="https://teams.microsoft.com/l/meetup-join/meeting-a/0">System reference</a>';

describe("appointmentJoinUrl", () => {
  it("takes Microsoft's marked primary join link and ignores its marked system reference", () => {
    expect(appointmentJoinUrl(primary + reference, document)).toBe("https://teams.microsoft.com/meet/123?p=secret");
    expect(appointmentJoinUrl((primary + reference).replaceAll('id="meet_', 'id="x_meet_'), document)).toBe("https://teams.microsoft.com/meet/123?p=secret");
  });

  it.each([
    ["a second visible meeting link", primary + reference + '<a href="https://teams.microsoft.com/meet/456">Other meeting</a>'],
    ["a second legacy link", primary + reference + '<a href="https://teams.microsoft.com/l/meetup-join/meeting-b/0">Other legacy meeting</a>'],
    ["a second link in text", primary + reference + "<p>https://teams.microsoft.com/meet/456</p>"],
    ["a duplicated reference", primary + reference + reference],
    ["a duplicated primary", primary + primary + reference],
    ["a reference that is not a legacy join", primary + reference.replace("/l/meetup-join/meeting-a/0", "/meet/456")],
    ["a foreign-host reference", primary + reference.replace("teams.microsoft.com", "evil.test")],
    ["a fragment reference", primary + reference.replace("meeting-a/0", "meeting-a/0#fragment")],
    ["a second legacy link in text", primary + reference + "<p>https://teams.microsoft.com/l/meetup-join/meeting-b/0</p>"],
    ["an unknown marker", primary + reference.replace("join_link_compatibility", "unknown_reference")],
    ["two bare links", '<a href="https://teams.microsoft.com/meet/123">A</a><a href="https://teams.microsoft.com/meet/456">B</a>'],
  ])("refuses %s", (_name, html) => {
    expect(() => appointmentJoinUrl(html, document)).toThrow(/multiple Teams/);
  });

  it("accepts a lone link wherever it is, without echoing the body", () => {
    expect(appointmentJoinUrl('<p>private appointment body</p><a href="https://teams.microsoft.com/meet/123?p=secret">Join</a>', document)).toBe("https://teams.microsoft.com/meet/123?p=secret");
    expect(appointmentJoinUrl('<a href="https://teams.cloud.microsoft/meet/123">Join</a>', document)).toBe("https://teams.cloud.microsoft/meet/123");
    expect(appointmentJoinUrl('<p><a href="https://teams.microsoft.com/meet/123?p=secret">https://teams.microsoft.com/meet/123?p=secret</a></p><p>Meeting ID: 123</p>', document)).toBe("https://teams.microsoft.com/meet/123?p=secret");
    expect(document.querySelector("a")).toBeNull(); // the body was never inserted into the page
  });

  it("says so when there is no Teams meeting", () => {
    expect(() => appointmentJoinUrl("<p>ordinary appointment</p>", document)).toThrow(/does not contain/);
  });
});

describe("supportedJoinUrl", () => {
  it.each(["http://teams.microsoft.com/meet/123", "https://teams.microsoft.com.evil.test/meet/123", "https://teams.live.com/meet/123", "https://user@teams.microsoft.com/meet/123", "https://teams.microsoft.com:444/meet/123"])("rejects %s", (candidate) => {
    expect(supportedJoinUrl(candidate)).toBeNull();
  });
});

describe("readAppointmentJoinUrl", () => {
  const office = { CoercionType: { Html: "html" }, AsyncResultStatus: { Succeeded: "ok" } } as unknown as OfficeApi;
  it("reads the body exactly once and resolves only the join URL", async () => {
    const getAsync = vi.fn((_type: string, cb: (r: { status: string; value: string }) => void) => cb({ status: "ok", value: "<p>private subject and attendees</p>" + primary.replace("?p=secret", "") + reference }));
    const item = { body: { getAsync } } as unknown as OfficeMailItem;
    await expect(readAppointmentJoinUrl(office, item, document)).resolves.toBe("https://teams.microsoft.com/meet/123");
    expect(getAsync).toHaveBeenCalledTimes(1);
    expect(getAsync.mock.calls[0][0]).toBe("html");
  });

  it("rejects when Outlook cannot read the body", async () => {
    const item = { body: { getAsync: (_t: string, cb: (r: { status: string; value: string }) => void) => cb({ status: "failed", value: "" }) } } as unknown as OfficeMailItem;
    await expect(readAppointmentJoinUrl(office, item, document)).rejects.toThrow(/could not read/);
  });
});
