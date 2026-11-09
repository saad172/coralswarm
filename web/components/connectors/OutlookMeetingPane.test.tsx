// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { OutlookMeetingPane } from "./OutlookMeetingPane";
import { api } from "@/lib/api";
import type { OfficeApi, OfficeMailItem } from "@/lib/addin/office";

const h = vi.hoisted(() => ({ office: null as unknown, token: vi.fn(), openLink: vi.fn(), status: vi.fn(), create: vi.fn(), intentStatus: vi.fn() }));
vi.mock("@/lib/addin/outlookHost", () => ({
  createOutlookHost: () => ({ name: "outlook", office: async () => h.office, getMicrosoftToken: () => h.token(), openLink: h.openLink, warm: async () => {} }),
}));
vi.mock("./EmbeddedMeetingCapture", () => ({ EmbeddedMeetingCapture: ({ selector }: { selector: unknown }) => <section aria-label="Clerk capture panel">{JSON.stringify(selector)}</section> }));
vi.mock("@/lib/api", async () => ({ ...(await vi.importActual<typeof import("@/lib/api")>("@/lib/api")), api: { outlookCaptureSchedule: { create: h.create, status: h.intentStatus, cancel: vi.fn() }, msTeams: { meetingStatus: h.status, captureMeeting: vi.fn(), stopMeetingSelection: vi.fn(), connectMeeting: vi.fn() } } }));

const join = '<p>private subject and attendees</p><a href="https://teams.microsoft.com/meet/123?p=secret">Join</a>';
const appointment = (html = join): OfficeMailItem => ({ itemType: "appointment", itemId: "appt-1", body: { getAsync: (_t: string, cb: (r: { status: string; value: string }) => void) => cb({ status: "ok", value: html }) } });
function fakeOffice(item: OfficeMailItem | null) {
  let handler: (() => void) | null = null;
  const office = {
    EventType: { ItemChanged: "itemChanged" }, CoercionType: { Html: "html" }, AsyncResultStatus: { Succeeded: "ok" },
    MailboxEnums: { ItemType: { Message: "message", Appointment: "appointment" }, RestVersion: { v2_0: "v2.0" } },
    context: { requirements: { isSetSupported: () => false }, mailbox: { item, convertToRestId: (id: string) => id, addHandlerAsync: (_e: string, cb: () => void) => { handler = cb; }, removeHandlerAsync: vi.fn() } },
  } as unknown as OfficeApi;
  return { office, change: (next: OfficeMailItem | null) => { office.context.mailbox.item = next; act(() => handler?.()); } };
}
const connected = (kind: "org" | "personal") => ({ status: "connected", token: "cs-1", expires_at: "2099-01-01T00:00:00Z", account: { email: "ada@acme.test" }, workspace: { account_id: "a", name: kind === "org" ? "Acme ISD" : "Personal", kind } });
const stubSession = (body: unknown, status = 200) => vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(body), { status })));

beforeEach(() => {
  vi.clearAllMocks(); h.token.mockResolvedValue("ms-token");
  h.status.mockResolvedValue({ connection_required: false, availability_window_open: true, meeting_id: "verified", title: "Budget review", occurrence_start: "", occurrence_end: "", capture: null });
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });

it("runs directly in the task pane (no iframe, no postMessage bridge) and offers 'Save meeting to CoralSwarm' for the parsed join URL", async () => {
  h.office = fakeOffice(appointment()).office; stubSession(connected("org"));
  const { container } = render(<OutlookMeetingPane />);
  expect(await screen.findByRole("button", { name: "Save meeting to CoralSwarm" })).toBeVisible();
  expect(container.querySelector("iframe")).toBeNull();
  expect(h.status.mock.calls[0][0]).toBe("cs-1");
  expect(h.status.mock.calls[0][1]).toEqual({ join_url: "https://teams.microsoft.com/meet/123?p=secret" });
  expect(JSON.stringify(h.status.mock.calls)).not.toContain("private subject");
});

it("a personal workspace reaches the saved-meeting consent with private-owner wording and saves nothing on open", async () => {
  h.office = fakeOffice(appointment()).office; stubSession(connected("personal"));
  render(<OutlookMeetingPane />);
  expect(await screen.findByRole("button", { name: "Save meeting to CoralSwarm" })).toBeVisible();
  expect(screen.getByText(/Only you can see this transcript, in your personal workspace/)).toBeVisible();
  expect(h.status.mock.calls[0][0]).toBe("cs-1");
  expect(api.msTeams.captureMeeting).not.toHaveBeenCalled();
});

it("clears and re-reads when the appointment changes", async () => {
  const fake = fakeOffice(appointment()); h.office = fake.office; stubSession(connected("org"));
  render(<OutlookMeetingPane />);
  await screen.findByRole("button", { name: "Save meeting to CoralSwarm" });
  fake.change({ ...appointment('<a href="https://teams.microsoft.com/meet/999">Join</a>'), itemId: "appt-2" });
  await vi.waitFor(() => expect(h.status.mock.calls.at(-1)?.[1]).toEqual({ join_url: "https://teams.microsoft.com/meet/999" }));
});

it("refuses an appointment with several Teams meetings and shows why", async () => {
  h.office = fakeOffice(appointment('<a href="https://teams.microsoft.com/meet/123">A</a><a href="https://teams.microsoft.com/meet/456">B</a>')).office; stubSession(connected("org"));
  render(<OutlookMeetingPane />);
  expect(await screen.findByRole("alert")).toHaveTextContent("multiple Teams meetings");
  expect(screen.queryByRole("button", { name: "Save meeting to CoralSwarm" })).not.toBeInTheDocument();
  expect(h.status).not.toHaveBeenCalled();
});

it("a message (not an appointment) gets the open-from-a-Teams-appointment hint", async () => {
  h.office = fakeOffice({ itemType: "message", itemId: "m" }).office; stubSession(connected("org"));
  render(<OutlookMeetingPane />);
  expect(await screen.findByRole("alert")).toHaveTextContent("Open this from a Teams appointment.");
});

it("flag off: the existing Clerk pane renders with the parsed selector", async () => {
  h.office = fakeOffice(appointment()).office; stubSession({ error: "microsoft_sso_disabled" }, 404);
  render(<OutlookMeetingPane />);
  expect(await screen.findByRole("region", { name: "Clerk capture panel" })).toHaveTextContent("https://teams.microsoft.com/meet/123?p=secret");
});

const draft = (): OfficeMailItem => {
  const data: Record<string, string> = {};
  const props = { get: (k: string) => data[k], set: (k: string, v: string) => { data[k] = v; }, remove: (k: string) => { delete data[k]; }, saveAsync: (cb: (r: { status: string; value: unknown }) => void) => cb({ status: "ok", value: undefined }) };
  return { itemType: "appointment", subject: { getAsync() {} }, loadCustomPropertiesAsync: (cb: (r: { status: string; value: unknown }) => void) => cb({ status: "ok", value: props }),
    body: { getAsync: () => { throw new Error("the draft body must not be read"); } } } as unknown as OfficeMailItem;
};

it("an unsaved draft opens the compose flow (no 'save the Teams meeting first' dead end, body never read)", async () => {
  h.office = fakeOffice(draft()).office; stubSession(connected("org"));
  h.create.mockResolvedValue({ intent_id: "22222222-2222-4222-a222-222222222222", nonce: "11111111-1111-4111-a111-111111111111", expires_at: "2099-01-01T00:00:00Z" });
  h.intentStatus.mockResolvedValue({ status: "pending_save", error_code: null, recurring: false, next_occurrence_start: null, auto_transcription: null, manual_reason: null });
  render(<OutlookMeetingPane />);
  const save = await screen.findByRole("button", { name: "Save this meeting to CoralSwarm" });
  expect(screen.queryByText(/Save the Teams meeting first/)).not.toBeInTheDocument();
  expect(h.status).not.toHaveBeenCalled();
  await act(async () => { save.click(); });
  expect(await screen.findByText(/^On — CoralSwarm saves the transcript after you save or send/)).toBeVisible();
});

it("SSO off + unsaved draft: the Clerk flow cannot help, so the message says to save then reopen", async () => {
  h.office = fakeOffice(draft()).office; stubSession({ error: "microsoft_sso_disabled" }, 404);
  render(<OutlookMeetingPane />);
  expect(await screen.findByText("Save the meeting, then reopen CoralSwarm.")).toBeVisible();
});

const NONE = "This appointment does not contain a supported Teams meeting link. Save the Teams meeting first, then reopen this action.";

it("a saved appointment with no supported Teams link shows the message exactly once, in the plus-row", async () => {
  h.office = fakeOffice(appointment("<p>Lunch, no link</p>")).office; stubSession(connected("org"));
  render(<OutlookMeetingPane />);
  expect(await screen.findByRole("alert")).toHaveTextContent(NONE);
  expect(screen.getAllByText(NONE)).toHaveLength(1);
  expect(screen.getAllByRole("alert")).toHaveLength(1);
});

it("a message opened in the meeting pane shows its hint exactly once", async () => {
  h.office = fakeOffice({ itemType: "message", itemId: "m" }).office; stubSession(connected("org"));
  render(<OutlookMeetingPane />);
  await screen.findByRole("alert");
  expect(screen.getAllByText("Open this from a Teams appointment.")).toHaveLength(1);
});

it("an appointment with several Teams meetings shows its message exactly once", async () => {
  h.office = fakeOffice(appointment('<a href="https://teams.microsoft.com/meet/123">A</a><a href="https://teams.microsoft.com/meet/456">B</a>')).office; stubSession(connected("org"));
  render(<OutlookMeetingPane />);
  await screen.findByRole("alert");
  expect(screen.getAllByText(/multiple Teams meetings/)).toHaveLength(1);
});
