// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DialogCancelledError } from "@/lib/addin/authDialog";
import { MeetApiError, MeetSignedOutError, type MeetAccount, type MeetCapture } from "@/lib/addin/meetClient";
import { MeetSidePanel, type MeetPanelClient } from "./MeetSidePanel";

afterEach(cleanup);

const MEETING = { meetingId: "spaces/2w0kwADNjNAB", meetingCode: "gwm-dvao-jhb" };
const ACCOUNT: MeetAccount = { email: "ada@acme.test", workspace: { account_id: "acc-1", name: "Acme", kind: "org" } };

function capture(over: Partial<MeetCapture> = {}): MeetCapture {
  return {
    id: "cap-1", conference_record_id: null, space_name: MEETING.meetingId, meeting_code: MEETING.meetingCode, status: "allowed",
    created_at: "2026-10-06T10:00:00Z", expires_at: "2026-11-05T10:00:00Z", ...over,
  };
}

function setup(over: Partial<MeetPanelClient> = {}, opts: { meeting?: () => Promise<typeof MEETING>; popup?: () => { closed: boolean; location: { href: string }; close(): void } | null } = {}) {
  const popup = { closed: false, location: { href: "about:blank" }, close: vi.fn() };
  const openPopup = vi.fn(opts.popup ?? (() => popup));
  const client: MeetPanelClient = {
    enabled: true,
    resume: vi.fn(async () => null),
    signIn: vi.fn(async () => ACCOUNT),
    signOut: vi.fn(),
    listCaptures: vi.fn(async () => ({ captures: [] as MeetCapture[] })),
    addCapture: vi.fn(async () => ({ capture: capture(), capture_state: "allowed", created: true })),
    removeCapture: vi.fn(async () => ({ revoked: true })),
    ...over,
  };
  render(<MeetSidePanel client={client} readMeeting={opts.meeting ?? (async () => MEETING)} openPopup={openPopup} />);
  return { client, openPopup, popup };
}

async function signIn() {
  fireEvent.click(await screen.findByRole("button", { name: "Sign in to CoralSwarm" }));
}

describe("Meet side panel", () => {
  it("not signed in: shows the question, this meeting's code, the transcripts hint and a sign-in button only", async () => {
    setup();
    expect(await screen.findByTestId("meeting-code")).toHaveTextContent("this meeting · gwm-dvao-jhb");
    expect(screen.getByRole("heading")).toHaveTextContent("Save this meeting to your ocean?");
    expect(screen.getByText("Turn on transcripts")).toBeInTheDocument();
    expect(screen.getByText(/Activities → Transcripts/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Sign in to CoralSwarm" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Add to CoralSwarm" })).toBeNull();
    expect(screen.getByText("Only your own copy. Others in this meeting choose for themselves.")).toBeInTheDocument();
  });

  it("a remembered sign-in (the next meeting) shows the account and Add without a click or a popup", async () => {
    const { client, openPopup } = setup({ resume: vi.fn(async () => ACCOUNT) });
    expect(await screen.findByTestId("workspace-name")).toHaveTextContent("Acme");
    expect(await screen.findByRole("button", { name: "Add to CoralSwarm" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Sign in to CoralSwarm" })).toBeNull();
    expect(openPopup).not.toHaveBeenCalled();
    expect(client.signIn).not.toHaveBeenCalled();
    expect(client.listCaptures).toHaveBeenCalledTimes(1);
  });

  it("while the sign-in is being restored there is no sign-in button; nothing remembered shows it", async () => {
    let settle!: (account: MeetAccount | null) => void;
    setup({ resume: vi.fn(() => new Promise<MeetAccount | null>((resolve) => { settle = resolve; })) });
    await screen.findByTestId("meeting-code");
    expect(screen.getByRole("status")).toHaveTextContent("Restoring your sign-in…");
    expect(screen.queryByRole("button", { name: "Sign in to CoralSwarm" })).toBeNull();
    settle(null);
    expect(await screen.findByRole("button", { name: "Sign in to CoralSwarm" })).toBeEnabled();
    expect(screen.queryByRole("status")).toBeNull();
  });

  it("opens the popup synchronously in the click, then signs in and shows the account footer", async () => {
    const { client, openPopup, popup } = setup();
    await signIn();
    expect(openPopup).toHaveBeenCalledTimes(1);
    expect(client.signIn).toHaveBeenCalledWith(popup, {});
    expect(await screen.findByTestId("workspace-name")).toHaveTextContent("Acme");
    expect(screen.getByText("ada@acme.test")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Add to CoralSwarm" })).toBeEnabled();
  });

  it("a cancelled sign-in says so and offers Try again; a blocked popup says to allow pop-ups", async () => {
    setup({ signIn: vi.fn(async () => { throw new DialogCancelledError(); }) });
    await signIn();
    expect(await screen.findByRole("alert")).toHaveTextContent("Sign-in was cancelled.");
    expect(screen.getByRole("button", { name: "Try again" })).toBeInTheDocument();
    cleanup();
    setup({}, { popup: () => null });
    await signIn();
    expect(await screen.findByRole("alert")).toHaveTextContent(/blocked the sign-in window/);
  });

  it("add: POSTs this meeting's space, then shows Added with Undo; Undo DELETEs the capture and returns to Add", async () => {
    const { client } = setup();
    await signIn();
    fireEvent.click(await screen.findByRole("button", { name: "Add to CoralSwarm" }));
    expect(client.addCapture).toHaveBeenCalledWith("spaces/2w0kwADNjNAB");
    expect(await screen.findByText("✓ Added")).toBeInTheDocument();
    expect(screen.getByText("Saves a few minutes after the meeting ends.")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Undo" }));
    await waitFor(() => expect(client.removeCapture).toHaveBeenCalledWith("cap-1"));
    expect(await screen.findByRole("button", { name: "Add to CoralSwarm" })).toBeInTheDocument();
  });

  it("existing capture: on load, an open approval for this space (or code) shows Added", async () => {
    setup({ listCaptures: vi.fn(async () => ({ captures: [capture({ id: "other", space_name: "spaces/zzz", meeting_code: "aaa-bbbb-ccc" }), capture({ id: "mine", space_name: null })] })) });
    await signIn();
    expect(await screen.findByText("✓ Added")).toBeInTheDocument();
  });

  it("an ingested or another meeting's capture does not count as added", async () => {
    setup({ listCaptures: vi.fn(async () => ({ captures: [capture({ status: "ingested" }), capture({ space_name: "spaces/other", meeting_code: "xyz-abcd-efg" })] })) });
    await signIn();
    expect(await screen.findByRole("button", { name: "Add to CoralSwarm" })).toBeInTheDocument();
  });

  it("409 google_meet_not_connected: shows Connect Google Meet opening /connectors in a new tab", async () => {
    setup({ listCaptures: vi.fn(async () => { throw new MeetApiError(409, "google_meet_not_connected"); }) });
    await signIn();
    const link = await screen.findByRole("link", { name: "Connect Google Meet" });
    expect(link).toHaveAttribute("href", "/connectors");
    expect(link).toHaveAttribute("target", "_blank");
    expect(screen.queryByRole("button", { name: "Add to CoralSwarm" })).toBeNull();
  });

  it("409 on add also lands on Connect Google Meet", async () => {
    setup({ addCapture: vi.fn(async () => { throw new MeetApiError(409, "google_meet_not_connected"); }) });
    await signIn();
    fireEvent.click(await screen.findByRole("button", { name: "Add to CoralSwarm" }));
    expect(await screen.findByRole("link", { name: "Connect Google Meet" })).toBeInTheDocument();
  });

  it("403 org disabled: says Google Meet saving is off for this workspace", async () => {
    setup({ listCaptures: vi.fn(async () => { throw new MeetApiError(403, "org_google_meet_disabled"); }) });
    await signIn();
    expect(await screen.findByRole("alert")).toHaveTextContent("Google Meet saving isn’t turned on for Acme");
    expect(screen.queryByRole("button", { name: "Add to CoralSwarm" })).toBeNull();
  });

  it("undo after the copy landed (409 already_ingested) keeps Added and explains", async () => {
    setup({
      listCaptures: vi.fn(async () => ({ captures: [capture()] })),
      removeCapture: vi.fn(async () => { throw new MeetApiError(409, "already_ingested"); }),
    });
    await signIn();
    fireEvent.click(await screen.findByRole("button", { name: "Undo" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("already saved");
    expect(screen.getByText("✓ Added")).toBeInTheDocument();
  });

  it("a lost sign-in returns to the sign-in card", async () => {
    const { client } = setup({ addCapture: vi.fn(async () => { throw new MeetSignedOutError(); }) });
    await signIn();
    fireEvent.click(await screen.findByRole("button", { name: "Add to CoralSwarm" }));
    expect(await screen.findByRole("button", { name: "Sign in to CoralSwarm" })).toBeInTheDocument();
    expect(client.signOut).toHaveBeenCalled();
  });

  it("⋮ menu: Change workspace re-runs sign-in with prompt=consent; Sign out clears the session", async () => {
    const { client, openPopup } = setup();
    await signIn();
    await screen.findByTestId("workspace-name");
    const menu = screen.getByRole("button", { name: "More" });
    fireEvent.mouseDown(menu); fireEvent.click(menu);
    fireEvent.click(screen.getByRole("menuitem", { name: "Change workspace" }));
    expect(openPopup).toHaveBeenCalledTimes(2);
    expect(client.signIn).toHaveBeenLastCalledWith(expect.anything(), { prompt: "consent" });
    await waitFor(() => expect(client.listCaptures).toHaveBeenCalledTimes(2));
    fireEvent.mouseDown(menu); fireEvent.click(menu);
    fireEvent.click(screen.getByRole("menuitem", { name: "Sign out" }));
    expect(client.signOut).toHaveBeenCalled();
    expect(await screen.findByRole("button", { name: "Sign in to CoralSwarm" })).toBeInTheDocument();
    expect(screen.queryByTestId("workspace-name")).toBeNull();
  });

  it("outside a Meet call (SDK unavailable) says where to open it", async () => {
    setup({}, { meeting: async () => { throw new Error("Meet add-ons SDK did not load"); } });
    expect(await screen.findByRole("alert")).toHaveTextContent(/Google Meet call/);
    expect(screen.queryByRole("button", { name: "Sign in to CoralSwarm" })).toBeNull();
  });

  it("a build without the Meet client id says sign-in is unavailable and opens no popup", async () => {
    const { openPopup } = setup({ enabled: false });
    await signIn();
    expect(openPopup).not.toHaveBeenCalled();
    expect(await screen.findByRole("alert")).toHaveTextContent(/isn’t available/);
  });
});
