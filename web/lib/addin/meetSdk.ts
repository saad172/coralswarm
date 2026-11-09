/* The Google Meet add-ons SDK, as the side panel uses it.
 *
 * web/app/meet/sidepanel/layout.tsx loads MEET_SDK_URL; it defines
 * `window.meet.addon`. The panel asks it for this meeting's identity and nothing
 * else: `createAddonSession({cloudProjectNumber})` -> `createSidePanelClient()`
 * -> `getMeetingInfo()` = `{meetingId: "spaces/…", meetingCode: "abc-defg-hij"}`
 * (verified live 2026-10-06). The SDK only works inside meet.google.com. */

export const MEET_SDK_URL = "https://www.gstatic.com/meetjs/addons/1.2.0/meet.addons.js";

/** Google Cloud project `coralswarm-workspace` (the add-on's deployment, extensions/meet-addon). */
export const MEET_CLOUD_PROJECT_NUMBER = "195592663815";

export interface MeetingInfo { meetingId: string; meetingCode?: string | null }

interface MeetSidePanelClient { getMeetingInfo(): Promise<MeetingInfo> }
interface MeetAddonSession { createSidePanelClient(): Promise<MeetSidePanelClient> }
export interface MeetAddonApi { createAddonSession(options: { cloudProjectNumber: string }): Promise<MeetAddonSession> }

declare global {
  interface Window { meet?: { addon?: MeetAddonApi } }
}

/** Wait (bounded) for the SDK script the layout loads. */
export async function waitForMeetSdk(timeoutMs = 10_000, win: Window = window): Promise<MeetAddonApi> {
  const started = Date.now();
  for (;;) {
    const addon = win.meet?.addon;
    if (addon) return addon;
    if (Date.now() - started > timeoutMs) throw new Error("Meet add-ons SDK did not load");
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

/** This meeting's space and code. Throws outside Meet. */
export async function readMeetingInfo(loadSdk: () => Promise<MeetAddonApi> = () => waitForMeetSdk()): Promise<MeetingInfo> {
  const addon = await loadSdk();
  const session = await addon.createAddonSession({ cloudProjectNumber: MEET_CLOUD_PROJECT_NUMBER });
  const client = await session.createSidePanelClient();
  const info = await client.getMeetingInfo();
  if (!info || typeof info.meetingId !== "string" || !info.meetingId) throw new Error("No meeting id");
  return { meetingId: info.meetingId, meetingCode: typeof info.meetingCode === "string" && info.meetingCode ? info.meetingCode : null };
}
