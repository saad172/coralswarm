import Script from "next/script";
import type { Metadata } from "next";
import { MEET_SDK_URL } from "@/lib/addin/meetSdk";

/* The Google Meet side panel (extensions/meet-addon's sidePanelUrl). Loads the
 * Meet add-ons SDK, which defines window.meet.addon; the panel waits for it
 * (lib/addin/meetSdk.ts). Framed by meet.google.com only (next.config.ts). */

export const metadata: Metadata = { title: "CoralSwarm for Google Meet" };

export default function MeetSidePanelLayout({ children }: { children: React.ReactNode }) {
  return (
    <>
      <Script src={MEET_SDK_URL} strategy="afterInteractive" />
      {children}
    </>
  );
}
