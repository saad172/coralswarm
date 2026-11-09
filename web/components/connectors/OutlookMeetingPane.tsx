"use client";

import { useEffect, useState } from "react";
import dynamic from "next/dynamic";
import { AddinApp } from "@/components/addin/AddinApp";
import { MeetingPane } from "@/components/addin/MeetingPane";
import { OutlookComposePane } from "@/components/addin/OutlookComposePane";
import { HomeBody } from "@/components/addin/screens";
import { useOfficeItem } from "@/components/addin/useOfficeItem";
import { readAppointmentJoinUrl } from "@/lib/addin/appointment";
import { copy } from "@/lib/addin/copy";
import { createPaneClient } from "@/lib/addin/oauthClient";
import { createOutlookHost } from "@/lib/addin/outlookHost";
import { apiBase } from "@/lib/apiBase";
import { meetingCapturePath } from "@/lib/teams/meetingCapture";

import { mark } from "@/lib/addin/perf";

// Clerk-based capture is only the fallback for SSO off/unavailable: out of the first-load bundle.
const EmbeddedMeetingCapture = dynamic(() => import("./EmbeddedMeetingCapture").then((m) => m.EmbeddedMeetingCapture), { ssr: false });

mark("pane-module");

/** The Outlook appointment task pane (route /outlook/meeting).
 *
 *  It runs directly in the task pane. It used to sit in an iframe under a
 *  static wrapper page that parsed the appointment; Office SSO / nested app auth
 *  does not work from a nested iframe, so the pane now owns Office.js itself.
 *  The parsing rules did not change (lib/addin/appointment.ts): the appointment
 *  body stays local and only one validated Teams join URL leaves it. */
export function OutlookMeetingPane() {
  const [{ host, client }] = useState(() => {
    const outlook = createOutlookHost();
    void outlook.warm(); // office.js, MSAL and the NAA client start now, in parallel, not one after another
    return { host: outlook, client: createPaneClient({ host: outlook, apiBase, dialog: { kind: "outlook", office: outlook.office } }) };
  });
  const { office, item } = useOfficeItem(host.office);
  const [selection, setSelection] = useState<{ join_url: string } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const itemKey = item.kind === "appointment" ? item.key : item.kind;
  // An unsaved draft has no saved Teams meeting to read yet: it gets its own flow.
  const composing = item.kind === "appointment" && item.compose;

  useEffect(() => {
    setSelection(null); setError(null);
    if (!office || item.kind === "loading") return;
    const current = office.context.mailbox.item;
    if (composing) return;
    if (item.kind !== "appointment" || !current) { setError(copy.meeting.outlookNoAppointment); return; }
    let live = true;
    readAppointmentJoinUrl(office, current, document).then((joinUrl) => {
      if (!live) return;
      try { meetingCapturePath(joinUrl); setSelection({ join_url: joinUrl }); }
      catch { setError("This appointment did not provide one supported Teams meeting."); }
    }).catch((cause: unknown) => { if (live) setError(cause instanceof Error ? cause.message : "Outlook could not read this appointment’s meeting link."); });
    return () => { live = false; };
    // `item` is a fresh object per ItemChanged read; its key is what identifies the appointment.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [office, itemKey]);

  const openAuthentication = (url: string) => host.openLink(url);
  // The existing Clerk flow, unchanged, for SSO off / unavailable.
  const legacy = composing
    ? <p role="status" className="p-5 text-sm">{copy.compose.unsupportedFallback}</p>
    : selection
    ? <EmbeddedMeetingCapture selector={selection} connectionReturnPath="/outlook/meeting/connected" openAuthentication={openAuthentication} />
    : <p role="status" className="p-5 text-sm">{error ?? copy.meeting.outlookReading}</p>;

  return (
    <AddinApp client={client} host={host} surface="meeting" label="CoralSwarm meeting" fallback={legacy}>
      {(context) => composing && office
        ? <OutlookComposePane key={itemKey} context={context} office={office} openAuthentication={openAuthentication} />
        : selection
        ? <MeetingPane context={context} selector={selection} host="outlook" connectionReturnPath="/outlook/meeting/connected" openAuthentication={openAuthentication} />
        : <HomeBody surface="meeting" message={error ?? copy.meeting.outlookReading} alert={Boolean(error)} />}
    </AddinApp>
  );
}
