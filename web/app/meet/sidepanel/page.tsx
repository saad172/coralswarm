"use client";

import { useMemo } from "react";
import { MeetSidePanel } from "@/components/addin/MeetSidePanel";
import { apiBase } from "@/lib/apiBase";
import { createMeetClient } from "@/lib/addin/meetClient";
import { readMeetingInfo } from "@/lib/addin/meetSdk";

const readMeeting = () => readMeetingInfo();

export default function MeetSidePanelPage() {
  const client = useMemo(() => createMeetClient({ apiBase }), []);
  return <MeetSidePanel client={client} readMeeting={readMeeting} />;
}
