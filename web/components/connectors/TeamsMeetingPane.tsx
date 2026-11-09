"use client";

import { useEffect, useState } from "react";
import { app, pages } from "@microsoft/teams-js";
import { EmbeddedMeetingCapture } from "./EmbeddedMeetingCapture";
import { TeamsSimpleApp } from "@/components/addin/TeamsSimpleApp";
import { TeamsAddMeeting } from "@/components/addin/TeamsAddMeeting";
import { HomeBody } from "@/components/addin/screens";
import type { MeetingSelector } from "@/components/addin/MeetingPane";
import { createTeamsHost } from "@/lib/addin/teamsHost";
import { createPaneClient } from "@/lib/addin/oauthClient";
import { apiBase } from "@/lib/apiBase";
import { copy } from "@/lib/addin/copy";
import { nativeMeetingCapturePath } from "@/lib/teams/meetingCapture";

/** SDK context is a selector only: saving happens after authenticated API verification. */
export function TeamsMeetingPane({ configure = false }: { configure?: boolean }) {
  const [selector, setSelector] = useState<MeetingSelector | null>(null);
  const [error, setError] = useState<string | null>(null);
  // One host + client per page load. The client's credential lives only in it (memory).
  const [{ host, client }] = useState(() => {
    const teams = createTeamsHost();
    return { host: teams, client: createPaneClient({ host: teams, apiBase, dialog: { kind: "teams" } }) };
  });
  useEffect(() => {
    let alive = true;
    void app.initialize().then(async () => {
      if (configure) {
        pages.config.registerOnSaveHandler((event) => {
          void pages.config.setConfig({
            entityId: "coralswarm-meeting-transcript", suggestedDisplayName: "CoralSwarm",
            contentUrl: "https://app.coralswarm.example/teams/meeting",
            websiteUrl: "https://app.coralswarm.example/teams/meeting",
          }).then(() => event.notifySuccess()).catch(() => event.notifyFailure("Unable to add the meeting tab."));
        });
        pages.config.setValidityState(true);
        return;
      }
      const context = await app.getContext();
      if (!context.meeting?.id) throw new Error("Open CoralSwarm from a Teams meeting to select that meeting.");
      const chatId = context.chat?.id ?? "";
      nativeMeetingCapturePath(context.meeting.id, chatId);
      if (!alive) return;
      setSelector({ teams_meeting_id: context.meeting.id, chat_id: chatId });
    }).catch((cause: unknown) => { if (alive) setError(cause instanceof Error ? cause.message : "Unable to read the meeting context."); });
    return () => { alive = false; };
  }, [configure]);

  if (configure) {
    return <main className="mx-auto max-w-xl p-5">
      <h1 className="mb-3 font-display text-3xl">Your meeting, your workspace.</h1>
      <p className="mb-4 text-sm">{copy.teams.configure}</p>
    </main>;
  }

  // The existing Clerk flow, unchanged: used when the backend has Microsoft SSO
  // off (404 microsoft_sso_disabled) or Teams cannot issue an SSO token.
  const legacy = selector
    ? <EmbeddedMeetingCapture selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={async (url) => { await app.openLink(url); }} />
    : <main className="mx-auto max-w-xl p-5">
        <h1 className="mb-3 font-display text-3xl">Your meeting, your workspace.</h1>
        <p className="mb-4 text-sm">Manage this meeting’s private transcript capture here in Teams.</p>
        {error ? <p role="alert">{error}</p> : <p role="status">Reading this meeting…</p>}
      </main>;

  return (
    <TeamsSimpleApp client={client} host={host} label="CoralSwarm meeting" fallback={legacy}>
      {(context) => selector
        ? <TeamsAddMeeting context={context} selector={selector} connectionReturnPath="/teams/meeting/connected" openAuthentication={async (url) => { await app.openLink(url); }} />
        : <>
            <HomeBody surface="meeting" message={error ? undefined : copy.meeting.teamsReading} />
            {error ? <p role="alert" className="rounded-card border border-line bg-bg px-3 py-3 text-ink-2">{error}</p> : null}
          </>}
    </TeamsSimpleApp>
  );
}
