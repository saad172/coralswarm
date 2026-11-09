/* Every user-facing string of the Teams and Outlook add-in panes, in the Gmail
 * add-on's voice (extensions/gmail-addin/Card.gs, Pairing.gs, Ingest.gs,
 * Settings.gs). One file so the panes cannot drift from each other or from
 * Gmail, and so copy has real unit coverage (copy.test.ts pins the strings
 * that other surfaces quote byte-for-byte).
 *
 * Vocabulary: "Save" (never "capture" / "Grow"), "your ocean", "workspace"
 * (never "Change ocean"). */

export type Surface = "email" | "meeting";

/** Pairing.gs button label. The approve page quotes it (lib/addinApprove.ts). */
export const CONTINUE_BUTTON = "I've approved — continue";

export const copy = {
  connectButton: "Connect CoralSwarm",
  startOver: "Start over",
  openApproval: "Open approval page",
  retry: "Try again",

  start: {
    email: {
      title: "Your inbox, in your workspace",
      hint: "Save any thread with one tap. Replies follow on their own.",
      rows: [
        { icon: "bookmark", hue: "coral", text: "Save any thread", bottom: "The whole conversation, one tap." },
        { icon: "sync", hue: "teal", text: "Replies follow automatically" },
        { icon: "lock", hue: "plum", text: "Nothing to copy", bottom: "Approve once in your browser." },
      ],
    },
    meeting: {
      title: "Your meetings, in your workspace",
      hint: "Save a meeting with one tap. Its transcript follows on its own.",
      rows: [
        { icon: "bookmark", hue: "coral", text: "Save any meeting", bottom: "The transcript, one tap." },
        { icon: "sync", hue: "teal", text: "Transcripts follow automatically" },
        { icon: "lock", hue: "plum", text: "Nothing to copy", bottom: "Approve once in your browser." },
      ],
    },
  },

  /** Clerk OAuth sign-in (rollout-gated; replaces the pairing screens when on). */
  signIn: {
    button: "Sign in to CoralSwarm",
    cancelled: "Sign-in was cancelled.",
    failed: "Sign-in did not finish. Try again.",
    signedOut: "Signed out of CoralSwarm.",
    changeHint: "Pick your workspace in the window. Don’t see it? Sign out there and sign in again.",
    hint: "Sign in once in a small window, then come back to this pane.",
  },

  pairing: {
    codeLabel: "Pairing code",
    codeHint: "The approval page shows the same code. If it doesn’t, start over.",
    stepsHeader: "Approve in your browser",
    steps: [
      { icon: "step-1", text: "Open the approval page", bottom: "Sign in if asked, then pick your workspace." },
      { icon: "step-2", text: "Tap Approve", bottom: "Check the code matches before you do." },
      { icon: "step-3", text: "Come back and continue", bottom: "The button below finishes the connection." },
    ],
    continue: CONTINUE_BUTTON,
    notApproved: "Not approved yet — approve in your browser, then tap this again.",
    expired: "Pairing expired — start over.",
    couldNotStart: "Could not start pairing. Try again in a moment.",
  },

  home: {
    email: {
      hint: "Open a message to save its thread",
      title: "Nothing from Outlook yet",
      body: "Threads you save show up in your workspace and on Today.",
    },
    meeting: {
      hint: "Open a meeting to save it",
      title: "Nothing from Teams yet",
      body: "Meetings you save show up in your workspace and on Today.",
    },
  },

  account: { connected: "Connected", connectedAs: (email: string) => `Connected as ${email}`, workspace: (name: string) => `Workspace: ${name}` },
  menu: { label: "More", changeWorkspace: "Change workspace", disconnect: "Disconnect" },
  workspace: {
    title: "Change workspace",
    personal: "Personal",
    organization: "Organization",
    loading: "Loading your workspaces…",
    failed: "Could not load your workspaces. Try again in a moment.",
    changed: (name: string) => `Workspace changed to ${name}`,
    changeFailed: "Could not change workspace. Try again in a moment.",
    close: "Close",
  },
  disconnect: {
    revoked: "Disconnected — the CoralSwarm credential was revoked.",
    notRevoked: "Disconnected here, but revoking the server credential failed. Try again, or contact support if it keeps happening.",
  },
  reconnect: "CoralSwarm needs to be reconnected.",
  connectedToast: (email: string) => `Connected as ${email}`,
  booting: "Connecting to CoralSwarm…",
  bootFailed: "Could not reach CoralSwarm. Check your connection and try again.",
  invalidMicrosoft: "CoralSwarm couldn’t verify your Microsoft sign-in. Try again.",

  /** The Teams side panel: log in, then add the meeting. Nothing else. */
  teams: {
    logIn: "Log in",
    loginTitle: "Add your meeting to CoralSwarm",
    loginHint: "Log in once, then add this meeting with one tap.",
    pairingHint: "Approve in your browser, then come back.",
    microsoftHint: "Finished signing in with Microsoft? Come back to this pane.",
    addMeeting: "Add meeting",
    adding: "Adding…",
    added: "Added to CoralSwarm",
    recordingOffTitle: "Turn on recording or transcription to add CoralSwarm",
    recordingOffSteps: "More (…) → Record and transcribe → Start transcription",
    personalNote: "Only you can see this transcript, in your personal workspace.",
    addFailed: "Couldn’t add this meeting. Try again.",
    checkFailed: "Couldn’t reach this meeting. Try again.",
    windowEnded: "This meeting’s transcript window has closed.",
    configure: "Adding CoralSwarm lets you save this meeting’s transcript to your workspace.",
  },

  /** The Google Meet side panel (web/app/meet/sidepanel): approve THIS meeting, from inside it. */
  meetPanel: {
    label: "CoralSwarm for Google Meet",
    hostTag: "Google Meet",
    question: "Save this meeting to your",
    questionOcean: "ocean",
    thisMeeting: (code: string) => `this meeting · ${code}`,
    transcriptsTitle: "Turn on transcripts",
    transcriptsHint: "Activities → Transcripts (or ask the host). Nothing can be saved without one.",
    add: "Add to CoralSwarm",
    adding: "Adding…",
    added: "✓ Added",
    addedHint: "Saves a few minutes after the meeting ends.",
    undo: "Undo",
    undoing: "Undoing…",
    ownCopy: "Only your own copy. Others in this meeting choose for themselves.",
    checking: "Checking this meeting…",
    loadingMeeting: "Reading this meeting…",
    notInMeeting: "Open CoralSwarm from the Activities panel of a Google Meet call.",
    signInHint: "Sign in once in a small window. You stay signed in on this browser until you sign out.",
    resuming: "Restoring your sign-in…",
    signInUnavailable: "Sign-in isn’t available in this build of CoralSwarm.",
    popupBlocked: "Your browser blocked the sign-in window. Allow pop-ups for meet.google.com and try again.",
    notConnected: "Connect Google Meet to CoralSwarm first. Connecting doesn’t save anything.",
    connect: "Connect Google Meet",
    orgDisabled: (workspace: string) => `Google Meet saving isn’t turned on for ${workspace}. Ask your admin, or switch to your personal workspace.`,
    forbidden: "This workspace can’t save Google Meet meetings. Try another workspace.",
    addFailed: "Couldn’t add this meeting. Try again.",
    undoFailed: "Couldn’t undo. Try again.",
    alreadySaved: "This meeting was already saved, so it can’t be undone here.",
    checkFailed: "Couldn’t check this meeting. Try again.",
    signOut: "Sign out",
  },

  email: {
    plusRow: "Save this thread to your workspace",
    fallbackSubject: "This thread",
    fallbackMeta: "The whole conversation, and replies follow automatically.",
    wholeThread: (count: number | null) => (count ? `Whole thread · ${count} ${count === 1 ? "message" : "messages"}` : "Whole thread"),
    justThis: "Just this message",
    follow: "Keep saving new replies",
    save: "Save to CoralSwarm",
    saving: "Saving…",
    meta: (count: number | null, from: string | null) => {
      const parts: string[] = [];
      if (count) parts.push(`${count} ${count === 1 ? "message" : "messages"}`);
      if (from) parts.push(from);
      return parts.join(" · ");
    },
    saved: (following: boolean, count: number) =>
      `In your workspace${following ? " · following" : ""}${count >= 1 ? ` · ${count} ${count === 1 ? "message saved" : "messages saved"}` : ""}`,
    savedToast: (count: number) => `Saved to CoralSwarm · ${count} ${count === 1 ? "message" : "messages"}`,
    alreadySaved: "Already in your workspace — nothing new to save.",
    couldNotSave: (status: number) => `Could not save this thread (${status}).`,
    couldNotRead: "Couldn’t read this message. Try reopening the pane.",
    unreachable: "Couldn’t reach CoralSwarm. Try again in a moment.",
    unconfirmed: "Couldn’t confirm this save. Try again.",
    noMessage: "No message is open.",
    refused: (workspace: string) => `CoralSwarm refused this for ${workspace}. Try changing your workspace.`,
    followOn: "Following again",
    followOff: "Stopped following",
    notInOcean: "This thread is not in your workspace yet — save it first.",
    couldNotFollow: (status: number) => `Could not change following (${status}). Try again in a moment.`,
  },

  mailbox: {
    row: "Ingest my whole mailbox",
    confirmHeader: "Ingest my whole mailbox",
    rows: [
      { icon: "inbox", hue: "coral", text: "Every message, including existing mail", bottom: "CoralSwarm pulls your mailbox in the background and keeps it in sync." },
      { icon: "lock", hue: "plum", text: "Into your private workspace only", bottom: "Nothing is shared unless you promote it yourself." },
      { icon: "sync", hue: "teal", text: "Turn it off any time", bottom: "What is already saved stays; the pull stops." },
    ],
    turnOn: "Turn on",
    cancel: "Cancel",
    on: "Whole-mailbox ingest is on. Your mailbox is being pulled in.",
    off: "Whole-mailbox ingest is off. Saved mail stays.",
    unavailable: "Whole-mailbox ingest is not available for this workspace.",
    failed: (status: number) => `Could not change whole-mailbox ingest (${status}). Try again in a moment.`,
  },

  meeting: {
    fallbackTitle: "This meeting",
    verifying: "Checking this meeting…",
    unavailable: "Meeting status is unavailable.",
    checkAgain: "Check again",
    save: { teams: "Save this meeting", outlook: "Save meeting to CoralSwarm" },
    stop: "Stop saving",
    updating: "Updating…",
    needsOrg: "Choose an organization workspace to save meetings",
    changeWorkspace: "Change workspace",
    personalNote: "Only you can see this transcript, in your personal workspace. Microsoft shares transcripts for work or school accounts only.",
    connectMicrosoft: "Connect Microsoft",
    checkConnection: "Check connection",
    statusPrefix: "In your workspace",
    status: {
      off: { line: "Not saved yet", hint: "Save this meeting to bring its transcript into your workspace." },
      waiting: { line: "In your workspace · waiting for transcript", hint: "CoralSwarm checks for the transcript automatically." },
      saved: { line: "In your workspace · transcript saved", hint: "Processing into your workspace can take a moment." },
      stopped: { line: "Stopped saving", hint: "Transcripts already saved stay in your workspace." },
      ended: { line: "Saving window ended", hint: "This meeting’s transcript window has closed. Transcripts already saved stay in your workspace." },
      connectNeeded: { line: "Microsoft connection needed", hint: "Saving is on, but CoralSwarm can’t fetch the transcript until you reconnect Microsoft." },
      connectFirst: { line: "Microsoft connection needed", hint: "Connect your Microsoft account, then come back. Connecting doesn’t save anything." },
    },
    retrieved: (count: number) => `${count} ${count === 1 ? "transcript" : "transcripts"} retrieved from Microsoft at the last check.`,
    noneYet: "No transcript was available at the last check.",
    lastChecked: (when: string) => `Last checked: ${when}`,
    lastErrorIdle: "The last transcript check did not complete.",
    lastErrorReconnect: "Reconnect Microsoft to resume transcript checks.",
    lastErrorRetry: "The last transcript check did not complete. CoralSwarm will retry while saving is on.",
    savedToast: "Saving this meeting — its transcript follows on its own.",
    stoppedToast: "Stopped saving. Transcripts already saved stay in your workspace.",
    microsoftCouldNotOpen: "Microsoft sign-in could not open. Try again.",
    explain: {
      availability_window_ended: "The transcript window has ended for this meeting.",
      occurrence_unverified: "Microsoft couldn’t confirm this meeting yet. Try again in a minute.",
      microsoft_identity_mismatch: "Connect the Microsoft account that added CoralSwarm to this meeting.",
      meeting_context_unavailable: "Add CoralSwarm to this meeting, then reopen this pane.",
      consumer_identity_unsupported: "Microsoft only shares meeting transcripts for work or school accounts. A personal Microsoft account can’t be used.",
      generic: "Couldn’t check this meeting. Try again — nothing has changed.",
    },
    teamsNoContext: "Open CoralSwarm from a Teams meeting to save that meeting.",
    teamsReading: "Reading this meeting…",
    outlookReading: "Reading this appointment…",
    outlookNoAppointment: "Open this from a Teams appointment.",
  },

  /** The recording prompt: Teams must record and transcribe, or there is nothing to save. */
  transcribe: {
    title: "Turn on transcription",
    steps: "In the meeting, open More (…) → Record and transcribe → Start transcription.",
    note: "CoralSwarm saves the transcript after the meeting.",
    autoHint: "Teams will record and transcribe it when it starts.",
    autoHintSeries: "Teams will record and transcribe each meeting when it starts.",
    /** Compose / one-off: we cannot confirm anything until the meeting is saved. */
    composeFirst: "Turn on Record and transcribe automatically in Teams Meeting options.",
    composeFallback: "Or start it in the meeting: More (…) → Record and transcribe → Start transcription.",
    /** Recurring: a series-level instruction. */
    seriesFirst: "Open Meeting options for this series and turn on Record and transcribe automatically.",
    seriesFallback: "Otherwise, in each meeting open More (…) → Record and transcribe → Start transcription.",
    reason: {
      not_organizer: "Only the organizer can turn this on in Teams.",
      missing_scope: "CoralSwarm couldn’t turn this on for you. Turn it on in Meeting options.",
      policy: "Your organization’s Teams settings don’t allow CoralSwarm to turn this on.",
      already_started: "This meeting has already started, so transcription has to be started in the meeting.",
    } as Record<string, string>,
  },

  /** Outlook, unsaved meeting (compose). */
  compose: {
    save: "Save this meeting to CoralSwarm",
    saving: "Turning on…",
    saveHint: "Turn this on now. CoralSwarm saves the transcript after you save or send this meeting.",
    on: "On — CoralSwarm saves the transcript after you save or send this meeting.",
    onSeries: "On — CoralSwarm saves the transcript of every meeting in this series after you save or send it.",
    seriesTitle: "Saves every meeting in this series",
    activeLine: "On — CoralSwarm is saving this meeting’s transcript.",
    activeLineSeries: "On — CoralSwarm is saving every meeting in this series.",
    nextMeeting: (when: string) => `Next meeting: ${when}`,
    turnOff: "Turn off",
    turningOff: "Turning off…",
    stopped: "Stopped saving. Transcripts already saved stay in your workspace.",
    /** Neutral: on reopen we don't know whether the event was saved without a Teams link or never saved. */
    expired: "This expired before CoralSwarm could find the meeting’s Teams link. Turn it on again.",
    /** Expired after the saved event was found but never had a Teams link. */
    expiredNoLink: "This expired before the meeting got a Teams link. Turn it on again.",
    turnOnAgain: "Turn on again",
    /** Saved, but the event has no Teams join link yet. `until` is the expiry in local time, when known. */
    waitingForLink: {
      title: "Waiting for a Teams link",
      body: (until: string | null) =>
        `Saved, but this meeting doesn’t have a Teams link yet. Add an attendee or turn on Teams meeting, then save again.${until ? ` CoralSwarm keeps waiting until ${until}.` : ""}`,
    },
    intentError: "CoralSwarm couldn’t finish setting this meeting up. Turn it off and on again.",
    errors: {
      teams_link_missing: "This event has no Teams meeting yet. Turn on Teams meeting, then save it again.",
      not_organizer: "Only the organizer can save this meeting’s transcript.",
      already_ended: "This meeting has already ended.",
      ambiguous_nonce: "More than one event carries this choice, so CoralSwarm can’t tell which to follow. Turn it off and on again.",
      unsupported_event: "CoralSwarm can’t follow this kind of event. Turn it off.",
      unsupported_join_url: "This event’s Teams link isn’t one CoralSwarm can follow. Turn it off.",
      generic: "CoralSwarm couldn’t follow this meeting. Turn it off and on again.",
    } as Record<string, string>,
    stoppedBecause: {
      event_cancelled: "This meeting was cancelled, so saving has stopped.",
      event_deleted: "This meeting was deleted, so saving has stopped.",
      series_ended: "This series has ended, so saving has stopped.",
      completed: "This meeting has finished. Transcripts already saved stay in your workspace.",
      not_organizer: "The organizer changed, so saving has stopped.",
      capture_stopped: "Saving was stopped from the Teams meeting pane. Transcripts already saved stay in your workspace.",
      superseded: "This meeting was turned on again, so this earlier choice stopped.",
    } as Record<string, string>,
    createFailed: {
      organization_required: "Choose an organization workspace to save meetings",
      too_many_pending: "You have too many unsaved meetings waiting. Save or send one, or turn one off, then try again.",
      too_many_active: "You are already saving the maximum number of meetings. Stop one, then try again.",
      rate_limited: "That was too many in a short time. Try again in a little while.",
    } as Record<string, string>,
    connectFirst: "Connect your Microsoft account, then come back. Connecting doesn’t save anything.",
    statusFailed: "Couldn’t check this meeting’s status. Your choice is kept.",
    failed: "Couldn’t turn this on. Try again.",
    unconfirmed: "Outlook is slow to confirm this. If it doesn’t show On when you reopen CoralSwarm, turn it off and on again.",
    offFailed: "Couldn’t turn this off. Try again before you save this meeting.",
    unsupported: "This version of Outlook can’t turn CoralSwarm on for an unsaved meeting.",
    unsupportedFallback: "Save the meeting, then reopen CoralSwarm.",
    resumed: "Reading this meeting…",
  },
} as const;

export function explainMeetingError(code: string | null | undefined): string {
  const table: Record<string, string> = copy.meeting.explain;
  // Own properties only: the code is server input, and "constructor"/"toString" must not resolve.
  return (code && Object.prototype.hasOwnProperty.call(table, code) && table[code]) || copy.meeting.explain.generic;
}
