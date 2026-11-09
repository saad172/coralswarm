/** A candidate selector, never proof of identity or consent. API re-verifies it. */
export function meetingCapturePath(candidate: string): string {
  const url = new URL(candidate);
  if (url.protocol !== "https:" || !["teams.microsoft.com", "teams.cloud.microsoft"].includes(url.hostname) ||
      url.username || url.password || url.port || url.hash ||
      !(url.pathname.startsWith("/l/meetup-join/") || /^\/meet\/\d+$/.test(url.pathname)))
    throw new Error("This meeting did not provide a supported Teams join link.");
  return `/connectors/microsoft-teams/meeting-capture?join_url=${encodeURIComponent(url.href)}`;
}

/** Teams IDs remain opaque untrusted selectors until the server verifies context. */
export function nativeMeetingCapturePath(meetingId: string, chatId: string): string {
  if (!meetingId || !chatId || meetingId.length > 2048 || chatId.length > 512)
    throw new Error("Teams did not provide this meeting's context. Open CoralSwarm from the meeting.");
  return `/connectors/microsoft-teams/meeting-capture?teams_meeting_id=${encodeURIComponent(meetingId)}&teams_chat_id=${encodeURIComponent(chatId)}`;
}

export function validateMeetingCaptureQuery(query: Pick<URLSearchParams, "get" | "getAll">): void {
  const keys = ["join_url", "meeting_ticket", "teams_meeting_id", "teams_chat_id"];
  for (const key of keys) if (query.getAll(key).length > 1) throw new Error("This link contains conflicting meeting selections. Reopen the meeting add-in.");
  const join = query.get("join_url");
  const ticket = query.get("meeting_ticket");
  const meeting = query.get("teams_meeting_id");
  const chat = query.get("teams_chat_id");
  const families = Number(join !== null) + Number(ticket !== null) + Number(meeting !== null || chat !== null);
  if (families > 1 || (families === 1 && !join && !ticket && (!meeting || !chat)))
    throw new Error("This link does not identify one meeting. Reopen the meeting add-in.");
  if ((join !== null && !join) || (ticket !== null && !ticket)) throw new Error("This meeting selection is empty. Reopen the meeting add-in.");
}
