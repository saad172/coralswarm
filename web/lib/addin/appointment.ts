/* Appointment parsing for the Outlook meeting pane. Ported unchanged in behaviour
 * from extensions/outlook-addin/src/meeting/meeting.js (which wrapped this pane in
 * an iframe; the pane now runs directly in the taskpane so Office SSO/NAA works).
 *
 * The appointment body stays local. Only one validated Teams join URL leaves this
 * module: no event body, attendees, subject, or email content. */

import type { OfficeApi, OfficeMailItem } from "./office";

const MULTIPLE = "This appointment contains multiple Teams meetings. Open the intended meeting's CoralSwarm add-in instead.";
const NONE = "This appointment does not contain a supported Teams meeting link. Save the Teams meeting first, then reopen this action.";

export function supportedJoinUrl(candidate: string | null): string | null {
  try {
    const url = new URL(candidate ?? "");
    if (url.protocol !== "https:" || !["teams.microsoft.com", "teams.cloud.microsoft"].includes(url.hostname) || url.port || url.username || url.password || url.hash) return null;
    if (!(url.pathname.startsWith("/l/meetup-join/") || /^\/meet\/\d+$/.test(url.pathname))) return null;
    return url.href;
  } catch { return null; }
}

export function appointmentJoinUrl(html: string, document: Document): string {
  const fragment = document.createElement("template");
  fragment.innerHTML = html; // Inert template: no scripts, images, or appointment HTML inserted into the pane.
  const markedLinks = (name: string) => Array.from(fragment.content.querySelectorAll("a[href]"))
    .filter((element) => [`meet_invite_block.action.${name}`, `x_meet_invite_block.action.${name}`].includes(element.id));
  const primary = markedLinks("join_link");
  const compatibility = markedLinks("join_link_compatibility");
  if (primary.length > 1 || compatibility.length > 1) throw new Error(MULTIPLE);
  let reference: Element | null = null;
  if (primary.length === 1 && compatibility.length === 1) {
    const joinUrl = supportedJoinUrl(primary[0].getAttribute("href"));
    const referenceUrl = supportedJoinUrl(compatibility[0].getAttribute("href"));
    if (!joinUrl || !/^\/meet\/\d+$/.test(new URL(joinUrl).pathname)
      || !referenceUrl || !new URL(referenceUrl).pathname.startsWith("/l/meetup-join/")) throw new Error(MULTIPLE);
    // Microsoft's marked primary Join control is the selector. Its marked
    // System reference is not a second intent; Graph still verifies the target.
    reference = compatibility[0];
  }
  const candidates: Array<string | null> = [];
  for (const element of Array.from(fragment.content.querySelectorAll("a[href]"))) {
    const href = element.getAttribute("href");
    if (element !== reference) candidates.push(href);
    if (supportedJoinUrl(href)) element.replaceWith(document.createTextNode(" "));
  }
  for (const element of Array.from(fragment.content.querySelectorAll("p,div,li,td,th,tr,h1,h2,h3,h4,h5,h6,blockquote,pre,br,section,article,header,footer,dt,dd"))) {
    element.insertAdjacentText("beforebegin", " ");
    if (element.nodeName !== "BR") element.insertAdjacentText("afterend", " ");
  }
  candidates.push(...(fragment.content.textContent?.match(/https:\/\/[^\s<>"']+/g) ?? []));
  const urls = new Set(candidates.map(supportedJoinUrl).filter((value): value is string => Boolean(value)));
  if (urls.size !== 1) throw new Error(urls.size ? MULTIPLE : NONE);
  return Array.from(urls)[0];
}

/** Read the open appointment's body and reduce it to one validated join URL. */
export function readAppointmentJoinUrl(office: OfficeApi, item: OfficeMailItem, document: Document): Promise<string> {
  return new Promise((resolve, reject) => {
    if (!item.body) { reject(new Error("Outlook could not read this appointment’s meeting link.")); return; }
    item.body.getAsync(office.CoercionType.Html, (result) => {
      if (result.status !== office.AsyncResultStatus.Succeeded) { reject(new Error("Outlook could not read this appointment’s meeting link.")); return; }
      try { resolve(appointmentJoinUrl(result.value, document)); } catch (error) { reject(error); }
    });
  });
}
