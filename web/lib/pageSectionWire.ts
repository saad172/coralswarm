import { conversationDetailPayload } from "./pageSectionTypes/conversationDetail";
import { captureDetailPayload } from "./pageSectionTypes/captureDetail";
import { isMembersAdminPayload } from "./pageSectionTypes/membersAdmin";
import { isConversationListPayload } from "./pageSectionTypes/conversationListWire";
import { parseConnectorStatus } from "./pageSectionTypes/connectorStatus";
import manifest from "./pageSections.generated.json" with { type: "json" };

export type PageModel = keyof typeof manifest;
export type SectionParams = Record<string, string | number | boolean | null | undefined>;
export interface Stored<P> {
  model: PageModel;
  version: number;
  payload: P;
  computed_at: string | null;
  stale: boolean;
  refresh_eta: string | null;
}

export class StoredCertificateError extends Error {}

export function storedTimestamp(value: unknown): boolean {
  if (value === null) return true;
  if (typeof value !== "string") return false;
  const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(Z|([+-])(\d{2}):(\d{2}))$/.exec(value);
  if (!parts || !Number.isFinite(Date.parse(value))) return false;
  const year = Number(parts[1]), month = Number(parts[2]), day = Number(parts[3]);
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days = [31, leap ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  return month >= 1 && month <= 12 && day >= 1 && day <= days[month - 1] && Number(parts[4]) <= 23 &&
    Number(parts[5]) <= 59 && Number(parts[6]) <= 59 && (parts[7] === "Z" || Number(parts[9]) <= 23 && Number(parts[10]) <= 59);
}

/** Validate the real wire envelope. Never upgrade an old bare payload into a
 * certificate: computed_at is the server's authority, including computed-empty. */
export function typedStored<P>(body: unknown, model: PageModel, params: SectionParams = {}): Stored<P> {
  const b = body as Partial<Stored<P>> | null;
  if (!b || b.model !== model || b.version !== manifest[model].version ||
      !("payload" in b) || b.payload === undefined || typeof b.stale !== "boolean" ||
      !storedTimestamp(b.computed_at) || !storedTimestamp(b.refresh_eta) ||
      (b.computed_at === null && (!b.stale || b.refresh_eta === null)) ||
      (!b.stale && b.refresh_eta !== null) ||
      b.payload === null || typeof b.payload !== "object") {
    throw new StoredCertificateError(`Could not verify the ${model} answer.`);
  }
  if (model === "connector_status" && b.computed_at !== null) {
    try { parseConnectorStatus(b.payload); } catch { throw new StoredCertificateError("Could not verify connector status."); }
  }
  if (model === "conversation_list" && b.computed_at !== null && !isConversationListPayload(b.payload)) throw new StoredCertificateError("Could not verify recent chats.");
  if (model === "conversation_detail" && b.computed_at !== null) {
    try { conversationDetailPayload(b.payload, params.id == null ? undefined : String(params.id)); }
    catch { throw new StoredCertificateError("Could not verify this saved chat."); }
  }
  if (model === "capture_detail" && b.computed_at !== null) {
    try { captureDetailPayload(b.payload, params); }
    catch { throw new StoredCertificateError("Could not verify this retained capture."); }
  }
  if (model === "members_admin" && b.computed_at !== null && !isMembersAdminPayload(b.payload)) throw new StoredCertificateError("Could not verify organization members.");
  return b as Stored<P>;
}

export function canonicalSectionParams(params: SectionParams = {}): Record<string, string> {
  return Object.fromEntries(Object.entries(params)
    .filter((entry) => entry[1] !== undefined && entry[1] !== null)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => {
      const value = String(v);
      return [k, ["from", "to"].includes(k) && Number.isFinite(Date.parse(value)) ? new Date(value).toISOString() : value];
    }));
}

export function pageSectionPath(model: PageModel, params: SectionParams = {}, refresh = false): string {
  const values = canonicalSectionParams(params);
  const path = manifest[model].path.replace(/\{([^}]+)\}/g, (_, key: string) => {
    const value = values[key];
    if (!value) throw new Error(`Missing ${key} for ${model}`);
    delete values[key];
    return encodeURIComponent(value);
  });
  const query = new URLSearchParams(values).toString();
  return `/v1${path}${refresh ? "/refresh" : ""}${query ? `?${query}` : ""}`;
}

/** Disabled queries can be declared before a required selector exists. They
 * still have a scoped descriptor; an enabled read must resolve the real path. */
export function pageSectionReadIdentity(model: PageModel, scope: string, params: SectionParams): string {
  const canonical = canonicalSectionParams(params);
  const complete = [...manifest[model].path.matchAll(/\{([^}]+)\}/g)].every((match) => !!canonical[match[1]]);
  return JSON.stringify(complete ? [scope, pageSectionPath(model, canonical)] : [scope, "incomplete", model, canonical]);
}

export function refreshOwed(stored: Stored<unknown> | undefined): boolean {
  return !!stored && (stored.computed_at === null || stored.refresh_eta !== null);
}


/** Calendar identities have date precision, never per-request timestamps. */
export function sectionCalendarDay(tz = "UTC", now = new Date()): string {
  const parts = new Intl.DateTimeFormat("en", { timeZone: tz, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(now);
  const part = (name: Intl.DateTimeFormatPartTypes) => parts.find((item) => item.type === name)?.value;
  return `${part("year")}-${part("month")}-${part("day")}`;
}
