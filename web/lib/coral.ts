// Shared coral presentation helpers.
//
// Corals (atoms) have no name of their own, so the UI derives a human-readable
// title from whatever provenance is available. The cardinal rule: NEVER show a
// raw id (atom_id / source_id) to a user — fall back to a neutral placeholder
// instead. Single source of truth for the corals list, the coral detail page,
// and the Sources view.

/** Friendly names for source types (slack → Slack, transcript → Meeting, …). */
export const SOURCE_LABEL: Record<string, string> = {
  slack: "Slack",
  email: "Email",
  transcript: "Meeting",
  note: "Note",
  notion: "Notion",
  // Corals captured by a coding agent under a `coding_sessions` container
  // (migration 0098). Derived server-side from the presence of a session_id,
  // so this label and the session row can never disagree.
  coding_session: "Coding session",
  // The other three session kinds (`routes::ingest::SessionKind`), e.g. an
  // autonomous agent's checkpoints.
  agent_session: "Agent session",
  work_session: "Work session",
  meeting_session: "Meeting session",
  // gsuite Train G4 PR 2 (design doc §3.6) — Google Chat spaces AND
  // DMs/group chats share this one label; mirrors
  // `routes::queries::source_type_label` in the backend (that fn's doc
  // names this map as the one it mirrors).
  google_chat: "Google Chat",
};

export const srcName = (t?: string | null): string =>
  (t && (SOURCE_LABEL[t] ?? t)) || "Source";

/** True when a value is a raw Slack user id (e.g. "U57C3A9EF09", "W…").
 * Ingest persists these as author/participant names when profile resolution
 * fails; per the cardinal rule above they must never render. Conservative on
 * purpose (uppercase Slack shape only) so real names are never masked. */
export const isRawSlackUserId = (v?: string | null): boolean =>
  !!v && /^[UW][A-Z0-9]{8,}$/.test(v);

/** True when a value is a raw Slack channel id (e.g. "C0B9SKC77S8", "G…",
 * "D…"). Same doctrine and same conservative shape as isRawSlackUserId. */
export const isRawSlackChannelId = (v?: string | null): boolean =>
  !!v && /^[CGD][A-Z0-9]{8,}$/.test(v);

/** Short date ("Jun 25"); empty string for a null timestamp. */
export const fmtDate = (ts: string | null | undefined): string =>
  ts ? new Date(ts).toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "";

/** Parse the provenance header ingest prepends to a coral: "{sender} in
 * #{channel} · {when}" OR the channel-less "{sender} · {when}". Detected by the
 * " · YYYY-MM-DD" timestamp so DM/email/transcript headers strip too; returns the
 * optional #channel, the optional author, and the message body with the header
 * removed. */
export function parseProvenance(preview?: string | null): {
  channel: string | null;
  author: string | null;
  body: string;
} {
  const raw = (preview ?? "").trim();
  const nl = raw.indexOf("\n");
  const firstLine = nl >= 0 ? raw.slice(0, nl) : raw;
  if (/ · \d{4}-\d{2}-\d{2}/.test(firstLine)) {
    const chan = firstLine.match(/ in (#[^\s·]+)/);
    // Author is the text before " in #channel" or before " · {when}".
    const author = firstLine.split(/ in #| · /)[0]?.trim() || null;
    return { channel: chan ? chan[1] : null, author, body: nl >= 0 ? raw.slice(nl + 1).trim() : "" };
  }
  return { channel: null, author: null, body: raw };
}

/** Message body of a coral preview, header stripped, whitespace collapsed. */
export function bodyOf(preview?: string | null): string {
  return parseProvenance(preview).body.replace(/\s+/g, " ").trim();
}

/** First non-empty line of a raw text body, truncated. Used for the coral detail
 * title where the full decrypted `content` is available. */
function firstLine(text?: string | null, max = 100): string {
  const line = (text ?? "")
    .split("\n")
    .map((l) => l.trim())
    .find((l) => l.length > 0);
  if (!line) return "";
  return line.length > max ? `${line.slice(0, max).trimEnd()}…` : line;
}

/** Derive a human title for a coral. Prefers an explicit `title` the caller
 * already has (the title-always API contract: `atoms`/`sources`/`sessions`/
 * `candidates`/citation endpoints all now guarantee a non-empty `title`) over
 * every body-derived heuristic below it. Falls through, in order, when that's
 * absent/blank: the message body (from an inline preview or the full
 * decrypted content) → "{author} in #{channel}" → "#{channel}" → author → a
 * neutral typed placeholder. NEVER returns an id. */
export function coralTitle(c: {
  title?: string | null;
  preview?: string | null;
  content?: string | null;
  channel_name?: string | null;
  author_name?: string | null;
  source_type?: string | null;
}): string {
  const explicit = c.title?.trim();
  if (explicit) return explicit;

  const body = bodyOf(c.preview) || firstLine(c.content);
  if (body) return body;

  const parsed = parseProvenance(c.preview);
  // Slack ids sometimes leak into persisted names (and provenance headers);
  // treat id-shaped values as absent so the title never surfaces a raw id.
  const channelName =
    [c.channel_name, parsed.channel?.replace(/^#/, "")].find(
      (n) => n && !isRawSlackChannelId(n),
    ) ?? null;
  const channel = channelName ? `#${channelName}` : null;
  const author =
    [c.author_name, parsed.author].find((a) => a && !isRawSlackUserId(a)) ?? null;
  if (author && channel) return `${author} in ${channel}`;
  if (channel) return channel;
  if (author) return author;

  return `Untitled ${srcName(c.source_type)} item`;
}

/** Primary/secondary line for a citation card (`Cite.tsx`'s `SourceCard`,
 * `ContextDrawer`, `AnswerTurn`). Title-first: prefers the API's `title`,
 * falling back to a neutral source-type label so the primary line is never
 * empty. The secondary line (`quote`) prefers `content` — the ask-time
 * synthesized snippet (`CitationDetail`'s own doc comment: "Synthesized-mode
 * snippet (backend field `content`); `preview` is the retrieval-only
 * alias") — over `preview`, the generic retrieval excerpt that's only
 * present when no synthesis ran. Both are trimmed before the fallback chain
 * so a whitespace-only `content` doesn't shadow a real `preview`; empty
 * string when neither is available — callers should omit the line entirely
 * rather than render a placeholder for it. */
export function citationCardLine(c: {
  title?: string | null;
  preview?: string | null;
  content?: string | null;
  source_type?: string | null;
}): { title: string; quote: string } {
  const title = c.title?.trim() || srcName(c.source_type);
  const quote = c.content?.trim() || c.preview?.trim() || "";
  return { title, quote };
}

/** Primary/secondary line for a coding-session checkpoint row (`session/
 * page.tsx`'s `CoralRow`). The API now guarantees `corals[].title` is
 * non-empty (title-always contract) — this only falls back to a neutral
 * placeholder for the degenerate case of a blank string reaching the client
 * anyway, never the raw checkpoint/source id. */
export function sessionCoralLine(c: {
  title?: string | null;
  excerpt?: string | null;
}): { title: string; untitled: boolean; excerpt: string | null } {
  const title = c.title?.trim();
  const excerpt = c.excerpt?.trim() || null;
  if (title) return { title, untitled: false, excerpt };
  return { title: "Untitled checkpoint", untitled: true, excerpt };
}

/** Primary/secondary line for a coral row under a source group (`library/
 * SourceDetail.tsx`). Title-first: the API's `title`, falling back to the
 * preview's message body, then a neutral placeholder — the primary line is
 * never empty. The secondary line prefers the excerpt, falling back to the
 * preview's body; empty string when neither exists (render nothing, not a
 * "(no preview)" placeholder). */
export function sourceCoralLine(c: {
  title?: string | null;
  excerpt?: string | null;
  preview?: string | null;
}): { title: string; body: string } {
  const title = c.title?.trim() || bodyOf(c.preview) || "Untitled item";
  const body = c.excerpt?.trim() || bodyOf(c.preview) || "";
  return { title, body };
}

/** The "show all N" expander slice shared by capped preview lists (currently
 * `SourceDetail`'s latest-corals list): the first `initial` items, or every
 * item once the caller has expanded. Generic and DOM-free so the toggle
 * behavior is unit-testable without a component-render harness. */
export function visibleSlice<T>(items: T[], expanded: boolean, initial = 5): T[] {
  return expanded ? items : items.slice(0, initial);
}
