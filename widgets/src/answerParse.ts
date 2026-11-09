// Minimal parser for ask_ocean's `answer` prose: markdown `**bold**` lead-ins
// and `[n]` citation markers (backend/prompts/synthesis_stream_v1.txt's
// "Formatting" section; `[n]` markers per `synthesis.rs`'s citation-marker
// scanner, `citations()`/`classify_marker`). Deliberately hand-rolled rather
// than pulling in a markdown library — the answer text only ever uses these
// two constructs plus paragraph breaks (blank lines), never headers, lists,
// links, or tables in a citation-bearing answer chip position.

export type InlineToken =
  | { type: "text"; value: string }
  | { type: "bold"; value: string }
  | { type: "chip"; index: number };

const INLINE_RE = /\*\*(.+?)\*\*|\[(\d{1,3})\]/g;

export function parseInline(text: string): InlineToken[] {
  const tokens: InlineToken[] = [];
  let lastIndex = 0;
  INLINE_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = INLINE_RE.exec(text)) !== null) {
    if (m.index > lastIndex) {
      tokens.push({ type: "text", value: text.slice(lastIndex, m.index) });
    }
    if (m[1] !== undefined) {
      tokens.push({ type: "bold", value: m[1] });
    } else if (m[2] !== undefined) {
      tokens.push({ type: "chip", index: Number(m[2]) });
    }
    lastIndex = INLINE_RE.lastIndex;
  }
  if (lastIndex < text.length) {
    tokens.push({ type: "text", value: text.slice(lastIndex) });
  }
  return tokens;
}

/** Split into paragraphs on blank lines, dropping empty ones. Matches CRLF
 *  (`\r\n\r\n`) and a "blank" line that still carries trailing spaces/tabs —
 *  either renders as a false single paragraph otherwise (HTML collapses the
 *  retained whitespace, so a plain `\n{2,}` split misses both). */
export function splitParagraphs(text: string): string[] {
  return text
    .split(/\r?\n(?:[ \t]*\r?\n)+/)
    .map((p) => p.trim())
    .filter((p) => p.length > 0);
}
