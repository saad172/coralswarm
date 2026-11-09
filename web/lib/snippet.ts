// Parser for the `search_meetings` snippet format (CONTRACT A): matched terms
// are wrapped in a pair of ASCII control characters — STX (code point 2) opens
// a match, ETX (code point 3) closes it. We split on those characters into
// plain-text + matched segments so the caller can build React nodes directly —
// never `dangerouslySetInnerHTML` — so transcript content can't inject markup.

const STX = String.fromCharCode(2);
const ETX = String.fromCharCode(3);

export interface SnippetPart {
  text: string;
  matched: boolean;
}

/** Split a snippet string into ordered plain/matched text runs. Unmatched
 *  control characters (a stray open with no close, etc.) degrade gracefully —
 *  everything after an unclosed STX is simply treated as matched text. */
export function parseSnippet(snippet: string): SnippetPart[] {
  const parts: SnippetPart[] = [];
  let matched = false;
  let buf = "";
  for (const ch of snippet) {
    if (ch === STX) {
      if (buf) parts.push({ text: buf, matched });
      buf = "";
      matched = true;
      continue;
    }
    if (ch === ETX) {
      if (buf) parts.push({ text: buf, matched });
      buf = "";
      matched = false;
      continue;
    }
    buf += ch;
  }
  if (buf) parts.push({ text: buf, matched });
  return parts;
}
