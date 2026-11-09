// gsuite Train G4 PR 2 (design doc §3.6) — pins `SOURCE_LABEL`'s
// `google_chat` membership, the web half of the five-map registration the
// design doc requires (the other four are backend maps:
// `chunker.rs::for_source_type`, `store/mod.rs::journal_block_title_fallback`,
// `routes/queries.rs::source_type_label`, `routes/queries.rs::
// intake_source_phrase`). `source_type_label`'s own doc names this map as
// the one it mirrors — this test is the tripwire for that pairing drifting
// apart.

import { describe, expect, it } from "vitest";
import {
  SOURCE_LABEL,
  srcName,
  coralTitle,
  citationCardLine,
  sessionCoralLine,
  sourceCoralLine,
  visibleSlice,
} from "./coral";

describe("SOURCE_LABEL google_chat registration", () => {
  it("carries a google_chat entry", () => {
    expect(SOURCE_LABEL.google_chat).toBe("Google Chat");
  });

  it("srcName resolves google_chat through the map, not the raw fallback", () => {
    expect(srcName("google_chat")).toBe("Google Chat");
  });

  it("srcName still falls back to the raw type for a genuinely unknown source", () => {
    expect(srcName("some_unregistered_source")).toBe("some_unregistered_source");
  });
});

// Previews-are-title-first (title-always contract): every one of these
// helpers must resolve to a non-empty primary line, and prefer the API's own
// `title` over any body-derived heuristic when one is supplied.
describe("coralTitle prefers an explicit title", () => {
  it("uses the explicit title over the body when both are present", () => {
    expect(
      coralTitle({ title: "Ship the billing webhook", preview: "sender · 2026-08-01\nsome body text" }),
    ).toBe("Ship the billing webhook");
  });

  it("ignores a blank/whitespace-only explicit title and falls through to the body heuristics", () => {
    expect(coralTitle({ title: "   ", preview: "sender · 2026-08-01\nfrom the body" })).toBe("from the body");
  });

  it("still derives from the body when no title is supplied (back-compat)", () => {
    expect(coralTitle({ preview: "sender · 2026-08-01\nfrom the body" })).toBe("from the body");
  });

  it("falls all the way to the neutral placeholder when nothing is available", () => {
    expect(coralTitle({ source_type: "slack" })).toBe("Untitled Slack item");
  });
});

describe("citationCardLine (Cite.tsx SourceCard's title/quote)", () => {
  it("renders the API title as the primary line, and the preview as the quote", () => {
    expect(citationCardLine({ title: "Q3 pricing decision", preview: "we're going with tiered pricing" })).toEqual({
      title: "Q3 pricing decision",
      quote: "we're going with tiered pricing",
    });
  });

  it("never renders an empty primary line — falls back to a neutral source label when title is missing", () => {
    expect(citationCardLine({ title: null, source_type: "slack" }).title).toBe("Slack");
    expect(citationCardLine({ source_type: "slack" }).title).toBe("Slack");
  });

  it("prefers content over preview for the quote, then falls back to empty (never a fake placeholder)", () => {
    expect(citationCardLine({ title: "t", content: "the synthesized snippet" }).quote).toBe(
      "the synthesized snippet",
    );
    expect(citationCardLine({ title: "t" }).quote).toBe("");
  });

  it("content wins over preview when both are present — content is the ask-time synthesized snippet, preview the generic retrieval-only alias", () => {
    expect(
      citationCardLine({
        title: "t",
        content: "the synthesized snippet",
        preview: "the retrieval-only preview",
      }).quote,
    ).toBe("the synthesized snippet");
  });

  it("a whitespace-only preview never suppresses a non-empty content", () => {
    expect(
      citationCardLine({ title: "t", content: "the synthesized snippet", preview: "   " }).quote,
    ).toBe("the synthesized snippet");
  });

  it("falls through to preview when content is present but whitespace-only", () => {
    expect(
      citationCardLine({ title: "t", content: "   ", preview: "the retrieval-only preview" }).quote,
    ).toBe("the retrieval-only preview");
  });
});

describe("sessionCoralLine (session/page.tsx CoralRow)", () => {
  it("uses the checkpoint's own title and carries the excerpt through", () => {
    expect(sessionCoralLine({ title: "Fixed the retry loop", excerpt: "root cause was a dropped socket" })).toEqual({
      title: "Fixed the retry loop",
      untitled: false,
      excerpt: "root cause was a dropped socket",
    });
  });

  it("never renders an empty primary line — falls back to 'Untitled checkpoint', never the id", () => {
    expect(sessionCoralLine({ title: "" }).title).toBe("Untitled checkpoint");
    expect(sessionCoralLine({ title: "" }).untitled).toBe(true);
    expect(sessionCoralLine({ title: "   " }).title).toBe("Untitled checkpoint");
    expect(sessionCoralLine({}).title).toBe("Untitled checkpoint");
  });

  it("normalizes a blank excerpt to null rather than an empty secondary line", () => {
    expect(sessionCoralLine({ title: "t", excerpt: "  " }).excerpt).toBeNull();
    expect(sessionCoralLine({ title: "t" }).excerpt).toBeNull();
  });
});

describe("sourceCoralLine (library/SourceDetail.tsx item rows)", () => {
  it("uses the item's own title and excerpt as the primary/secondary lines", () => {
    expect(
      sourceCoralLine({ title: "Decided to ship Friday", excerpt: "team agreed after the standup" }),
    ).toEqual({
      title: "Decided to ship Friday",
      body: "team agreed after the standup",
    });
  });

  it("never renders an empty primary line — falls back through the preview body, then a neutral placeholder", () => {
    expect(sourceCoralLine({ preview: "sender · 2026-08-01\nfrom the preview body" }).title).toBe(
      "from the preview body",
    );
    expect(sourceCoralLine({}).title).toBe("Untitled item");
  });

  it("secondary line is empty (not a '(no preview)' placeholder) when nothing is available", () => {
    expect(sourceCoralLine({ title: "t" }).body).toBe("");
  });
});

describe("visibleSlice (SourceDetail's 'Show all N' expander)", () => {
  const items = Array.from({ length: 12 }, (_, i) => i);

  it("caps at the initial count before expanding", () => {
    expect(visibleSlice(items, false)).toEqual([0, 1, 2, 3, 4]);
  });

  it("shows every item once expanded", () => {
    expect(visibleSlice(items, true)).toEqual(items);
    expect(visibleSlice(items, true)).toHaveLength(12);
  });

  it("honors a custom initial count", () => {
    expect(visibleSlice(items, false, 3)).toEqual([0, 1, 2]);
  });
});
