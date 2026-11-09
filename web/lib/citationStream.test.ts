import { describe, expect, it } from "vitest";
import {
  applyCitation,
  applyFinal,
  emptyAssistant,
  latestAssistantTurn,
  pendingLabel,
} from "@/components/chat/types";
import type { AssistantTurn, Turn } from "@/components/chat/types";
import type { ChatFinal, StreamedCitation } from "@/lib/api";

/* Covers the mid-stream citation reducer that lives in
   components/chat/types.ts — kept here because vitest.config.ts deliberately
   scopes `include` to lib/**. Pure functions only, no component rendering. */

/** A citation frame as the backend emits it mid-answer. */
function cite(n: number, atom_id: string, content = "snippet"): StreamedCitation {
  return {
    n,
    atom_id,
    graph_id: "g1",
    source_type: "slack",
    title: `Citation ${atom_id}`,
    content,
    timestamp: "2026-08-04T00:00:00Z",
  };
}

function turn(): AssistantTurn {
  return emptyAssistant("k", "why stripe?");
}

describe("applyCitation", () => {
  it("shows a source as soon as its marker streams, before the answer finishes", () => {
    const t = applyCitation(turn(), cite(1, "a1"));
    expect(t.citationDetails).toHaveLength(1);
    expect(t.citationDetails[0].atom_id).toBe("a1");
    expect(t.citations).toEqual(["a1"]);
    // Still streaming — the turn must not be marked complete by a citation.
    expect(t.pending).toBe(true);
  });

  it("accumulates in display order as more markers arrive", () => {
    let t = applyCitation(turn(), cite(1, "a1"));
    t = applyCitation(t, cite(2, "a2"));
    t = applyCitation(t, cite(3, "a3"));
    expect(t.citations).toEqual(["a1", "a2", "a3"]);
  });

  it("slots by n so a repeated frame overwrites rather than duplicates", () => {
    let t = applyCitation(turn(), cite(1, "a1"));
    t = applyCitation(t, cite(2, "a2"));
    t = applyCitation(t, cite(2, "a2"));
    expect(t.citationDetails).toHaveLength(2);
    expect(t.citations).toEqual(["a1", "a2"]);
  });

  it("keeps card n at position n when an earlier frame is missing", () => {
    // A dropped frame must not promote source 3 into slot 1 — the [3] chip in
    // the prose would then point at the wrong card.
    const t = applyCitation(turn(), cite(3, "a3"));
    expect(t.citationDetails).toHaveLength(3);
    expect(t.citationDetails[2].atom_id).toBe("a3");
    expect(t.citationDetails[0].atom_id).toBe("");
  });

  it("ignores a non-positive display number", () => {
    const t0 = turn();
    expect(applyCitation(t0, cite(0, "a0"))).toBe(t0);
  });

  it("hands over to the enriched final list", () => {
    let t = applyCitation(turn(), cite(1, "a1", "live snippet"));
    const final: ChatFinal = {
      mode: "synthesized",
      citations: ["a1"],
      citation_details: [
        {
          atom_id: "a1",
          graph_id: "g1",
          graph_name: "Primary ocean",
          source_type: "slack",
          content: "enriched snippet",
        },
      ],
    } as ChatFinal;
    t = applyFinal(t, final);
    expect(t.citationDetails[0].graph_name).toBe("Primary ocean");
    expect(t.citationDetails[0].content).toBe("enriched snippet");
    expect(t.pending).toBe(false);
    expect(t.walk).toBeNull();
  });

  it("keeps the walk this question generated", () => {
    const walk = {
      merged: true,
      seeds: [{ id: "s1", title: "CoralSwarm", hop: 0 }],
      hops: [{ id: "h1", title: "CodeRabbit", hop: 1 }],
      currents: [{ src: "s1", dst: "h1", predicate: "works_on", derived: false }],
    };
    const t = applyFinal(emptyAssistant("a", "q1"), {
      mode: "synthesized",
      traversal_walk: walk,
    } as ChatFinal);
    expect(t.walk).toEqual(walk);
  });

  it("keeps the streamed sources when a truncated final carries none", () => {
    // The 2026-08-03 failure mode: the stream dies before `final`. Whatever was
    // cited before the cut has already been delivered and must survive.
    let t = applyCitation(turn(), cite(1, "a1"));
    t = applyFinal(t, { mode: "synthesized" } as ChatFinal);
    expect(t.citationDetails).toHaveLength(1);
    expect(t.citationDetails[0].atom_id).toBe("a1");
  });
});

describe("pendingLabel", () => {
  it("names retrieve / pack / synthesize / cite instead of a generic thinking", () => {
    expect(pendingLabel(null, false)).toBe("Starting your answer…");
    expect(pendingLabel("embedding", false)).toBe("Understanding your question…");
    expect(pendingLabel("retrieving", false)).toBe("Retrieving sources…");
    expect(pendingLabel("packing", false)).toBe("Preparing sources…");
    expect(pendingLabel("synthesizing", false)).toBe("Synthesizing answer…");
    expect(pendingLabel("citing", false)).toBe("Checking citations…");
  });

  it("says writing once prose has started, even if the stage is still packing", () => {
    expect(pendingLabel("packing", true)).toBe("writing…");
  });
});

describe("latestAssistantTurn", () => {
  const user = (text: string): Turn => ({ key: `u-${text}`, role: "user", text });

  it("follows a still-streaming answer so the drawer fills in live", () => {
    // The regression: this used to skip `pending` turns, so the sidebar sat on
    // the previous answer's sources for the whole generation and only swapped
    // at the end — while the thread showed the new cards arriving.
    const done = applyFinal(emptyAssistant("a", "q1"), {
      mode: "synthesized",
      citations: ["old"],
      citation_details: [{ atom_id: "old", graph_id: "g1" }],
    } as ChatFinal);
    const streaming = applyCitation(emptyAssistant("b", "q2"), cite(1, "new"));

    const picked = latestAssistantTurn([user("q1"), done, user("q2"), streaming]);
    expect(picked?.key).toBe("b");
    expect(picked?.citationDetails[0].atom_id).toBe("new");
  });

  it("returns the newest completed answer when nothing is in flight", () => {
    const first = applyFinal(emptyAssistant("a", "q1"), { mode: "synthesized" } as ChatFinal);
    const second = applyFinal(emptyAssistant("b", "q2"), { mode: "synthesized" } as ChatFinal);
    expect(latestAssistantTurn([user("q1"), first, user("q2"), second])?.key).toBe("b");
  });

  it("returns null when the thread has no assistant turn yet", () => {
    expect(latestAssistantTurn([])).toBeNull();
    expect(latestAssistantTurn([user("q1")])).toBeNull();
  });
});
