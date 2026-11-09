import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

// Every `<Row icon=… hue=…>` / `<Icon name=… hue=…>` resolves to
// /addin/icons/<name>--<hue>.png. A combination with no PNG renders as a
// broken image inside Teams/Outlook, and no render test notices (jsdom never
// loads images), so pin the literal combinations against the files on disk.
const dir = join(__dirname, "..", "..", "public", "addin", "icons");
const present = new Set(readdirSync(dir).filter((f) => f.endsWith(".png")).map((f) => f.replace(/\.png$/, "")));
const sources = readdirSync(__dirname).filter((f) => f.endsWith(".tsx") && !f.includes(".test."));

function literalCombos(src: string): string[] {
  const combos: string[] = [];
  // Ternary icon/hue pairs (e.g. MeetingPane's status row) are listed explicitly below.
  for (const m of src.matchAll(/<(?:Row|Icon)\b[^>]*?\b(?:icon|name)="([a-z0-9-]+)"([^>]*)/g)) {
    if (/\bhue=\{/.test(m[2])) continue; // computed hue (e.g. kit's plus row) — not a literal combination
    const hue = /\bhue="([a-z]+)"/.exec(m[2])?.[1] ?? "ink";
    combos.push(`${m[1]}--${hue}`);
  }
  return combos;
}

describe("add-in icon assets", () => {
  it("found the icon directory and component sources", () => {
    expect(present.size).toBeGreaterThan(10);
    expect(sources.length).toBeGreaterThan(3);
  });

  it("every literal icon/hue combination has a PNG", () => {
    const missing = sources.flatMap((f) => literalCombos(readFileSync(join(__dirname, f), "utf8"))).filter((c) => !present.has(c));
    expect(missing).toEqual([]);
  });

  it("every MeetingPane status-row combination has a PNG", () => {
    for (const c of ["check--ok", "key--ink", "sync--teal", "subject--amber"]) expect(present.has(c), c).toBe(true);
    const src = readFileSync(join(__dirname, "MeetingPane.tsx"), "utf8");
    expect(src).not.toMatch(/"coral" : "ink"\}/);
  });

  it("the Teams hero card glyphs have PNGs", () => {
    for (const c of ["video--coral", "check--ok"]) expect(present.has(c), c).toBe(true);
    const src = readFileSync(join(__dirname, "TeamsAddMeeting.tsx"), "utf8");
    expect(src).toContain('glyph={added ? "check" : "video"} hue={added ? "ok" : "coral"}');
  });
});
