import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import tailwindConfig from "../tailwind.config";
import { DURATION_MS, EASE } from "./motion";

const css = readFileSync(path.resolve(__dirname, "../app/globals.css"), "utf8");

function cssVar(name: string): string | undefined {
  return css.match(new RegExp(`--${name}:\\s*([^;]+);`))?.[1].trim();
}

const CSS_EASING_KEYWORDS: Record<string, readonly number[]> = { ease: [0.25, 0.1, 0.25, 1] };

function bezier(value: string): readonly number[] | undefined {
  if (value in CSS_EASING_KEYWORDS) return CSS_EASING_KEYWORDS[value];
  return value.match(/^cubic-bezier\(([^)]+)\)$/)?.[1].split(",").map(Number);
}

describe("motion tokens", () => {
  it.each(Object.entries(DURATION_MS))("--dur-%s is %ims in globals.css", (key, ms) => {
    expect(cssVar(`dur-${key}`)).toBe(`${ms}ms`);
  });

  it.each(Object.entries(EASE))("--ease-%s matches the motion.ts curve", (key, curve) => {
    const value = cssVar(key === "ease" ? "ease" : `ease-${key}`);
    expect(value && bezier(value)).toEqual(curve);
  });

  it("globals.css defines no duration or easing that motion.ts lacks", () => {
    const durations = [...css.matchAll(/--dur-([\w-]+):/g)].map((m) => m[1]);
    const easings = [...css.matchAll(/--ease(?:-([\w-]+))?:/g)].map((m) => m[1] ?? "ease");
    expect(durations.sort()).toEqual(Object.keys(DURATION_MS).sort());
    expect(easings.sort()).toEqual(Object.keys(EASE).sort());
  });

  it("tailwind exposes every duration", () => {
    const durations = tailwindConfig.theme?.extend?.transitionDuration as Record<string, string>;
    expect(durations).toEqual(Object.fromEntries(Object.keys(DURATION_MS).map((k) => [k, `var(--dur-${k})`])));
  });

  it("tailwind type sizes match the --type-* shorthands", () => {
    const sizes = tailwindConfig.theme?.extend?.fontSize as Record<string, [string, string]>;
    for (const [key, name] of [["greeting", "greeting"], ["question", "question"], ["detail-title", "detail-title"], ["ask", "ask"],
      ["lead", "lead"], ["item", "item"], ["note", "note"], ["micro", "micro"]]) {
      const [size, lineHeight] = sizes[key];
      expect(cssVar(`type-${name}`)).toMatch(new RegExp(`^400 ${size}/${lineHeight} `));
    }
  });
});
