#!/usr/bin/env node
// Drives five single-entry `vite build` invocations (see vite.config.ts's
// own comment for why singlefile + multi-input can't share one build under
// Vite 8) and asserts the final `dist/` contains exactly the five expected
// files with no incidental output.

import { spawnSync } from "node:child_process";
import { rmSync, mkdirSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, "..");
const distDir = resolve(root, "dist");

const ENTRIES = ["ask_ocean", "search_atoms", "get_reef", "recent_activity", "result-card-v1"];

rmSync(distDir, { recursive: true, force: true });
mkdirSync(distDir, { recursive: true });

for (const entry of ENTRIES) {
  console.log(`\n> building ${entry}…`);
  const result = spawnSync("npx", ["vite", "build"], {
    cwd: root,
    stdio: "inherit",
    env: { ...process.env, WIDGET_ENTRY: entry },
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

const produced = readdirSync(distDir).sort();
const expected = ENTRIES.map((e) => `${e}.html`).sort();
const unexpected = produced.filter((f) => !expected.includes(f));
const missing = expected.filter((f) => !produced.includes(f));

if (unexpected.length > 0 || missing.length > 0) {
  console.error("dist/ does not match the expected five-file output.");
  if (missing.length) console.error("  missing:", missing.join(", "));
  if (unexpected.length) console.error("  unexpected:", unexpected.join(", "));
  process.exit(1);
}

console.log(`\nBuilt ${expected.join(", ")} in dist/.`);
