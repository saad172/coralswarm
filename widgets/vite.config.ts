import { defineConfig } from "vite";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import preact from "@preact/preset-vite";
import { viteSingleFile } from "vite-plugin-singlefile";

const __dirname = dirname(fileURLToPath(import.meta.url));

// vite-plugin-singlefile forces `output.codeSplitting = false` under Vite 8
// (Rolldown), which Rollup/Rolldown only allows with a SINGLE input — so
// this can't build all five entries in one `rollupOptions.input` map (that
// worked fine pre-8, but the option didn't exist then). Instead each
// template is its own single-entry build, selected by `WIDGET_ENTRY` —
// `scripts/build.mjs` drives the five invocations and cleans `dist/` once up
// front (see that script for why `emptyOutDir` is false here).
const ENTRIES = ["ask_ocean", "search_atoms", "get_reef", "recent_activity", "result-card-v1"] as const;
type EntryName = (typeof ENTRIES)[number];

const entry = process.env.WIDGET_ENTRY as EntryName | undefined;
if (!entry || !ENTRIES.includes(entry)) {
  throw new Error(`WIDGET_ENTRY must be one of ${ENTRIES.join(", ")} (got ${JSON.stringify(entry)})`);
}

// Deterministic output: no content hashes in filenames/chunks and no
// build-time timestamp comments — `scripts/check-drift.sh` rebuilds and
// diffs against `backend/assets/widgets/`, verified by building twice and
// diffing.
export default defineConfig({
  // Entries live directly under `root` so the emitted HTML lands flat at
  // `dist/<entry>.html` instead of mirroring `src/entries/` inside `dist/`.
  root: resolve(__dirname, "src/entries"),
  plugins: [preact(), viteSingleFile()],
  build: {
    outDir: resolve(__dirname, "dist"),
    emptyOutDir: false,
    cssCodeSplit: false,
    assetsInlineLimit: 100_000_000,
    modulePreload: false,
    target: "esnext",
    rollupOptions: {
      input: resolve(__dirname, `src/entries/${entry}.html`),
      output: {
        entryFileNames: "[name].js",
        chunkFileNames: "[name].js",
        assetFileNames: "[name].[ext]",
      },
    },
  },
});
