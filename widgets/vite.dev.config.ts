import { defineConfig } from "vite";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import preact from "@preact/preset-vite";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Separate from vite.config.ts (the production singlefile build, which
// requires WIDGET_ENTRY — see its own comment) so `npm run dev` can serve
// the harness page against source with normal HMR, no env var required.
export default defineConfig({
  root: resolve(__dirname, "dev"),
  plugins: [preact()],
  server: {
    fs: {
      // The harness imports fixtures from `../fixtures` and templates from
      // `../src`, both outside `dev/`.
      allow: [resolve(__dirname), resolve(__dirname, "../packages/answer-ui")],
    },
  },
});
