import path from "node:path";
import { defineConfig } from "vitest/config";

// Unit tests for the in-browser engine's pure modules (crypto, Shamir, vector).
// Node env — they use Web Crypto (present in Node) and need no DOM.
export default defineConfig({
  test: {
    environment: "node",
    setupFiles: ["./lib/testing/browserSetup.ts"],
    // `.test.tsx` files (component tests) opt into jsdom PER FILE via a
    // `// @vitest-environment jsdom` docblock instead of switching the
    // default here — Vitest 4 dropped `environmentMatchGlobs`, and a global
    // default of jsdom would be wrong for every existing `.test.ts` (Web
    // Crypto needs no DOM, and jsdom's `Date`/`Intl` shims are one more thing
    // that could quietly disagree with journal.test.ts's TZ-pinned
    // assertions).
    // `components/**` is included for the PURE model modules that live beside
    // their components (components/connectors/slackOnboardingModel.ts — the
    // one shared derivation docs/connectors/slack-onboarding.md §3 mandates).
    // `.test.tsx` runs component-render tests (`@testing-library/react` + jsdom).
    include: ["lib/**/*.test.ts", "lib/**/*.test.tsx", "components/**/*.test.ts", "components/**/*.test.tsx"],
    // Pinned to a real timezone (America/Chicago) rather than the runner's
    // default, for lib/journal.test.ts's DST-boundary tests — CI/local dev
    // don't share a TZ. `test.env` sets this in the worker BEFORE any test
    // module (including its static imports) runs, which an in-file
    // `process.env.TZ = ...` assignment can't guarantee once other imported
    // modules also touch `Date` at module scope.
    //
    // `LANG`/`LC_ALL` are pinned alongside it for the same reason: several
    // lib/journal.ts helpers (`dayRowLabel`, `weekRowLabel`, `monthCrumbLabel`,
    // `periodGapLabel`, ...) call `toLocaleDateString(undefined, ...)`, whose
    // output follows the runner's default `Intl` locale — Node resolves that
    // default from `LANG`/`LC_ALL` (verified: `LANG=de_DE.UTF-8 node -e
    // 'console.log(Intl.DateTimeFormat().resolvedOptions().locale)'` prints
    // `de-DE`), so the exact-string assertions in journal.test.ts would fail
    // on a non-`en-US` CI/dev machine without this.
    env: { TZ: "America/Chicago", LANG: "en_US.UTF-8", LC_ALL: "en_US.UTF-8" },
  },
  resolve: {
    // Mirrors tsconfig.json's "@/*" -> "./*" path mapping (Next's own
    // resolver honors that at build time; vitest needs it spelled out
    // explicitly since it doesn't read tsconfig `paths`). Needed as of
    // lib/askScope.test.ts, the first test file to import a module
    // (lib/askScope.ts) that itself VALUE-imports (not `import type`) from
    // components/ui.tsx, for the `HUES` array — a type-only import would
    // have been erased at compile time and never hit this at all.
    alias: {
      "@": path.resolve(__dirname, "."),
    },
  },
  // tsconfig.json sets "jsx": "preserve" (Next's own pipeline transforms
  // JSX downstream, not tsc) — Vite's default transformer (oxc, this Vite
  // major) auto-reads that from tsconfig and otherwise leaves raw JSX in
  // the output, which then fails import-analysis with no jsx-transform
  // plugin installed here (no @vitejs/plugin-react — oxc's own "automatic"
  // mode lowers JSX to `react/jsx-runtime` calls directly, which is enough
  // for both uses below: value-importing a .tsx module for its
  // non-component exports (e.g. askScope.ts importing the `HUES` array),
  // and now actually rendering a component in a `.test.tsx` file (see
  // `environmentMatchGlobs` above). (Setting the equivalent `esbuild.jsx`
  // instead is a no-op on this Vite major once `oxc.jsx` is also present —
  // Vite warns "both set" and keeps oxc's.)
  oxc: {
    jsx: { runtime: "automatic" },
  },
});
