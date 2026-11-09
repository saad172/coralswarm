import { dirname } from "path";
import { fileURLToPath } from "url";
import { FlatCompat } from "@eslint/eslintrc";
import tseslint from "typescript-eslint";

/* ESLint for web/.
 *
 * There was no linter here at all until now — `package.json` advertised a
 * `lint` script but nothing was installed, so `react-hooks/exhaustive-deps`,
 * `jsx-a11y/*`, `react/jsx-key` and every `@typescript-eslint` rule had never
 * run on this code. The script also called `next lint`, which is deprecated
 * and removed in Next 16; it is now plain `eslint .`.
 *
 * DELIBERATELY NO STYLISTIC/FORMATTING RULES. This repo is not prettier-clean
 * (a formatting pass produced 126 lines of pure churn and was reverted), and
 * CLAUDE.md treats reformatting as off-limits. This config is here to find
 * BUGS, not to restyle the codebase — if a rule's only effect is whitespace or
 * quote style, it does not belong here.
 *
 * Type-aware rules (`no-floating-promises`, `no-misused-promises`) are NOT on
 * yet: they need `parserOptions.project`, which makes a run several times
 * slower and adds a large batch of findings on top of the existing backlog.
 * Worth turning on once the current backlog is closed.
 */

/** The function passed straight to useEffect/useLayoutEffect — the place a
 *  data load used to live. Descendants include nested callbacks and promise
 *  chains, which is where `void load()` / `.then(...)` reads sit. */
const EFFECT_BODY = "CallExpression[callee.name=/^(useEffect|useLayoutEffect)$/] > :function";

/** The Clerk bearer STRING rotates about once a minute; see the block that
 *  applies these below. */
const BEARER_DEPENDENCY_RULES = [
  "CallExpression[callee.name=/^(useEffect|useLayoutEffect|useCallback|useMemo)$/] > ArrayExpression > Identifier[name='token']",
  "CallExpression[callee.name=/^(useEffect|useLayoutEffect|useCallback|useMemo)$/] > ArrayExpression > MemberExpression[property.name='token']",
  "CallExpression[callee.property.name=/^(useEffect|useLayoutEffect|useCallback|useMemo)$/] > ArrayExpression > Identifier[name='token']",
].map((selector) => ({
  selector,
  message:
    "Do not depend on the rotating bearer `token`: read it with getAuthToken() inside the request and key on authReady (lib/providers.tsx).",
}));

// Server reads go through React Query (lib/appQueries.ts,
// lib/libraryQueries.ts), not `useEffect` + `useState(true)`. An
// effect-owned fetch throws its data away on unmount, so every return
// to the screen showed a spinner and refetched; a cached query paints
// the last result instantly and revalidates in the background. These
// flag a request made inside an effect callback: `api.*`, `fetch` /
// `guardedFetch`, or a call to a local loader named load*/reload*/
// refresh*/refetch*/fetch*. A genuine exception (a stream or SSE
// subscription, polling an external auth popup, a native
// bridge, a one-shot mutation the effect exists to perform) takes an
// `eslint-disable-next-line no-restricted-syntax -- <reason>` on the
// flagged line.
const EFFECT_FETCH_RULES = [
  `${EFFECT_BODY} MemberExpression[object.name='api']`,
  `${EFFECT_BODY} CallExpression[callee.name=/^(fetch|guardedFetch)$/]`,
  `${EFFECT_BODY} CallExpression[callee.name=/^((load|reload|refresh|refetch)\\w*|fetch[A-Z]\\w*)$/]`,
  // `useEffect(load, [load])` — the loader passed as the effect itself.
  `CallExpression[callee.name=/^(useEffect|useLayoutEffect)$/] > Identifier:first-child[name=/^((load|reload|refresh|refetch)\\w*|fetch[A-Z]\\w*)$/]`,
].map((selector) => ({
  selector,
  message:
    "Do not fetch inside useEffect: read through a React Query queryOptions factory (lib/appQueries.ts) so returning to the screen paints cached data. A genuine exception (stream/SSE, auth-popup polling, native bridge) needs an eslint-disable with its reason.",
}));

const compat = new FlatCompat({ baseDirectory: dirname(fileURLToPath(import.meta.url)) });

export default [
  {
    ignores: [
      ".next/**",
      "out/**",
      "node_modules/**",
      "next-env.d.ts",
      // Archived pre-redesign screens, kept only for reference when porting
      // the remaining pages. Not shipped, not worth linting.
      "../archive/**",
    ],
  },

  // Next's own preset: bundles the next, react, react-hooks and jsx-a11y
  // plugins, including the Core Web Vitals rules.
  ...compat.extends("next/core-web-vitals"),

  // TypeScript rules, non-type-aware (see the note above).
  ...tseslint.configs.recommended,

  {
    rules: {
      // tsc already runs with noUnusedLocals/noUnusedParameters, so ESLint
      // repeating it would be duplicate noise in a different format.
      "@typescript-eslint/no-unused-vars": "off",

      // Not on by default, and each one caught a real defect in the kit
      // review: 7 kit buttons defaulted to type="submit" (fixed in 3ee8337).
      "react/button-has-type": "error",
      "react/no-array-index-key": "warn",
      "@typescript-eslint/no-non-null-assertion": "warn",
    },
  },

  {
    // The kit's Button/IconButton/SuggestionPill ARE the abstraction that
    // guarantees a type — they take `type` as a prop defaulting to "button"
    // and forward it. The rule only accepts a literal or a trivial ternary, so
    // it can't see through the forward and flags the safe wrapper while
    // staying useful on every bare <button> elsewhere.
    files: ["components/ui/Button.tsx"],
    rules: { "react/button-has-type": "off" },
  },

  {
    // The Clerk bearer STRING rotates about once a minute (and on most tab
    // returns). A data-loading effect or callback keyed on it re-runs that
    // often, and every such re-run that resets state blanks the page: that is
    // how Today, the coral page and the hour drilldown went to "loading"
    // whenever the user came back to the tab. Read the bearer inside the
    // request with `getAuthToken()` and key on `authReady` instead (see
    // `token` in lib/providers.tsx). Sites that predate this rule carry a
    // per-line disable with the reason; do not add new ones.
    files: ["app/**/*.{ts,tsx}", "components/**/*.{ts,tsx}"],
    // Tests drive effects and fetches deliberately, so only the fetch rule is
    // relaxed for them (next block); the bearer-dependency rule still applies.
    ignores: ["**/*.test.{ts,tsx}"],
    rules: {
      "no-restricted-syntax": ["error", ...BEARER_DEPENDENCY_RULES, ...EFFECT_FETCH_RULES],
    },
  },

  {
    // `no-restricted-syntax` is one rule, so a later block REPLACES its
    // selector list rather than adding to it: tests get the bearer-dependency
    // selectors without the fetch-in-effect ones (a test harness fetches in an
    // effect on purpose, e.g. components/org/DomainVerification.test.tsx).
    files: ["app/**/*.test.{ts,tsx}", "components/**/*.test.{ts,tsx}"],
    rules: {
      "no-restricted-syntax": ["error", ...BEARER_DEPENDENCY_RULES],
    },
  },

  {
    // Config files legitimately export an anonymous object/array.
    files: ["*.config.mjs", "*.config.ts"],
    rules: { "import/no-anonymous-default-export": "off" },
  },
];
