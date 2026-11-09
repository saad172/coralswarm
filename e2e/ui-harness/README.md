# Lean connector UI browser checks

Uses production React components, Tailwind styles and API client, with Playwright-intercepted fixture responses and minimal Next navigation/auth shims. No backend, credentials, provider calls, or provider lifecycle claims. Navigation shims do not validate Next routing or the full app shell. Fonts use local fallbacks.

From the worktree root:

```sh
web/node_modules/.bin/tailwindcss -c web/tailwind.config.ts -i web/app/globals.css -o e2e/ui-harness/styles.css --content 'web/components/**/*.{ts,tsx},web/app/**/*.{ts,tsx}'
cd e2e
npx playwright test -c playwright.ui.config.ts
```

Port 23600 is exclusive; Playwright starts/stops Vite and refuses existing servers. Set UI_ARTIFACTS to a durable absolute output directory. Desktop 1280x900; mobile 390x844.
