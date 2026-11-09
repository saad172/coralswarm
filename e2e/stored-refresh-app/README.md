# Stored refresh with the actual Next app

This local regression loop runs the same three stored-refresh helpers as
`tests/page-stored-refresh.spec.ts` through the standalone Next app, Clerk DEV
sign-in, AppProvider bootstrap, routing and the real query/transport code.
Backend responses are synthetic DTOs shaped like the successful landing reads
in merge-queue run 41921323287. Clerk traffic is never intercepted. Each test
creates and deletes its own DEV user with the existing ownership helpers.

With the Clerk DEV keys exported (see the main e2e README), from `e2e/`:

```sh
node stack/build-web.mjs --skip-install
npx playwright test -c playwright.stored-refresh-app.config.ts
```

Port 13702 is exclusive; Playwright starts and stops its own standalone web
server and refuses an existing one. No backend, Postgres or Docker is needed.
This checks the full frontend/auth boot path, not backend JWT verification or
worker throughput. The merge queue continues to run the original three tests
against the real backend and database through `playwright.config.ts`.

## Failure and fix

Run 41921323287 failed all three scenarios on both attempts. The screenshots
and error-context snapshots show only `surfacing…`. Network traces show a
successful authenticated Home bootstrap before `about:blank`, followed by a
successful Clerk client response on `/connectors`, but no new backend
bootstrap or connectors read. There were no console errors.

The original helper reproduces the same `storedRefreshBrowser.ts` readiness
timeout here (12.3s for the stale-data scenario). Changing only its fixed date
to the current date still reproduces the timeout. The helper froze browser
timers before navigating, then advanced only a few 1ms notification ticks
while waiting; the real Clerk/AppProvider boot never completed. The lean Vite
harness supplied already-ready auth and could not catch this.

The corrected helper installs the clock before navigation but lets it run
until the first authorized connectors request. It pauses before fulfilling
that response, anchors the certificate's 60s ETA to that paused instant, and
waits for the response before advancing recovery time. It preserves the
59s no-poll assertion, exact finite polling counts, empty loading/completion,
authority clearing, and no further reads after 403.
