# Fresh-document frontend timing loop

This uses the actual production standalone Next app, AppProvider, real Clerk
DEV sign-in, query factories, authority fences, transport and page rendering.
The local HTTP server in `navigation.spec.ts` returns **synthetic backend DTOs**:
bootstrap takes 280 ms and each page/sidebar read takes 150 ms. It does not
verify backend JWT signatures, measure SQL or simulate production worker fills.

With the existing Clerk DEV keys exported, from `e2e/`:

```sh
E2E_API_PORT=13703 node stack/build-web.mjs --skip-install
E2E_API_PORT=13703 npx playwright test -c playwright.fresh-navigation-app.config.ts
```

Ports 13702 (Next) and 13703 (DTO replay) must be free. Both servers are owned
by the run and stopped afterward. Each scenario creates and deletes its own
DEV user through the existing ownership helpers. No production auth profile or
production backend is used.

Each of four routes gets three new-document samples under each cache condition.
The kept-cache case first warms the route; the cold case clears Chromium's HTTP
cache before each sample, retaining session cookies. Clerk testing interception
is removed after sign-in because interception disables browser asset caching.
Every sample asserts the actual `/_next/static/` transfer sizes: all retained
resources must be cached in the kept case, and none may be cached in the cold
case. Do not relabel an intercepted run as cache-kept.

A document-start MutationObserver records the real successful loaded marker
with `performance.now()`, its installation time, and `performance.timeOrigin`.
Navigation timing starts at zero. The probe does not wrap fetch, clone bodies,
replace auth responses, freeze timers or advance the loaded marker. Resource
timing is collected after readiness. The marker observes the application's
committed DOM; response-end to marker includes parsing, query notification and
React work, rather than isolating React CPU time.

`test-results/fresh-navigation-app/*/navigation-samples.json` retains safe timing
metadata, path-only resources and transfer sizes. It contains no headers,
tokens, query strings, user content or response bodies. Preserve the directory
between builds when comparing versions. Compare total time as well as first
app GET to readiness: the latter isolates the controlled bootstrap/read delivery
from variable real Clerk DEV network time. Three samples establish a repeatable
mechanism; they do not establish a production percentile or SLO.

Correctness has a separate red/green loop:

```sh
node stack/build-web.mjs --skip-install
npx playwright test -c playwright.stored-refresh-app.config.ts
```

This runs real Next/Clerk auth with intercepted backend DTOs. The shared
`initialAuthBrowser.ts` scenarios also run through `tests/initial-auth.spec.ts`
in the normal real-backend merge-queue stack. They hold optional UI delivery or
bootstrap, require early canonical reads, withhold page publication before
admission, and reject duplicate required reads after admission. Existing stale,
empty, withholding and refusal scenarios remain in that full-app loop.
