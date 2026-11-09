# e2e

Playwright end-to-end scenarios against a real stack booted on the runner (or
your machine). This is the `e2e` check of `.github/workflows/pr.yml`. The scenarios run in the merge queue only; a PR push runs just the fast compile gate.

The PR gate's other required check, `guards`, runs separately: `scripts/ci_guards.sh`, the
marketing and widgets typecheck/build, and the web typecheck, which runs as part of
`next build` in `build web`.

| Piece | What runs |
|---|---|
| Backend | `target/debug/coralswarm-backend`, built `--features e2e` (debug only) |
| Control plane | pgvector Postgres in Docker. The CONTROL tree is applied from zero by the real `migrate_control` |
| Tenant DBs | Created by the backend's own provisioning job (`TENANT_PROVISIONER=local`). The TENANT tree is applied from zero |
| R2 | S3Mock in Docker (`adobe/s3mock`; MinIO no longer publishes images), reached through the fakes' outage proxy |
| OpenRouter, Stripe, Slack | `stack/fakes.mjs`, a deterministic fake HTTP server |
| Auth | The real Clerk **dev** instance, signed in with Clerk Testing Tokens (`@clerk/testing`). The backend verifies real JWTs; there is no bypass |
| Web | `web/` standalone build (`node server.js`), the same way production runs it |

## Run locally

You need Docker, Node 22+, and the Clerk dev-instance keys in your environment
(see "Secrets" below; strip any `\r` that a Windows `.env` leaves on values).

```sh
# 1. Build once: the backend (debug, e2e feature) and the web bundle
CORALSWARM_E2E_BUILD=1 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo build -p coralswarm-backend --features e2e \
  --bin coralswarm-backend --bin migrate_control --bin migrate_tenants
cd e2e && npm ci && npx playwright install chromium
node stack/build-web.mjs          # rebuild after any web/ change

# 2. Boot the stack. It detaches; re-run it to reset to a clean database.
#    It also runs the database assertions (see "Tenant isolation")
node stack/up.mjs

# 3. Run the scenarios
npx playwright test               # or: npx playwright test tests/ask.spec.ts --headed

# 4. Tear down (--keep-containers keeps Postgres and its migrated template)
node stack/down.mjs
```

Logs are in `e2e/.stack/{backend,web,fakes}.log`. A failing scenario keeps a
trace: `npx playwright show-trace test-results/<test>/trace.zip`.

`CORALSWARM_E2E_BUILD=1` is required: `backend/build.rs` refuses the `e2e` feature
without it, and in any optimized profile.

`CARGO_PROFILE_DEV_DEBUG=0` matters. Without it, the debug backend rlib grows
past 4 GB and the Windows linker fails with unresolved externals. On Windows,
`up.mjs` runs a copy of the backend with a 64 MiB main-thread stack, because
the debug `run()` future overflows Windows' default 1 MiB.

The default stack generates an ephemeral RSA key pair in `.stack/` for GitHub App
JWT verification and enables its live HTTP fake. To supply a pair, set both
`E2E_GITHUB_PRIVATE_KEY_FILE` and `E2E_GITHUB_PUBLIC_KEY_FILE`. The debug backend
uses a 16 MiB worker stack for the connector futures exercised by these scenarios.

Ports are `E2E_PG_PORT` 15580, `E2E_S3_PORT` 19090, `E2E_S3_PROXY_PORT` 19091,
`E2E_FAKES_PORT` 18090, `E2E_API_PORT` 18080 and `E2E_WEB_PORT` 13000. The web build bakes in the API
port. Set `E2E_STACK_ID` to run a second stack beside the first.

## Databases: migrate once, clone per shard

`up.mjs` applies the control tree with the real `migrate_control` into
`cs_e2e_tpl_<hash of migrations_control/>`. It builds under a scratch name,
renames when done, and marks the result `ALLOW_CONNECTIONS false`; this is the
same scheme `backend/src/test_db.rs` uses for the pg suites. Every boot then
runs `CREATE DATABASE w<shard> TEMPLATE cs_e2e_tpl_…`, and runs
`migrate_control --check` and `migrate_tenants` against the clone, as
`backend/release.sh` does on a deploy. If the migrations are unchanged, the
template is reused.

## Add a scenario

1. Make a `tests/<journey>.spec.ts`. Import `test`/`expect` from
   `../lib/fixtures`. `signedIn` gives you a fresh Clerk user who is signed in
   on `page`, plus `.api` for API calls as that user. `makeUser` makes more
   users.
2. API-only scenarios use Playwright's `request` fixture through `lib/api.ts`
   (see `tests/smoke.spec.ts`).
3. Select by role, label or placeholder, the way a user finds things. There are
   no `data-testid`s.
4. A behavior or API-contract PR adds or updates at least one scenario in the
   same PR. The PR template asks which one.

### Under 30 seconds per scenario

`timeout: 30_000` is the hard limit, and it is a design rule, not a number to
raise. A scenario that needs longer is two scenarios, or it is waiting on
something it should seed. Shards absorb the growth: add a shard (`SHARDS` and
the matrix in `pr.yml`), never lengthen one.

Playwright splits the run by test count, in its own scheduling order: every
scenario without a worker fixture first, in file order, then each
worker-fixture group. The heavy page-load and page-model specs all carry
worker fixtures, so an equal split leaves the last shard with most of the
minutes. `PWTEST_SHARD_WEIGHTS` in `pr.yml` gives each shard its share of the
count instead; the current weights come from the merge queue's per-test
durations and keep every shard near seven minutes of tests. When a shard's job
time drifts or a shard is added, list each shard
(`PWTEST_SHARD_WEIGHTS=… npx playwright test --list --shard=i/N --grep-invert
'@quarantine|@nightly'`), sum its durations from the last merge-queue logs,
and move the cuts. Playwright's CLI reads the variable but its sharding docs
do not mention it; an upgrade that drops it falls back to equal counts, which
the last shard's job time shows at once.

The local `standing-admission.spec.ts` runtime regression shares the real-extraction smoke's 180-second scenario / 140-second extraction window exception: it uses the unaccelerated 60-second production scheduler. It adds 106 synthetic idle accounts on its isolated stack, captures through the real API, and waits for the real managed worker against the fake provider. Its SQL is confined to setup/finalization of those exact UUIDs; it creates only one Clerk DEV user. Both success and failure finalize the owned inventory. The release smoke and its assertions are unchanged. Deterministic PostgreSQL regressions separately check exact cursor/continuation deadlines on both planes without real sleeps.

### Seeding

Seed through the real API: `POST /v1/ingest/atom`, `POST /v1/orgs`, invites,
and so on. Tests never write SQL.

There is exactly one test-only route, `POST /v1/e2e/seed/extraction`
(`backend/src/e2e_seed.rs`). It exists only in the `--features e2e` debug
build. Three guards keep that build out of any deploy:
- `build.rs` refuses it without opt-level 0 and `CORALSWARM_E2E_BUILD=1`.
- `lib.rs` refuses it without debug assertions.
- The binary refuses to start where `FLY_APP_NAME`, `FLY_MACHINE_ID` or
  `APP_ENV=production` is set. Real extraction
waits for the 60-second standing-admission sweep and then an LLM tool call.
This route writes the result instead: one fact per sentence of a source you
already captured through ingest, written as the caller, with RLS enforced. Do
not add a second seeding route. Extend the real API, or this one.

Payment is the real webhook: `completeStripeCheckout()` asks the fake Stripe to
complete the Checkout, then signs the event and posts it to
`/v1/stripe/webhook`.

Slack is the real OAuth routes and the real webhook: the fake Slack names the
workspace after the OAuth code, so a scenario connects its own, and
`tests/slack-ingest.spec.ts` signs each Events API callback with the stack's
signing secret and posts it to `/v1/slack/events`.

R2 outages are the fakes' proxy in front of S3Mock, which the backend uses as
its R2 endpoint. `r2Outage(request, oceanId, true)` makes every object write
under that one ocean answer 503 SlowDown until `r2Outage(..., false)`; nothing
else changes, so parallel scenarios are unaffected.
`tests/ingest-r2-outage.spec.ts` uses it.

### Tags

- `@smoke`: needs no e2e-only route, so it can run against staging with real
  providers at release time (see "Remote mode").
- `@quarantine`: flaky. It is skipped by the blocking run and runs only in the
  non-blocking `e2e quarantine` job. Fix it or delete it.
- `@isolation`: a tenant-isolation scenario (next section). Run them alone with
  `npx playwright test --grep @isolation`.
- `@nightly`: kept out of the merge queue. Page-load budget samples outside the
  spec's `CORE` set carry it; the full budget matrix runs in the manual
  `page-load-speed.yml` workflow (`budget` job, `playwright.page-load-budget.config.ts`).

## Tenant isolation

These replace ci.yml's tenant-isolation Postgres suites (`cross-tenant-probe`,
`consumer-rls`, `rls-probe-harness`, `binding-gate`,
`tenant-role-provisioning`, `definer-search-path`, `migrator-dsn-split`). The
old suites asserted on SQL; these assert on the product, as real users, so a
pass cannot come from a test connecting as a role that bypasses RLS (the #700
defect). They come in two halves.

**Scenarios** (`tests/isolation-*.spec.ts`, helpers in `lib/isolation.ts`).
The attacker guesses the victim's REAL ids on every route that takes one, and
`expectIndistinguishable()` requires that the answer is a refusal, is exactly
what an id that never existed gets (status and body, ids masked), and never
contains the victim's content. Every scenario also has a positive control (the
owner can read it), so a gate that refuses everything cannot pass.

| Spec | What it proves |
|---|---|
| `isolation-personal` | One user cannot read, write, ask about or delete another user's personal ocean, atoms or sources |
| `isolation-tenants`, first block | Two real self-serve orgs with their own tenant databases, both live in one backend: neither reaches the other's oceans or atoms, writes, asks, or admin routes (members, invites, org-id routes). A member never sees another member's private ocean, an org session is never served the consumer plane, and a personal session is never served the tenant plane |
| `isolation-tenants`, binding gate | `<slug>.<apex>` answers 410 and foreign hosts 404 before auth; `X-Forwarded-Host`/`Forwarded`/`X-Original-Host` never change a verdict; an org whose tenant is not ready refuses its org session instead of falling back to the personal plane |

The two orgs are a worker fixture (`tenants`): each shard provisions them once
(~7 s each, in parallel), and the scenarios only read them or assert that a
write did nothing. Org sessions come from `orgApi()`: Clerk activates an org on
a session created after the user joined it, and the claim is checked. Session
JWTs are cached for 40 s (`cached()`), because the Clerk dev instance's rate
limit is shared by every run.

The self-serve org caps key on the registrable domain (every test user is
`@example.com`, and per-run subdomains would share one bucket) plus a
stack-wide hourly cap, so `stack/config.mjs` raises
`ORG_SELFSERVE_MAX_PER_DOMAIN_PER_HOUR` and `ORG_SELFSERVE_MAX_PER_HOUR` for the
e2e backend. The caps themselves are covered by the `orgs_create` pg tests.

**Boot assertions** (`stack/assert-db.mjs`, run by `up.mjs` after the release
migrations; `--no-db-assert` skips them, `node stack/assert-db.mjs` re-runs
them). They depend only on the tree, so in CI only shard 1's boot runs them
(`E2E_DB_ASSERT=1` forces them on any boot); every local boot runs them. These are the invariants that live in the schema or in how the binaries
treat it, which no request can see. Each one fails the boot:

| Check | What it proves |
|---|---|
| `serving-role` | Requests are served as `coralswarm_app`: LOGIN, NOSUPERUSER, NOBYPASSRLS, `row_security` on (read in its own session), owns nothing, cannot write `tenant_databases` or run DDL. There is no `MIGRATOR_DATABASE_URL`. The only other DSNs are the in-process provisioning workers' (`PROVISIONER_DATABASE_URL`, a NOSUPERUSER NOBYPASSRLS login unrelated to the app role, and `TENANT_PROVISIONER_ADMIN_DSN`, the local stand-in for Neon's project API), and only those workers' files in `backend/src` reference them. This is what makes the scenarios above non-vacuous |
| `rls-strict` | As that role, an unscoped read of an RLS table is an error (no `missing_ok`), and a scope naming no account sees nothing |
| `rls-coverage` | Every control table with an `account_id`/`graph_id` column has RLS, except the 28 in `RLS_EXEMPT_SCOPED_TABLES`. The list is a ratchet: it fails when it changes in either direction |
| `tenant-owner` | The real `migrate_tenants` applies the tenant tree under a Neon-shaped owner (CREATEDB CREATEROLE, not superuser); the resulting `coralswarm_tenant_app` is NOSUPERUSER NOBYPASSRLS and the managed-clock role is sealed |
| `definer-search-path` | `scripts/check_definer_search_path.sh --self-test`, then `--database` on the control clone and on that tenant database |
| `provisioning-privileges` | `scripts/check_provisioning_privileges.sh --database` on the control clone |
| `migrator-split` | `migrate_control` refuses the app role and exits 2 with no DSN; `--check` flags a database one migration behind; the server refuses to boot against a database behind this build or never migrated, naming `migrate_control` |

The guard scripts run their SQL through `stack/psql-in-container.sh`, so the
runner needs no psql of its own.

Not ported, deliberately: the tampering cases of
`tenant_rls_role_provisioning_pg` (a BYPASSRLS-tampered tenant role must fail
the migration closed; a superuser owner self-heals it) alter a CLUSTER-wide
role that the running stack's tenant databases share.

## N-1 compatibility (merge queue only)

Rollback redeploys the previous release's image (decision 11). So the backend
in production must keep serving the schema the next release migrates AND the
data the next release writes. `pr.yml` answers that on `merge_group` only, as
part of the required `e2e` check, in three jobs:

1. `n1-locate` runs `stack/n1-prior-release.sh`, which finds the latest
   published `server-*` release. The binary comes from the release's
   `backend-dist.tar.gz` asset (attached by the Release workflow, never
   expires) or else the tag build's `backend-release-<sha>` artifact, which
   expires after days. With no `server-*` release yet it prints a notice and
   the check passes.
2. `n1-build-old` runs only when neither exists. It builds the tag the way
   `deploy.yml` does: release profile, `rust:1-trixie`, no `e2e` feature. It
   runs on a standard runner, with two jobs and deploy.yml's disk reclaim.
3. `n1-compat` runs the scenarios in `n1/` with `playwright.n1.config.ts`.
   They live outside `tests/`, so the shards never run them.
   - `node stack/up.mjs --no-db-assert` boots THIS tree. Its migrators apply
     both trees from zero, and its e2e backend is the one serving.
   - `n1/seed.spec.ts` (`@n1-seed`) writes the data through the API: two
     users, a source with seeded extraction, and an org with a tenant
     database, a member and private-ocean content. It records the ids in
     `.stack/n1-manifest.json`.
   - `node stack/n1-backend.mjs --bin-dir DIR --image debian:trixie-slim`
     stops the e2e backend and boots the release binary on the same
     databases, on trixie as in production. It fails fast if the process
     exits.
   - `n1/serve.spec.ts` (`@n1`) reads everything back through the release
     binary: readiness, oceans, bootstrap, sources, atoms, the org's tenant
     plane and members. It checks that isolation still holds and makes one
     light write (a new capture). It never seeds, asks the LLM, or touches
     blob storage or Stripe.

A release build has none of the `e2e` feature's seams, so `n1-backend.mjs`
boots it without R2 (blob storage disabled) and without the Stripe base-URL
override. It also cannot use the fake OpenRouter. A non-https
`OPENROUTER_BASE_URL` panics (the SSRF guard in `llm::openai::with_base_url`),
the embedder ignores that variable, and at boot the release build embeds one
string against the real OpenRouter, refusing to start if that fails. No
release-build switch avoids this. So N-1 runs with a real key,
`N1_OPENROUTER_API_KEY` (CI secret `E2E_N1_OPENROUTER_API_KEY`), and with the
standing extraction sweep off. That leaves the boot probe and one capture's
embedding as the only provider calls. Without the secret, the job fails in
`n1-backend.mjs` before booting, rather than passing without a check.

The seed and serve runs share Clerk users, so both run with
`E2E_SKIP_CLERK_CLEANUP=1` and one `E2E_RUN_ID`. The job cleans up once, at
the end.

Locally (with a stack from `node stack/up.mjs`):

```sh
E2E_RUN_ID=n1local E2E_SKIP_CLERK_CLEANUP=1 npx playwright test -c playwright.n1.config.ts seed
node stack/n1-backend.mjs --bin-dir <dir with another coralswarm-backend build>
E2E_RUN_ID=n1local E2E_SKIP_CLERK_CLEANUP=1 npx playwright test -c playwright.n1.config.ts serve
node stack/n1-backend.mjs --stop && E2E_RUN_ID=n1local node stack/clerk-cleanup.mjs
```

## Remote mode

Set `E2E_TARGET` to anything but `local` and the scenarios run against a
deployed stack instead of the local one. The release flow's staging smoke
works this way: deploy.yml `smoke-staging` runs `npx playwright test --grep @smoke`.

| Variable | Meaning |
|---|---|
| `E2E_TARGET` | e.g. `staging`. Unset or `local` means the local stack |
| `E2E_BASE_URL` | The target's web origin (required) |
| `E2E_API_BASE` | The target's backend origin (required) |
| `E2E_RELEASE_SHA` | Optional. The commit the backend should be serving. The check is enforced once `/healthz` reports a sha; until then the scenario only annotates a warning |
| `E2E_CLERK_SECRET_KEY`, `E2E_CLERK_PUBLISHABLE_KEY` | Keys for the Clerk instance the target uses. They win over `CLERK_SECRET_KEY` / `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY`. Testing Tokens need a development instance, so live keys are refused |

In remote mode:
- `stack/up.mjs` refuses to boot anything.
- Global setup only checks that the target answers.
- Only `@smoke` scenarios run, whatever `--grep` says.
- The seeding route and the fakes throw if a scenario reaches for them.
- Teardown deletes this run's Clerk users and orgs.

The `@smoke` set:
- Backend live and ready, plus the release sha check.
- Unauthenticated calls refused.
- First call provisions the personal ocean.
- Sign-in through a Clerk Testing Token.
- Ask with a cited answer, over content ingested through the real API, extracted by the real managed pipeline and answered by the real model.

That last scenario waits on the 60 s standing-admission sweep. It took
11–58 s locally and is budgeted at 140 s of its 180 s timeout. It is the one
exception to the 30 s rule. Locally it is skipped unless
`E2E_REAL_EXTRACTION=1` is set; there it runs against the fake LLM's
deterministic extraction.

To rehearse remote mode on your machine, with the local stack up:

```sh
E2E_TARGET=sim E2E_BASE_URL=http://localhost:13000 E2E_API_BASE=http://localhost:18080 \
  npx playwright test
```

## Secrets

| GitHub secret | Local env var | What |
|---|---|---|
| `E2E_CLERK_SECRET_KEY` | `CLERK_SECRET_KEY` or `E2E_CLERK_SECRET_KEY` | Clerk dev instance (`sk_test_…`) |
| `E2E_CLERK_PUBLISHABLE_KEY` | `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` or `E2E_CLERK_PUBLISHABLE_KEY` | Clerk dev instance (`pk_test_…`) |

The issuer is derived from the publishable key; `CLERK_ISSUER` overrides it.
The harness refuses `sk_live_`/`pk_live_` keys. Every other secret it uses is a
fixed fake in `stack/config.mjs`.

Every PR run and every laptop share the dev instance. Test users are
`cs-e2e-<run id>-…+clerk_test@example.com`. The run id is the CI run attempt
plus the shard; locally it is host + pid + time. Teardown deletes only its own
run's users and orgs, plus any harness user older than an hour. In CI,
`e2e-cleanup` always runs after the shards and removes the whole run
attempt's users, even when a shard was cancelled. A local run that lasts more
than an hour can lose its users to another run's sweep.

The dev instance holds at most 100 users across all of these. `makeUser`
deletes its users when the scenario ends. When Clerk answers
`user_quota_exceeded`, `createUser` deletes harness users older than 30
minutes (dead runs only) and retries for up to 150 s, extending the
scenario's timeout to match.

Clerk also rate-limits the Backend API per 10 s window, shared the same way,
and answers 429 `too_many_requests` with a Retry-After. Every call the harness
makes (`lib/clerk.ts`, `stack/clerk-cleanup.mjs`) goes through
`stack/clerk-client.mjs`, which waits out the Retry-After (else 1 s doubling
to 10 s) for up to the same 150 s and stretches the scenario's timeout by the
wait. Cleanup reads only a 404 as already gone; a 429 that outlasts its
retries fails the cleanup instead of leaking the user.

## Known gaps

- If a brand-new user's first two requests arrive together, one can see the
  account before its Personal ocean exists. `GET /v1/graphs` then answers `[]`.
  The `signedIn` fixture avoids this by provisioning through the API before
  the browser loads.
- `ORG_TEAMS_ENABLED`/`MEMBER_OCEANS_ENABLED` are on in the stack, because the
  members page answers 403 without them.
