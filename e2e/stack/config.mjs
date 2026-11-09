// One place for every port, name and fixed test secret the stack uses, so
// `up.mjs`, `down.mjs`, the Playwright config and the web build agree.
//
// Nothing here is a real credential. The only real secrets the stack reads
// are the Clerk DEV instance keys, from the environment (README "Secrets").

import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
export const E2E_DIR = path.resolve(here, "..");
export const REPO = path.resolve(E2E_DIR, "..");
export const STATE_DIR = path.join(E2E_DIR, ".stack");

const env = process.env;
const num = (name, fallback) => Number(env[name] ?? fallback);

// One stack per runner (one per CI shard). `E2E_STACK_ID` keeps two local
// stacks (or another session's) from sharing container names.
export const STACK_ID = (env.E2E_STACK_ID ?? "local").replace(/[^a-z0-9-]/gi, "").toLowerCase() || "local";

export const PORTS = {
  pg: num("E2E_PG_PORT", 15580),
  s3: num("E2E_S3_PORT", 19090),
  // The fakes' outage proxy in front of S3: the backend's R2 endpoint.
  s3Proxy: num("E2E_S3_PROXY_PORT", 19091),
  fakes: num("E2E_FAKES_PORT", 18090),
  api: num("E2E_API_PORT", 18080),
  internal: num("E2E_INTERNAL_PORT", 18081),
  web: num("E2E_WEB_PORT", 13000),
};

/** Where the local stack (`up.mjs`) serves. */
export const LOCAL_API_BASE = `http://localhost:${PORTS.api}`;
export const LOCAL_WEB_BASE = `http://localhost:${PORTS.web}`;

// Remote mode (README "Remote mode"): `E2E_TARGET` names a deployed
// environment (e.g. `staging`) and the scenarios run against
// E2E_BASE_URL / E2E_API_BASE instead of the local stack. Only `@smoke`
// scenarios may run there: they use production routes and real providers.
export const TARGET = (env.E2E_TARGET ?? "local").trim() || "local";
export const REMOTE = TARGET !== "local";
const trimSlash = (v) => v.replace(/\r/g, "").trim().replace(/\/+$/, "");
if (REMOTE && (!env.E2E_BASE_URL || !env.E2E_API_BASE)) {
  throw new Error(`e2e: E2E_TARGET=${TARGET} needs E2E_BASE_URL (web) and E2E_API_BASE (backend)`);
}
/** Where the scenarios point: the remote target, else the local stack. */
export const API_BASE = REMOTE ? trimSlash(env.E2E_API_BASE) : LOCAL_API_BASE;
export const WEB_BASE = REMOTE ? trimSlash(env.E2E_BASE_URL) : LOCAL_WEB_BASE;
/** The commit the remote backend must be serving (release smoke). */
export const RELEASE_SHA = (env.E2E_RELEASE_SHA ?? "").trim();

export const CONTAINERS = {
  pg: `cs-e2e-pg-${STACK_ID}`,
  s3: `cs-e2e-s3-${STACK_ID}`,
};

export const IMAGES = {
  pg: "pgvector/pgvector:pg16",
  // MinIO no longer publishes images; S3Mock is the S3-compatible stand-in
  // for R2 (path-style, initial bucket from env).
  s3: "adobe/s3mock:4.7.0",
};

export const BUCKET = "coralswarm-e2e";

// The control-plane database the backend serves from, cloned per stack from
// the migrated template (`up.mjs`). One per shard: shards are separate
// runners, so `w${shard}` only has to be unique within one cluster.
export const CONTROL_DB = env.E2E_DB_NAME ?? `w${env.E2E_SHARD_INDEX ?? "0"}`;

export const pgUrl = (user, db) => `postgres://${user}@127.0.0.1:${PORTS.pg}/${db}`;

const exe = process.platform === "win32" ? ".exe" : "";
export const BIN_DIR = path.resolve(env.E2E_BACKEND_BIN_DIR ?? path.join(REPO, "target", "debug"));
export const BINS = {
  backend: path.join(BIN_DIR, `coralswarm-backend${exe}`),
  migrateControl: path.join(BIN_DIR, `migrate_control${exe}`),
  migrateTenants: path.join(BIN_DIR, `migrate_tenants${exe}`),
};

export const WEB_DIR = path.join(REPO, "web");

// Fixed, obviously-fake values for the secrets the backend requires.
export const FAKE = {
  internalToken: "e2e-internal-token-not-a-real-secret",
  masterKek: "e2e0".repeat(16),
  accountKek: "e2e1".repeat(16),
  stripeSecret: "sk_test_e2efakekeynotarealstripekey00000",
  stripeWebhookSecret: "whsec_e2efakewebhooksecret000000000000",
  s3Key: "e2e-access",
  s3Secret: "e2e-secret-key",
  slackSigningSecret: "e2e-fake-slack-signing-secret",
  oauthStateSecret: "e2e-fake-oauth-state-secret-0000000000000000",
  googlePushServiceAccount: "e2e-push@coralswarm-e2e.iam.gserviceaccount.com",
  // Slack's OAuth app and Composio's API key. The fake Slack checks the id and
  // secret; keep these in step with `FAKE_SLACK` in stack/fakes.mjs.
  slackClientId: "e2e-slack-client-id",
  slackClientSecret: "e2e-slack-client-secret",
  // Verifies the signature of the events a scenario posts to /v1/slack/events,
  // as Slack's own signing secret does (tests/connectors-api.spec.ts signs them).
  slackSigningSecret: "e2e-slack-signing-secret-not-a-real-secret",
  composioKey: "e2e-fake-composio-key",
  oauthStateSecret: "e2e-oauth-state-secret-not-a-real-secret-0123456789",
};

/** The audience Pub/Sub push tokens carry: the push route's own URL. */
export const GOOGLE_PUSH_AUDIENCE = `${LOCAL_API_BASE}/v1/webhooks/google`;

const strip = (v) => (v ?? "").replace(/\r/g, "").trim();

/** The Clerk DEV instance. Required; never a production instance. */
export function clerk() {
  // E2E_-prefixed names win: a release job can carry the e2e Clerk instance's
  // keys beside the deployment's own NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY.
  const secretKey = strip(env.E2E_CLERK_SECRET_KEY || env.CLERK_SECRET_KEY);
  const publishableKey = strip(env.E2E_CLERK_PUBLISHABLE_KEY || env.NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY || env.CLERK_PUBLISHABLE_KEY);
  // The Frontend API host is encoded in the publishable key
  // (`pk_test_` + base64("<host>$")), so CLERK_ISSUER is optional.
  let issuer = strip(env.CLERK_ISSUER).replace(/\/+$/, "");
  if (!issuer && publishableKey) {
    const host = Buffer.from(publishableKey.replace(/^pk_(test|live)_/, ""), "base64").toString("utf8").replace(/\$$/, "");
    if (host) issuer = `https://${host}`;
  }
  return { secretKey, publishableKey, issuer };
}

export function requireClerk() {
  const c = clerk();
  const missing = [];
  if (!c.secretKey) missing.push("CLERK_SECRET_KEY");
  if (!c.publishableKey) missing.push("NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY");
  if (missing.length) throw new Error(`e2e: missing Clerk dev-instance secrets: ${missing.join(", ")} (see e2e/README.md)`);
  if (!c.secretKey.startsWith("sk_test_") || !c.publishableKey.startsWith("pk_test_")) {
    throw new Error("e2e: Clerk keys must belong to a DEVELOPMENT instance (sk_test_/pk_test_); refusing to run against production");
  }
  return c;
}

/** Environment for the backend process: a debug `--features e2e` build. */
export function backendEnv() {
  const c = requireClerk();
  const fakes = `http://127.0.0.1:${PORTS.fakes}`;
  return {
    RUST_LOG: env.E2E_RUST_LOG ?? "info,sqlx=warn,tower_http=warn",
    RUST_BACKTRACE: "1",
    // L1 ships observe-only; opt in to exercise Other shedding locally.
    CONCURRENCY_ENFORCE: env.E2E_CONCURRENCY_ENFORCE ?? "0",
    APP_ENV: "development",
    PORT: String(PORTS.api),
    INTERNAL_PORT: String(PORTS.internal),
    INTERNAL_TOKEN: FAKE.internalToken,
    BASE_URL: LOCAL_API_BASE,
    FRONTEND_URL: LOCAL_WEB_BASE,
    E2E_WEB_ORIGIN: LOCAL_WEB_BASE,
    E2E_API_ORIGIN: LOCAL_API_BASE,
    DATABASE_URL: pgUrl("coralswarm_app", CONTROL_DB),
    PROVISIONER_DATABASE_URL: pgUrl("cs_e2e_provisioner", CONTROL_DB),
    TENANT_PROVISIONER: "local",
    TENANT_PROVISIONER_ADMIN_DSN: pgUrl("postgres", "postgres"),
    TENANT_APEX_DOMAIN: "localhost",
    WEB_APEX_DOMAIN: "localhost",
    MULTITENANT_ENABLED: "1",
    ORG_SELFSERVE_SIGNUP_ENABLED: "1",
    // The self-serve org caps key on the REGISTRABLE domain (Public Suffix
    // List, so per-run subdomains share one bucket) and also cap the whole
    // stack per hour. Every test user is @example.com, so the production
    // defaults (10 per domain, 30 overall, halved when throttled) would fail
    // a shard once org scenarios, retries and worker respawns add up. The
    // caps' own behaviour is covered by routes/orgs_create pg tests.
    ORG_SELFSERVE_MAX_PER_DOMAIN_PER_HOUR: "1000",
    ORG_SELFSERVE_MAX_PER_HOUR: "1000",
    // The org member roster (`GET /v1/orgs/members`, Settings › Members)
    // answers 403 without org teams, and org teams require member oceans.
    ORG_TEAMS_ENABLED: "1",
    MEMBER_OCEANS_ENABLED: "1",
    // The Home dashboard reads: /v1/home/metrics (tests/home-metrics.spec.ts), /v1/connectors/status, /v1/home/workspace-activity (tests/workspace-activity.spec.ts). 404 while unset.
    HOME_DASHBOARD_ENABLED: "1",
    // Chat find: `source_results` tiles on a find-question answer and
    // `GET /v1/find/titles` (404 while unset). backend/src/services/find_results.rs.
    CHAT_FIND_RESULTS_ENABLED: "1",
    // Today activity cards (`GET /v1/journal/activities`) answer 404 while this
    // is unset (backend/src/services/journal_activities.rs).
    TODAY_ACTIVITIES_ENABLED: "1",
    // Today gist (`GET /v1/journal/gist`) answers 404 while this is unset
    // (backend/src/services/journal_gist.rs). The compile half stays idle
    // here: it runs inside the journal sweep, which managed enrichment gates.
    TODAY_GIST_ENABLED: "1",
    MESSAGE_FEEDBACK_ENABLED: "1",
    MASTER_KEK_HEX: FAKE.masterKek,
    ACCOUNT_MASTER_KEK: FAKE.accountKek,
    CLERK_SECRET_KEY: c.secretKey,
    CLERK_ISSUER: c.issuer,
    // Signs the OAuth `state` a connect carries through the provider and back to
    // the callback. Without it every connect answers 503.
    OAUTH_STATE_SECRET: FAKE.oauthStateSecret,
    OPENROUTER_API_KEY: "e2e-fake-openrouter-key",
    OPENROUTER_BASE_URL: `${fakes}/openrouter/api`,
    OPENROUTER_GENERATION_LOOKUP_DISABLED: "1",
    ...{
      ORG_GITHUB_ENABLED: "1",
      GITHUB_APP_ID: "12345",
      GITHUB_APP_SLUG: "e2e-github",
      GITHUB_CLIENT_ID: "e2e-github",
      GITHUB_CLIENT_SECRET: "e2e-github-secret",
      GITHUB_APP_PRIVATE_KEY_FILE: env.E2E_GITHUB_PRIVATE_KEY_FILE ?? path.join(STATE_DIR, "github-private.pem"),
      GITHUB_WEBHOOK_SECRET: "e2e-github-webhook",
      GITHUB_API_BASE: `${fakes}/github/api`,
      E2E_GITHUB_WEB_BASE: `${fakes}/github`,
    },
    STRIPE_SECRET_KEY: FAKE.stripeSecret,
    STRIPE_WEBHOOK_SECRET: FAKE.stripeWebhookSecret,
    STRIPE_API_BASE: `${fakes}/stripe`,
    // A Slack workspace connects through the real OAuth routes against the
    // fake Slack, and its events arrive signed on the real webhook.
    SLACK_SIGNING_SECRET: FAKE.slackSigningSecret,
    OAUTH_STATE_SECRET: FAKE.oauthStateSecret,
    // Slack's Web API and Composio's REST API are in-runner fakes; the
    // backend reads both bases only in an `e2e` build (Composio) or whenever
    // set (Slack, `connectors::slack::slack_api_base`).
    SLACK_CLIENT_ID: FAKE.slackClientId,
    SLACK_CLIENT_SECRET: FAKE.slackClientSecret,
    SLACK_SIGNING_SECRET: FAKE.slackSigningSecret,
    SLACK_API_BASE: `${fakes}/slack/api`,
    MICROSOFT_CLIENT_ID: "e2e-ms-client",
    MICROSOFT_CLIENT_SECRET: "e2e-ms-secret",
    E2E_MICROSOFT_AUTHORITY_BASE: `${fakes}/microsoft`,
    E2E_MICROSOFT_GRAPH_BASE: `${fakes}/microsoft/graph`,
    COMPOSIO_API_KEY: FAKE.composioKey,
    COMPOSIO_API_BASE: `${fakes}/composio/api/v3.1`,
    COMPOSIO_CAPTURE_POLLING_ENABLED: "true",
    E2E_COMPOSIO_POLL_INTERVAL_MS: "250",
    COMPOSIO_JIRA_AUTH_CONFIG_ID: "ac_e2e_jira",
    COMPOSIO_LINEAR_AUTH_CONFIG_ID: "ac_e2e_linear",
    COMPOSIO_CONFLUENCE_AUTH_CONFIG_ID: "ac_e2e_confluence",
    COMPOSIO_HUBSPOT_AUTH_CONFIG_ID: "example_14",
    COMPOSIO_ZENDESK_AUTH_CONFIG_ID: "ac_e2e_zendesk",
    COMPOSIO_DART_AUTH_CONFIG_ID: "ac_e2e_dart",
    R2_ACCOUNT_ID: "e2e",
    R2_ACCESS_KEY_ID: FAKE.s3Key,
    R2_SECRET_ACCESS_KEY: FAKE.s3Secret,
    R2_BUCKET_NAME: BUCKET,
    // Through the fakes' proxy, so a scenario can stage an R2 outage.
    R2_ENDPOINT_OVERRIDE: `http://127.0.0.1:${PORTS.s3Proxy}`,
    // Pub/Sub push verification against the fake Google signing key.
    GOOGLE_CALENDAR_CLIENT_ID: "e2e-google-client",
    GOOGLE_CALENDAR_CLIENT_SECRET: "e2e-google-secret",
    GOOGLE_GMAIL_PUBSUB_TOPIC: "projects/e2e/topics/gmail",
    GOOGLE_CHAT_PUBSUB_TOPIC: "projects/e2e/topics/chat",
    GOOGLE_MEET_PUBSUB_TOPIC: "projects/e2e/topics/meet",
    ORG_GOOGLE_GMAIL_ENABLED: "true",
    ORG_GOOGLE_CHAT_ENABLED: "true",
    ORG_GOOGLE_MEET_ENABLED: "true",
    GOOGLE_CALENDAR_WEBHOOK_URL: `${LOCAL_API_BASE}/v1/webhooks/google/calendar`,
    E2E_GOOGLE_BASE: `${fakes}/google-native`,
    GOOGLE_PUSH_AUDIENCE,
    GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL: FAKE.googlePushServiceAccount,
    GOOGLE_OIDC_CERTS_URL: `${fakes}/google/oauth2/v3/certs`,
    // Prod's [env] (fly.toml) for the paths the journeys touch.
    GRAPH_TRAVERSAL_ENABLED: "true",
    TOKIO_WORKER_THREADS: "2",
    // Debug connector futures exceed the runtime worker default stack in CI.
    RUST_MIN_STACK: "16777216",
    // Real managed extraction runs (prod's fly.toml values), against the fake
    // LLM's deterministic extraction: the @smoke ask waits for it.
    STANDING_PROCESSING_ENABLED: "true",
    SOURCE_EXTRACTION_PROFILE: "source-atoms-v3",
  };
}
