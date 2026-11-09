//! `provision_stripe` — the idempotent provisioning binary named in the
//! registered plan (`.scratch/billing-self-serve/plan.md` §1.13/§2.C):
//! creates the 4 Products / 6 Prices, writes `stripe_price_id`s into the
//! control DB, and registers (or reconciles) the webhook endpoint, printing
//! its signing secret exactly once. `scripts/provision_stripe.sh` is the
//! thin shell wrapper (`backfill_billing_enforcement`'s sh+bin split,
//! followed exactly — see that pair for the template this binary matches).
//!
//! ## What one pass does
//!
//! 1. **Products (4, deterministic ids).** `cs_plan_pro`, `cs_seat_standard`,
//!    `cs_seat_premium`, `cs_pack_credits`. Existence is an exact
//!    `GET /v1/products/{id}` — Stripe permits a caller-supplied id, so no
//!    search API is needed.
//! 2. **Prices (6, resolved by `lookup_key`).** Verify-don't-mutate: a found
//!    price whose amount/currency/interval disagrees with the registered
//!    numbers below FAILS LOUDLY with an actionable message — Stripe prices
//!    are immutable, so this can only mean either a stale catalog here or a
//!    price that predates this script, and adopting/duplicating it silently
//!    would be worse than either.
//! 3. **Webhook endpoint.** Matched by exact URL. Present ⇒ reconcile
//!    `enabled_events` in place (Stripe endpoints ARE mutable, unlike
//!    prices) — the signing secret is NOT re-printed (Stripe returns
//!    `whsec_…` only on create). Absent ⇒ create, then print the secret to
//!    **stdout, once** — never to a file, never through `tracing`.
//! 3b. **Billing Portal configuration** (settings launch PR 3.3). Found by
//!    its `metadata[cs_portal_config]=v1` marker and converged in place
//!    (`stripe_client::portal_configuration_params`): payment-method update,
//!    invoice history and cancel-at-period-end ON; seat/plan changes and
//!    customer edits OFF. Created when absent. `POST /v1/billing/portal`
//!    finds it by the same marker, so no id is stored; it refuses (503) if
//!    this step has never run. Re-running is the way to change the portal.
//! 4. **DB writes, last, one transaction.** All six `stripe_price_id` writes
//!    happen only after every Stripe object above is resolved cleanly. Each
//!    write asserts exactly one row affected; a CONFLICTING existing value
//!    aborts the whole transaction naming both ids, unless `--rewrite-price-ids`
//!    is given (the test→live mode-flip escape hatch).
//!
//! The deactivated `team_standard`/`team_premium` plan rows (migration
//! `0167`, `is_active = false`) get NO Stripe objects — this binary's
//! catalog below never references either code, by construction.
//!
//! ## FOUNDER-CONFIRM items (see this PR's report)
//!
//! * **API version pin** ([`STRIPE_API_VERSION`]) — `2026-07-29.dahlia`, the
//!   version `routes::stripe_webhook`'s verified fixtures
//!   (`testdata/stripe_invoice_paid_2026-07-29.dahlia.json`) are in and the
//!   Stripe account's default. It is the ONLY place the backend pins a Stripe
//!   API version (API calls carry no `Stripe-Version` header, so they run at
//!   the account default; only the webhook endpoint is pinned, here). Bump
//!   it together with those fixtures.
//! * `tax_behavior=exclusive` on every price (Q3) — a one-way door per
//!   price (Stripe prices are immutable).
//!
//! ## Dry-run by default
//!
//! No flags = dry run. Every Stripe READ still happens (a rehearsal that
//! skips the lookups rehearses nothing) — only the Stripe WRITES (create
//! product/price/webhook) and the DB transaction are skipped.
//!
//! ## Usage
//!
//! ```text
//! provision_stripe [--execute] [--rewrite-price-ids] [--live] [--accept-api-version-mismatch]
//!                  [--webhook-url URL]
//! ```
//!
//! `--webhook-url URL` is the endpoint registered with Stripe. Without it the
//! URL is `$API_BASE_URL/v1/stripe/webhook` when `API_BASE_URL` is set (so a
//! staging run is `API_BASE_URL=https://coralswarm-staging.example`), and
//! `https://api.coralswarm.example/v1/stripe/webhook` (production) otherwise.
//!
//! `--live` is required whenever `STRIPE_SECRET_KEY` is a LIVE key
//! (`sk_live_…`) — refused otherwise, so a mistyped/forgotten mode switch
//! cannot silently provision against the real account. A RESTRICTED key
//! (`rk_test_…`/`rk_live_…`) is refused OUTRIGHT regardless of `--live`, and
//! so is anything that isn't a recognized `sk_`/`rk_` key with material
//! after the prefix (empty, a bare prefix, a `pk_…`/`whsec_…` value,
//! arbitrary text) — see [`classify_secret_key`]. Both refusals happen
//! before any DB connection or Stripe HTTP call.
//!
//! `--accept-api-version-mismatch` is required to reconcile a pre-existing
//! webhook endpoint whose `api_version` is a DIFFERENT pinned version than
//! [`STRIPE_API_VERSION`] — see `ensure_webhook`'s doc comment for why this is
//! never auto-patched. An endpoint with no pinned version (`api_version` null:
//! it follows the account default) reconciles without the flag, because the
//! account default is the pin; see [`check_webhook_api_version`].
//!
//! ## Env
//!
//!   STRIPE_SECRET_KEY                    required. `sk_test_…`/`sk_live_…`
//!       (unrestricted only — see `--live` above).
//!   MIGRATOR_DATABASE_URL / DATABASE_URL  control-plane DSN (same
//!       precedence as `backfill_billing_enforcement`, via
//!       [`coralswarm_backend::migrator_database_url`]).
//!
//! ## Exit codes
//!
//!   0  clean — every product/price resolved (or would resolve, in dry
//!      run), webhook reconciled/created, DB writes committed.
//!   1  a Stripe API error, a price amount mismatch, a webhook api_version
//!      mismatch (without `--accept-api-version-mismatch`), or a DB error.
//!   2  usage error, missing env, a restricted key, or a live key without
//!      `--live`.
//!
//! `println!`/`eprintln!` only — this bin does not init `tracing` (same
//! convention as `backfill_billing_enforcement`).

use std::process::ExitCode;

use anyhow::Context;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use coralswarm_backend::billing::stripe_client::{stripe_http_client, Idem, StripeClient};
use coralswarm_backend::migrator_database_url;

const USAGE: &str = "usage: provision_stripe [--execute] [--rewrite-price-ids] [--live] \
[--accept-api-version-mismatch] [--webhook-url URL]   (env: API_BASE_URL)";

/// The one Stripe API version the backend pins; see this module's doc comment.
const STRIPE_API_VERSION: &str = "2026-07-29.dahlia";

const DEFAULT_API_BASE_URL: &str = "https://api.coralswarm.example";
const WEBHOOK_PATH: &str = "/v1/stripe/webhook";

/// The webhook URL to register: `--webhook-url`, else `$API_BASE_URL` +
/// [`WEBHOOK_PATH`], else production. Pure so it is tested without a process
/// environment. Must be an http(s) URL; a base URL's trailing slash is dropped.
fn resolve_webhook_url(flag: Option<&str>, env_base: Option<&str>) -> Result<String, String> {
    let url = match (flag.map(str::trim).filter(|v| !v.is_empty()), env_base.map(str::trim).filter(|v| !v.is_empty())) {
        (Some(url), _) => url.to_string(),
        (None, Some(base)) => format!("{}{WEBHOOK_PATH}", base.trim_end_matches('/')),
        (None, None) => format!("{DEFAULT_API_BASE_URL}{WEBHOOK_PATH}"),
    };
    if !(url.starts_with("https://") || url.starts_with("http://")) || url.contains(char::is_whitespace) {
        return Err(format!("webhook URL must be an http(s) URL without spaces, got {url:?}"));
    }
    Ok(url)
}
/// The three events `routes::stripe_webhook` acts on today, plus what PR D
/// (checkout) and PR G (refill) will need — subscribing ahead of the
/// handler is safe: unknown event types are accepted and answered 200
/// (`routes::stripe_webhook`'s own module doc).
const WEBHOOK_EVENTS: &[&str] = &[
    "invoice.paid",
    "customer.subscription.updated",
    "customer.subscription.deleted",
    "checkout.session.completed",
    // Acted on only to end a subscription started for an org being deleted.
    "customer.subscription.created",
    "checkout.session.async_payment_succeeded",
    "checkout.session.async_payment_failed",
    // Clears the account's open Checkout so the next one replaces it cleanly.
    "checkout.session.expired",
    "invoice.payment_failed",
    "payment_intent.succeeded",
    "payment_intent.payment_failed",
    // Alert-only (refunds and disputes are reconciled by hand).
    "charge.refunded",
    "charge.dispute.created",
    // Settings 3.5 (auto-reload): a 3DS-required card and an operator cancel.
    "payment_intent.requires_action",
    "payment_intent.canceled",
];

// ── Catalog (plan §1.3/§1.10, cross-checked against migrations 0155/0165/
// 0166/0167 in this worktree) ────────────────────────────────────────────

struct ProductSpec {
    id: &'static str,
    name: &'static str,
    coralswarm_code: &'static str,
}

const PRODUCTS: &[ProductSpec] = &[
    ProductSpec {
        id: "cs_plan_pro",
        name: "Coralswarm Pro",
        coralswarm_code: "pro",
    },
    ProductSpec {
        id: "cs_seat_standard",
        // Renamed in the live account on 2026-10-28. `ensure_product` only
        // creates a missing product and never updates an existing one, so a
        // re-run cannot revert the live name; this keeps a FRESH environment
        // (a new sandbox, a dev account) in step with live.
        name: "CoralSwarm Team Account",
        coralswarm_code: "seat_standard",
    },
    ProductSpec {
        id: "cs_seat_premium",
        name: "Coralswarm Team — Premium seat",
        coralswarm_code: "seat_premium",
    },
    ProductSpec {
        id: "cs_pack_credits",
        name: "Coralswarm credit pack",
        coralswarm_code: "pack",
    },
];

struct PriceSpec {
    lookup_key: &'static str,
    product_id: &'static str,
    unit_amount_cents: i64,
    /// `Some("month")` for a recurring (seat/plan) price, `None` for a
    /// one-time (pack) price. Seat/plan prices are `usage_type=licensed`,
    /// never `metered` — quantity is the subscription-item seat count.
    recurring_interval: Option<&'static str>,
    credits: i64,
    db_table: &'static str,
    db_code: &'static str,
}

const PRICES: &[PriceSpec] = &[
    PriceSpec {
        lookup_key: "cs_pro_v1",
        product_id: "cs_plan_pro",
        unit_amount_cents: 2000,
        recurring_interval: Some("month"),
        credits: 1500,
        db_table: "plans",
        db_code: "pro",
    },
    PriceSpec {
        lookup_key: "cs_seat_standard_v1",
        product_id: "cs_seat_standard",
        unit_amount_cents: 4000,
        recurring_interval: Some("month"),
        credits: 3000,
        db_table: "seat_tiers",
        db_code: "standard",
    },
    PriceSpec {
        lookup_key: "cs_seat_premium_v1",
        product_id: "cs_seat_premium",
        unit_amount_cents: 17500,
        recurring_interval: Some("month"),
        credits: 15000,
        db_table: "seat_tiers",
        db_code: "premium",
    },
    PriceSpec {
        lookup_key: "cs_pack_500_v1",
        product_id: "cs_pack_credits",
        unit_amount_cents: 1100,
        recurring_interval: None,
        credits: 500,
        db_table: "credit_packs",
        db_code: "pack_500",
    },
    PriceSpec {
        lookup_key: "cs_pack_2000_v1",
        product_id: "cs_pack_credits",
        unit_amount_cents: 4400,
        recurring_interval: None,
        credits: 2000,
        db_table: "credit_packs",
        db_code: "pack_2000",
    },
    PriceSpec {
        lookup_key: "cs_pack_5000_v1",
        product_id: "cs_pack_credits",
        unit_amount_cents: 11000,
        recurring_interval: None,
        credits: 5000,
        db_table: "credit_packs",
        db_code: "pack_5000",
    },
];

// ── Args ──────────────────────────────────────────────────────────────────

#[derive(Debug, Default, PartialEq, Eq)]
struct Args {
    execute: bool,
    rewrite_price_ids: bool,
    live: bool,
    accept_api_version_mismatch: bool,
    webhook_url: Option<String>,
}

/// What `parse_args` decided argv means. A distinct `Help` variant (rather
/// than `parse_args` itself printing `USAGE` and calling
/// `std::process::exit(0)`) keeps the exit call — and the process-wide
/// side effect it implies — out of the unit-tested parser entirely; `main`
/// is the only place that prints and exits, exactly like the `Err` path
/// just below it.
#[derive(Debug, PartialEq, Eq)]
enum ParseOutcome {
    Run(Args),
    Help,
}

/// Takes `impl Iterator<Item = String>` so the test below drives the real
/// parser against a synthetic argv — `backfill_billing_enforcement::parse_args`'s
/// own precedent for why (finding 10, that binary's review round 2).
fn parse_args(args: impl Iterator<Item = String>) -> Result<ParseOutcome, String> {
    let mut out = Args::default();
    let mut args = args;
    while let Some(arg) = args.next() {
        if let Some(v) = arg.strip_prefix("--webhook-url=") {
            out.webhook_url = Some(v.to_string());
            continue;
        }
        match arg.as_str() {
            "--webhook-url" => {
                out.webhook_url = Some(args.next().ok_or_else(|| "--webhook-url needs a value".to_string())?);
            }
            "--execute" => out.execute = true,
            "--rewrite-price-ids" => out.rewrite_price_ids = true,
            "--live" => out.live = true,
            "--accept-api-version-mismatch" => out.accept_api_version_mismatch = true,
            "-h" | "--help" => return Ok(ParseOutcome::Help),
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(ParseOutcome::Run(out))
}

// ── Key-mode classification (pure fn, tested without HTTP) ─────────────────

/// How `STRIPE_SECRET_KEY` classifies for the two boot-time gates below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyModeCheck {
    /// An unrestricted key (`sk_test_…`/`sk_live_…`) with key material after
    /// the prefix. `live` is whether it is the live-mode variant.
    Ok { live: bool },
    /// A restricted key (`rk_test_…`/`rk_live_…`) with key material after
    /// the prefix — refused outright, regardless of `--live`. See the
    /// refusal message at the call site for WHY: provisioning creates
    /// Products, Prices, AND webhook endpoints in one pass, and a
    /// restricted key's scopes are a hand-picked subset that may not cover
    /// all three — a partial run that fails halfway through on a missing
    /// scope is worse than refusing up front. This also closes the
    /// live-mode gate hole a restricted key would otherwise open: an
    /// `rk_live_…` key was previously invisible to a check that only
    /// matched `sk_live_`.
    Restricted,
    /// Anything else: empty, a bare prefix with nothing after it (e.g.
    /// exactly `sk_live_`), a publishable key (`pk_live_…`), a webhook
    /// secret (`whsec_…`), or arbitrary text. Matches the ALLOW-list
    /// `is_valid_stripe_secret_key` uses for the same env var read at
    /// server boot (`coralswarm_backend::lib`) — refused outright, BEFORE
    /// any DB connection or Stripe HTTP call (see `main`'s call site, which
    /// classifies the key first).
    Invalid,
}

fn classify_secret_key(key: &str) -> KeyModeCheck {
    const RESTRICTED_PREFIXES: [&str; 2] = ["rk_test_", "rk_live_"];
    const UNRESTRICTED_PREFIXES: [(&str, bool); 2] = [("sk_test_", false), ("sk_live_", true)];

    if let Some(prefix) = RESTRICTED_PREFIXES.iter().find(|p| key.starts_with(**p)) {
        return if key.len() > prefix.len() {
            KeyModeCheck::Restricted
        } else {
            KeyModeCheck::Invalid
        };
    }
    if let Some((prefix, live)) = UNRESTRICTED_PREFIXES.iter().find(|(p, _)| key.starts_with(p)) {
        return if key.len() > prefix.len() {
            KeyModeCheck::Ok { live: *live }
        } else {
            KeyModeCheck::Invalid
        };
    }
    KeyModeCheck::Invalid
}

// ── Idempotency-key payload digest ──────────────────────────────────────

/// Full-width (32 hex char, 128-bit) digest of a canonicalized request
/// payload, appended to this binary's deterministic idempotency keys
/// (`prov:product:{code}:{env}:{digest}`, `prov:price:{code}:{env}:
/// {digest}`, etc).
///
/// WHY: a key like `prov:product:pro:test` alone is stable across re-runs
/// even when the BODY it keys (the product name, a price's metadata) is
/// edited in this file — Stripe's `Idempotency-Key` contract only allows
/// reusing a key with an IDENTICAL body. Re-running this binary after such
/// an edit, within Stripe's ~24h idempotency window, would otherwise either
/// 409 with a confusing `idempotency_error` (a materially different body
/// under the same key) or silently replay the STALE cached response (an
/// unchanged body that happens to still collide) — neither is "apply my
/// edit." Folding a digest of the actual body into the key means an edited
/// body automatically gets a fresh key, while a genuine retry (identical
/// body) still gets the identical key Stripe expects for real replay
/// safety. Sorted by field name so incidental construction-order
/// differences never change the digest for the same logical body.
///
/// 128 bits (32 hex chars) rather than a truncated 32-bit (8 hex char)
/// prefix: a birthday collision within Stripe's ~24h idempotency window —
/// two DIFFERENT bodies hashing to the same short digest — would produce
/// the exact same confusing `idempotency_error` this digest exists to
/// avoid. At 128 bits that collision is cryptographically negligible while
/// still leaving every constructed key comfortably under Stripe's 255-char
/// `Idempotency-Key` limit (see `idempotency_keys_stay_well_under_stripes_255_char_limit`).
fn payload_digest(pairs: &[(&str, &str)]) -> String {
    use sha2::{Digest, Sha256};
    let mut sorted: Vec<&(&str, &str)> = pairs.iter().collect();
    sorted.sort_by_key(|(k, _)| *k);
    let mut canonical = String::new();
    for (k, v) in sorted {
        canonical.push_str(k);
        canonical.push('=');
        canonical.push_str(v);
        canonical.push('\n');
    }
    let digest = Sha256::digest(canonical.as_bytes());
    hex::encode(&digest[..16])
}

// ── Pure verification logic (Layer-1-style pure fn, tested without HTTP) ──

/// Whether a price object Stripe returned for `spec.lookup_key` actually
/// matches the registered numbers. `None` inside `existing` for a field
/// Stripe genuinely omits (`recurring` is absent/null on a one-time price)
/// is handled the same way `spec.recurring_interval == None` reads it —
/// both mean "no recurring interval", not a mismatch.
///
/// Also requires `product`, `tax_behavior`, and (for recurring prices)
/// `recurring.usage_type` to match what this catalog always sends on
/// create (`ensure_price`'s `create_price` call) — a price that drifted on
/// any of these is just as incompatible as one with the wrong amount, and
/// must fall into the mismatch (verify-don't-mutate refusal) branch rather
/// than being silently `Reused`.
fn price_matches(existing: &serde_json::Value, spec: &PriceSpec) -> bool {
    let unit_amount = existing.get("unit_amount").and_then(|v| v.as_i64());
    let currency = existing.get("currency").and_then(|v| v.as_str());
    let interval = existing
        .get("recurring")
        .and_then(|r| r.get("interval"))
        .and_then(|v| v.as_str());
    let product = existing.get("product").and_then(|v| v.as_str());
    let tax_behavior = existing.get("tax_behavior").and_then(|v| v.as_str());
    let usage_type_ok = match spec.recurring_interval {
        Some(_) => {
            existing
                .get("recurring")
                .and_then(|r| r.get("usage_type"))
                .and_then(|v| v.as_str())
                == Some("licensed")
        }
        None => true,
    };
    unit_amount == Some(spec.unit_amount_cents)
        && currency == Some("usd")
        && interval == spec.recurring_interval
        && product == Some(spec.product_id)
        && tax_behavior == Some("exclusive")
        && usage_type_ok
}

// ── Orchestration decisions (pure, tested without HTTP or a DB) ─────────

/// Whether the price/webhook/DB phases should run at all, given the error
/// count accumulated by the products phase. Pure so the gating decision
/// itself is unit-testable: these phases perform Stripe WRITES in
/// `--execute` mode, and continuing past a product-phase failure
/// contradicted this module's own "resolved cleanly" promise (finding #9,
/// this PR's review).
fn should_run_downstream_phases(product_phase_errors: u32) -> bool {
    product_phase_errors == 0
}

/// The dry-run "N row(s) would be written" preview count. `resolved_count`
/// is prices that already exist in Stripe (`PriceOutcome::Reused`, which
/// carries a real id to write); `would_create_count` is prices that don't
/// exist yet (`PriceOutcome::WouldCreate`, which has no id yet — nothing to
/// push into `resolved`, but --execute would still create AND write one).
/// Reporting `resolved_count` alone (the bug this fixes, finding #8) means
/// a first-ever dry run — where NOTHING exists yet, so every price is
/// `WouldCreate` — always previewed "0 row(s)" no matter how many
/// `--execute` was about to write.
fn dry_run_write_count(resolved_count: usize, would_create_count: u32) -> usize {
    resolved_count + would_create_count as usize
}

// ── Stripe resolution ───────────────────────────────────────────────────

enum ProductOutcome {
    Existed,
    Created,
    WouldCreate,
}

async fn ensure_product(
    client: &StripeClient,
    spec: &ProductSpec,
    execute: bool,
) -> anyhow::Result<ProductOutcome> {
    match client.retrieve_product(spec.id).await {
        Ok(_) => Ok(ProductOutcome::Existed),
        Err(e) if e.is_not_found() => {
            if !execute {
                return Ok(ProductOutcome::WouldCreate);
            }
            let env = if client.livemode() { "live" } else { "test" };
            let metadata = [("coralswarm_code", spec.coralswarm_code), ("env", env)];
            // Digest of the actual body (id/name/metadata) — see
            // `payload_digest`'s doc comment for WHY a catalog edit must
            // not collide with a stale key from before the edit.
            let digest = payload_digest(&[
                ("id", spec.id),
                ("name", spec.name),
                ("coralswarm_code", spec.coralswarm_code),
                ("env", env),
            ]);
            let idem_key = format!("prov:product:{}:{env}:{digest}", spec.coralswarm_code);
            client
                .create_product(spec.id, spec.name, &metadata, Idem::Key(&idem_key))
                .await
                .with_context(|| format!("product {}: create failed", spec.id))?;
            Ok(ProductOutcome::Created)
        }
        Err(e) => Err(e).with_context(|| format!("product {}: retrieve failed", spec.id)),
    }
}

enum PriceOutcome {
    Reused,
    Created,
    WouldCreate,
}

async fn ensure_price(
    client: &StripeClient,
    spec: &PriceSpec,
    execute: bool,
) -> anyhow::Result<(String, PriceOutcome)> {
    match client.find_price_by_lookup_key(spec.lookup_key).await {
        Ok(Some(existing)) => {
            if !price_matches(&existing, spec) {
                let unit_amount = existing.get("unit_amount").and_then(|v| v.as_i64());
                let currency = existing.get("currency").and_then(|v| v.as_str());
                let interval = existing
                    .get("recurring")
                    .and_then(|r| r.get("interval"))
                    .and_then(|v| v.as_str());
                anyhow::bail!(
                    "price {:?} is {}¢/{}/{} in Stripe, the catalog says {}¢/usd/{}. Stripe \
                     prices are immutable; bump the lookup key to a new version (e.g. `_v2`) and \
                     re-run.",
                    spec.lookup_key,
                    unit_amount.map(|a| a.to_string()).unwrap_or_else(|| "?".into()),
                    currency.unwrap_or("?"),
                    interval.unwrap_or("one-time"),
                    spec.unit_amount_cents,
                    spec.recurring_interval.unwrap_or("one-time"),
                );
            }
            let id = existing
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("price {}: found price had no \"id\" field", spec.lookup_key))?
                .to_string();
            Ok((id, PriceOutcome::Reused))
        }
        Ok(None) => {
            if !execute {
                return Ok((String::new(), PriceOutcome::WouldCreate));
            }
            let env = if client.livemode() { "live" } else { "test" };
            let credits_str = spec.credits.to_string();
            let metadata: Vec<(&str, &str)> = vec![
                ("coralswarm_code", spec.db_code),
                ("credits", credits_str.as_str()),
                ("env", env),
            ];
            // Digest of the actual body — see `payload_digest`'s doc
            // comment. A `unit_amount_cents`/interval catalog edit must not
            // collide with (or 409 against) a stale key from before the
            // edit — this cannot happen via price MUTATION (Stripe prices
            // are immutable, enforced above by `price_matches`), only via
            // re-running this binary after editing `PRICES` before the
            // first-ever create for that lookup_key has landed.
            let unit_amount_str = spec.unit_amount_cents.to_string();
            let digest = payload_digest(&[
                ("product", spec.product_id),
                ("unit_amount", unit_amount_str.as_str()),
                ("currency", "usd"),
                ("lookup_key", spec.lookup_key),
                ("tax_behavior", "exclusive"),
                ("recurring_interval", spec.recurring_interval.unwrap_or("")),
                ("coralswarm_code", spec.db_code),
                ("credits", credits_str.as_str()),
                ("env", env),
            ]);
            let idem_key = format!("prov:price:{}:{env}:{digest}", spec.db_code);
            let v = client
                .create_price(
                    spec.product_id,
                    spec.unit_amount_cents,
                    "usd",
                    spec.recurring_interval,
                    spec.lookup_key,
                    // Q3, founder-recommended default: list price is
                    // pre-tax. One-way door — Stripe prices are immutable.
                    "exclusive",
                    &metadata,
                    Idem::Key(&idem_key),
                )
                .await
                .with_context(|| format!("price {}: create failed", spec.lookup_key))?;
            let id = v
                .get("id")
                .and_then(|x| x.as_str())
                .ok_or_else(|| anyhow::anyhow!("price {}: create response had no \"id\" field", spec.lookup_key))?
                .to_string();
            Ok((id, PriceOutcome::Created))
        }
        Err(e) => Err(e).with_context(|| format!("price {}: lookup failed", spec.lookup_key)),
    }
}

enum WebhookOutcome {
    Reconciled { endpoint_id: String },
    Created { endpoint_id: String },
    WouldReconcile { endpoint_id: String },
    WouldCreate,
}

/// Reconciles (or creates) the webhook endpoint. `accept_api_version_mismatch`
/// gates the FOUNDER-CONFIRM check below — see the comment at that check for
/// why a mismatch is never auto-patched.
/// How an existing endpoint's `api_version` relates to [`STRIPE_API_VERSION`].
#[derive(Debug, PartialEq, Eq)]
enum ApiVersionCheck {
    /// Pinned to exactly our version.
    Matches,
    /// `api_version` is null: the endpoint follows the account default, which
    /// is the pin (`2026-07-29.dahlia`). Reconciling `enabled_events` cannot
    /// change the shape of its payloads.
    FollowsAccountDefault,
    /// A different pinned version, explicitly accepted with
    /// `--accept-api-version-mismatch`.
    MismatchAccepted(String),
}

/// Decide whether an existing endpoint may be reconciled. A null version
/// follows the account default; this binary cannot read that default through
/// the API, so it is a documented precondition (Developers → API version shows
/// `2026-07-29.dahlia`), and it is reconcile-only (the version is never
/// written), so a wrong default is surfaced by the dashboard, not changed here.
fn check_webhook_api_version(existing: Option<&str>, accept_mismatch: bool) -> Result<ApiVersionCheck, String> {
    match existing {
        Some(v) if v == STRIPE_API_VERSION => Ok(ApiVersionCheck::Matches),
        None => Ok(ApiVersionCheck::FollowsAccountDefault),
        Some(v) if accept_mismatch => Ok(ApiVersionCheck::MismatchAccepted(v.to_string())),
        Some(v) => Err(format!(
            "FOUNDER-CONFIRM: the webhook endpoint's api_version is {v:?}, this binary is pinned to \
             {STRIPE_API_VERSION:?}. Changing a LIVE endpoint's api_version changes the shape of \
             every event payload `routes::stripe_webhook` parses — this is NEVER auto-patched. \
             Confirm whether the drift is expected, then re-run with --accept-api-version-mismatch \
             to proceed (enabled_events reconciliation only — the version itself is still never \
             written by this binary)."
        )),
    }
}

async fn ensure_webhook(
    client: &StripeClient,
    webhook_url: &str,
    execute: bool,
    accept_api_version_mismatch: bool,
) -> anyhow::Result<WebhookOutcome> {
    let endpoints = client
        .list_webhook_endpoints()
        .await
        .context("webhook: list failed")?;
    let existing = endpoints
        .iter()
        .find(|e| e.get("url").and_then(|u| u.as_str()) == Some(webhook_url));

    match existing {
        Some(ep) => {
            let id = ep
                .get("id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow::anyhow!("webhook: existing endpoint had no \"id\" field"))?
                .to_string();

            // FOUNDER-CONFIRM: reconcile used to touch ONLY `enabled_events`
            // and silently ignore `api_version`, while still reporting
            // "reconciled" — a pre-existing endpoint could drift from
            // STRIPE_API_VERSION with nothing here ever noticing. Checked
            // in BOTH dry-run and --execute (a rehearsal should surface
            // this too, per this module's own "every Stripe READ still
            // happens" doc). Never auto-patched: Stripe's API version pins
            // the SHAPE of every event payload `routes::stripe_webhook`
            // parses, so silently changing it on a live endpoint is a
            // one-way door with no verify-don't-mutate escape hatch —
            // unlike a price mismatch, there is no "bump to `_v2`" move,
            // only a human confirming the drift is intentional.
            let existing_version = ep.get("api_version").and_then(|v| v.as_str());
            match check_webhook_api_version(existing_version, accept_api_version_mismatch)
                .map_err(|why| anyhow::anyhow!("webhook endpoint {id}: {why}"))?
            {
                ApiVersionCheck::Matches => {}
                ApiVersionCheck::FollowsAccountDefault => println!(
                    "webhook: endpoint {id} has no pinned api_version (follows the account default); \
                     the account default must be {STRIPE_API_VERSION} (Stripe dashboard → Developers \
                     → API version). Reconciling enabled_events only."
                ),
                ApiVersionCheck::MismatchAccepted(v) => println!(
                    "webhook: endpoint {id} is pinned to {v}, not {STRIPE_API_VERSION}; \
                     --accept-api-version-mismatch given, reconciling enabled_events only."
                ),
            }

            if !execute {
                return Ok(WebhookOutcome::WouldReconcile { endpoint_id: id });
            }
            // UNKEYED: reconciling `enabled_events` to a fixed list has no
            // cumulative effect no matter how many times it's sent, so
            // there's nothing for an `Idempotency-Key` to protect against —
            // see `Idem::NaturallyIdempotent`'s doc comment.
            client
                .update_webhook_endpoint(&id, WEBHOOK_EVENTS)
                .await
                .with_context(|| format!("webhook {id}: reconcile failed"))?;
            Ok(WebhookOutcome::Reconciled { endpoint_id: id })
        }
        None => {
            if !execute {
                return Ok(WebhookOutcome::WouldCreate);
            }
            let events_joined = WEBHOOK_EVENTS.join(",");
            let digest = payload_digest(&[
                ("url", webhook_url),
                ("events", events_joined.as_str()),
                ("api_version", STRIPE_API_VERSION),
            ]);
            let idem_key = format!("prov:webhook:create:{digest}");
            let v = client
                .create_webhook_endpoint(webhook_url, WEBHOOK_EVENTS, STRIPE_API_VERSION, Idem::Key(&idem_key))
                .await
                .context("webhook: create failed")?;
            let id = v
                .get("id")
                .and_then(|x| x.as_str())
                .ok_or_else(|| anyhow::anyhow!("webhook: create response had no \"id\" field"))?
                .to_string();
            let secret = v
                .get("secret")
                .and_then(|x| x.as_str())
                .ok_or_else(|| anyhow::anyhow!("webhook: create response had no \"secret\" field"))?;
            // Printed to stdout, ONCE, on its own line — Stripe returns
            // whsec_… only on create, never again. Never written to a file
            // by this binary, never emitted through tracing, never included
            // in the summary line below. Deliberately NOT embedded in a
            // ready-to-paste `fly secrets set NAME=VALUE` command: a secret
            // in a command line lands in shell history and is visible to
            // any process inspecting argv (ps, auditd, EDR). The follow-up
            // instructions route it through the untracked `.env` file and
            // `fly secrets import` (stdin) instead, so the value itself
            // never becomes a command argument.
            println!();
            println!("Created webhook endpoint {id} at {webhook_url}.");
            println!("Signing secret — COPY THIS NOW, Stripe will not show it again:");
            println!();
            println!("  {secret}");
            println!();
            println!("Do NOT paste this into a `fly secrets set NAME=VALUE` command — it would");
            println!("land in your shell history and be readable by any process inspecting argv.");
            println!("Instead, add this line to the untracked .env:");
            println!();
            println!("  STRIPE_WEBHOOK_SECRET=<paste the signing secret shown above>");
            println!();
            println!("...then deploy it from the file, so the secret never appears as a command");
            println!("argument:");
            println!();
            println!("  grep '^STRIPE_WEBHOOK_SECRET=' .env | fly secrets import --app coralswarm-backend");
            println!();
            Ok(WebhookOutcome::Created { endpoint_id: id })
        }
    }
}

/// Finds the Billing Portal configuration this system owns (by its metadata
/// marker, see `stripe_client::is_owned_portal_configuration`) and converges
/// it to `portal_configuration_params()`: payment-method update, invoice
/// history and cancel at period end on; seat/plan changes off. Absent, it is
/// created. `POST /v1/billing/portal` finds it at request time by the same
/// marker, so no id is stored anywhere.
async fn ensure_portal_configuration(client: &StripeClient, execute: bool) -> anyhow::Result<String> {
    let existing = client
        .find_portal_configuration()
        .await
        .context("portal: list failed")?;
    match (existing, execute) {
        (Some(id), true) => {
            client
                .update_portal_configuration(&id)
                .await
                .with_context(|| format!("portal {id}: reconcile failed"))?;
            Ok(format!("configuration {id} reconciled"))
        }
        (Some(id), false) => Ok(format!("would reconcile configuration {id} (dry run)")),
        (None, true) => {
            let id = client
                .create_portal_configuration()
                .await
                .context("portal: create failed")?;
            Ok(format!("configuration {id} created"))
        }
        (None, false) => Ok("would create the configuration (dry run)".to_string()),
    }
}

// ── DB write phase ───────────────────────────────────────────────────────

/// Writes `stripe_price_id` for one catalog row and asserts exactly one row
/// was affected. `table` is always one of the three literal, hardcoded
/// catalog table names in [`PRICES`] — never derived from external input —
/// so interpolating it into the SQL text is not an injection surface.
async fn write_price_id(
    tx: &mut sqlx::PgConnection,
    table: &'static str,
    code: &str,
    price_id: &str,
    rewrite: bool,
) -> anyhow::Result<()> {
    if !matches!(table, "plans" | "seat_tiers" | "credit_packs") {
        anyhow::bail!("write_price_id: unknown table {table:?}");
    }
    let sql = if rewrite {
        format!("UPDATE {table} SET stripe_price_id = $1 WHERE code = $2")
    } else {
        format!(
            "UPDATE {table} SET stripe_price_id = $1 WHERE code = $2 \
             AND (stripe_price_id IS NULL OR stripe_price_id = $1)"
        )
    };
    let res = sqlx::query(&sql)
        .bind(price_id)
        .bind(code)
        .execute(&mut *tx)
        .await
        .with_context(|| format!("{table}.{code}: UPDATE failed"))?;
    if res.rows_affected() == 1 {
        return Ok(());
    }

    let current: Option<String> = sqlx::query_scalar(&format!(
        "SELECT stripe_price_id FROM {table} WHERE code = $1"
    ))
    .bind(code)
    .fetch_optional(&mut *tx)
    .await
    .with_context(|| format!("{table}.{code}: post-failure lookup failed"))?
    .flatten();

    match current {
        None => Err(anyhow::anyhow!(
            "{table}.{code}: no such row (catalog code not seeded — check migrations 0165-0167)"
        )),
        Some(existing) => Err(anyhow::anyhow!(
            "{table}.{code}: refusing to overwrite existing stripe_price_id {existing:?} with \
             {price_id:?} — you are changing which Stripe account this database bills against. \
             Re-run with --rewrite-price-ids if this is a deliberate test\u{2192}live mode flip."
        )),
    }
}

// ── main ──────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(ParseOutcome::Run(a)) => a,
        Ok(ParseOutcome::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("{e}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    let webhook_url = match resolve_webhook_url(
        args.webhook_url.as_deref(),
        std::env::var("API_BASE_URL").ok().as_deref(),
    ) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::from(2);
        }
    };

    let secret_key = match std::env::var("STRIPE_SECRET_KEY") {
        Ok(k) if !k.trim().is_empty() => k.trim().to_string(),
        _ => {
            eprintln!("STRIPE_SECRET_KEY must be set");
            return ExitCode::from(2);
        }
    };
    match classify_secret_key(&secret_key) {
        KeyModeCheck::Invalid => {
            eprintln!(
                "STRIPE_SECRET_KEY is not a recognized Stripe secret key format. Expected \
                 sk_test_…/sk_live_… with key material after the prefix — got something else \
                 (empty, a bare prefix with nothing after it, a publishable pk_… key, a \
                 whsec_… webhook secret, or arbitrary text). Refusing rather than guessing."
            );
            return ExitCode::from(2);
        }
        KeyModeCheck::Restricted => {
            eprintln!(
                "STRIPE_SECRET_KEY is a RESTRICTED key (rk_test_…/rk_live_…). Provisioning \
                 creates Products, Prices, AND a webhook endpoint in one pass — scopes an \
                 unrestricted secret key always has and a restricted key may not. Refusing \
                 outright rather than risk a partial run that fails halfway through on a \
                 missing scope; use an sk_test_…/sk_live_… key instead."
            );
            return ExitCode::from(2);
        }
        KeyModeCheck::Ok { live } if live && !args.live => {
            eprintln!(
                "STRIPE_SECRET_KEY is a LIVE key (sk_live_…) but --live was not given. Refusing to \
                 provision against the real Stripe account without an explicit --live flag."
            );
            return ExitCode::from(2);
        }
        KeyModeCheck::Ok { .. } => {}
    }

    let dsn = match migrator_database_url() {
        Some(u) => u,
        None => {
            eprintln!("MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set");
            return ExitCode::from(2);
        }
    };
    let pool: PgPool = match PgPoolOptions::new().max_connections(2).connect(&dsn).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to connect: {e}");
            return ExitCode::FAILURE;
        }
    };

    let client = StripeClient::new(stripe_http_client(), secret_key);
    let livemode = client.livemode();

    println!(
        "provision_stripe — mode={} livemode={}",
        if args.execute { "EXECUTE" } else { "DRY RUN" },
        livemode
    );
    if args.rewrite_price_ids {
        println!("  --rewrite-price-ids: a conflicting existing stripe_price_id WILL be overwritten");
    }
    println!(
        "  skipping deactivated plans team_standard/team_premium (migration 0167, is_active=false) \
         — no Stripe objects created for either"
    );

    let mut errors: u32 = 0;

    // ── 1. Products ──────────────────────────────────────────────────────
    let mut products_existed = 0u32;
    let mut products_created = 0u32;
    for spec in PRODUCTS {
        match ensure_product(&client, spec, args.execute).await {
            Ok(ProductOutcome::Existed) => {
                products_existed += 1;
                println!("product {}: exists", spec.id);
            }
            Ok(ProductOutcome::Created) => {
                products_created += 1;
                println!("product {}: created", spec.id);
            }
            Ok(ProductOutcome::WouldCreate) => {
                println!("product {}: would create (dry run)", spec.id);
            }
            Err(e) => {
                eprintln!("ERROR product {}: {e:#}", spec.id);
                errors += 1;
            }
        }
    }

    // ── 2. Prices, 3. Webhook, 4. DB — gated on a clean products phase. ────
    // WHY gate here, not just at the DB write (the module doc's original
    // promise): prices and the webhook endpoint are still Stripe WRITES in
    // --execute mode, and continuing to make them after a product failure
    // contradicts "resolved cleanly" — a caller investigating a product
    // error has no reason to also expect price/webhook state to have
    // changed underneath them. The products loop above still visits EVERY
    // product before this gate, so one run reports every product-phase
    // failure at once rather than stopping at the first.
    let mut prices_reused = 0u32;
    let mut prices_created = 0u32;
    let mut prices_would_create = 0u32;
    let mut resolved: Vec<(&PriceSpec, String)> = Vec::new();
    let mut webhook_status = "skipped";
    let mut db_updated = 0u32;

    if !should_run_downstream_phases(errors) {
        println!("prices/webhook/db: skipped — {errors} product error(s) above; fix and re-run");
    } else {
        // ── 2. Prices ────────────────────────────────────────────────────
        for spec in PRICES {
            match ensure_price(&client, spec, args.execute).await {
                Ok((id, PriceOutcome::Reused)) => {
                    prices_reused += 1;
                    println!("price {}: reused ({id})", spec.lookup_key);
                    resolved.push((spec, id));
                }
                Ok((id, PriceOutcome::Created)) => {
                    prices_created += 1;
                    println!("price {}: created ({id})", spec.lookup_key);
                    resolved.push((spec, id));
                }
                Ok((_, PriceOutcome::WouldCreate)) => {
                    println!("price {}: would create (dry run)", spec.lookup_key);
                    prices_would_create += 1;
                }
                Err(e) => {
                    eprintln!("ERROR price {}: {e:#}", spec.lookup_key);
                    errors += 1;
                }
            }
        }

        // ── 3. Webhook endpoint ──────────────────────────────────────────
        webhook_status = match ensure_webhook(&client, &webhook_url, args.execute, args.accept_api_version_mismatch).await {
            Ok(WebhookOutcome::Reconciled { endpoint_id }) => {
                println!(
                    "webhook: endpoint {endpoint_id} already registered at {webhook_url}, events \
                     reconciled. The signing secret cannot be retrieved via the API after \
                     creation — but it IS visible any time via Reveal on the endpoint's page in \
                     the Stripe dashboard. Keep the existing STRIPE_WEBHOOK_SECRET, or use \
                     Reveal if it's been lost."
                );
                "reconciled"
            }
            Ok(WebhookOutcome::Created { endpoint_id }) => {
                println!("webhook: created endpoint {endpoint_id}");
                "created"
            }
            Ok(WebhookOutcome::WouldReconcile { endpoint_id }) => {
                println!("webhook: would reconcile enabled_events on existing endpoint {endpoint_id} (dry run)");
                "dry-run"
            }
            Ok(WebhookOutcome::WouldCreate) => {
                println!(
                    "webhook: would create a new endpoint at {webhook_url} with {} events (dry run)",
                    WEBHOOK_EVENTS.len()
                );
                "dry-run"
            }
            Err(e) => {
                eprintln!("ERROR webhook: {e:#}");
                errors += 1;
                "error"
            }
        };

        // ── 3b. Billing Portal configuration (settings launch PR 3.3) ────
        match ensure_portal_configuration(&client, args.execute).await {
            Ok(msg) => println!("portal: {msg}"),
            Err(e) => {
                eprintln!("ERROR portal configuration: {e:#}");
                errors += 1;
            }
        }

        // ── 4. DB writes, last, one transaction — only if everything above
        //      was clean AND this is an --execute run. ────────────────────
        if args.execute && errors == 0 {
            match pool.begin().await {
                Ok(mut tx) => {
                    let mut tx_failed = false;
                    for (spec, price_id) in &resolved {
                        if let Err(e) = write_price_id(
                            &mut tx,
                            spec.db_table,
                            spec.db_code,
                            price_id,
                            args.rewrite_price_ids,
                        )
                        .await
                        {
                            eprintln!("ERROR db write: {e:#}");
                            errors += 1;
                            tx_failed = true;
                            break;
                        }
                        db_updated += 1;
                    }
                    if tx_failed {
                        let _ = tx.rollback().await;
                        db_updated = 0;
                    } else if let Err(e) = tx.commit().await {
                        eprintln!("ERROR db commit failed: {e}");
                        errors += 1;
                        db_updated = 0;
                    }
                }
                Err(e) => {
                    eprintln!("ERROR failed to open DB transaction: {e}");
                    errors += 1;
                }
            }
        } else if !args.execute {
            // `resolved.len()` alone undercounts: a lookup_key not yet
            // created in Stripe reports PriceOutcome::WouldCreate and is
            // never pushed into `resolved` (it has no id yet to write) —
            // on a first-ever dry run this printed "0 row(s)" while
            // --execute would go on to write every one of them. Folding in
            // prices_would_create makes the rehearsal preview the REAL
            // write set --execute would produce.
            println!(
                "db: {} row(s) would be written (dry run — nothing written)",
                dry_run_write_count(resolved.len(), prices_would_create)
            );
        } else {
            println!("db: skipped — {errors} error(s) above; fix and re-run before any DB write");
        }
    }

    println!(
        "summary: products_existed={products_existed} products_created={products_created} \
         prices_reused={prices_reused} prices_created={prices_created} db_updated={db_updated} \
         webhook={webhook_status} errors={errors}"
    );
    if !args.execute {
        println!("Nothing was written. Read the report above, then re-run with --execute to apply.");
    }

    if errors > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unwraps a `ParseOutcome::Run` for the tests below that assert on
    /// `Args` fields — a `Help` outcome there is itself a test failure.
    fn expect_run(outcome: ParseOutcome) -> Args {
        match outcome {
            ParseOutcome::Run(a) => a,
            ParseOutcome::Help => panic!("expected ParseOutcome::Run, got Help"),
        }
    }

    #[test]
    fn parses_execute_and_rewrite_and_live_together() {
        let argv = vec![
            "--execute".to_string(),
            "--rewrite-price-ids".to_string(),
            "--live".to_string(),
        ];
        let args = expect_run(parse_args(argv.into_iter()).expect("must parse"));
        assert!(args.execute);
        assert!(args.rewrite_price_ids);
        assert!(args.live);
    }

    #[test]
    fn classify_secret_key_ok_for_unrestricted_test_and_live() {
        assert_eq!(classify_secret_key("sk_test_abc"), KeyModeCheck::Ok { live: false });
        assert_eq!(classify_secret_key("sk_live_abc"), KeyModeCheck::Ok { live: true });
    }

    /// The gate hole this finding closes: a restricted `rk_live_…` key
    /// previously matched neither `sk_live_*` (the old live check) nor any
    /// refusal, so it bypassed both the `--live` requirement and the typed
    /// LIVE confirmation. It must now classify as `Restricted`, not `Ok`.
    #[test]
    fn classify_secret_key_restricted_for_both_rk_test_and_rk_live() {
        assert_eq!(classify_secret_key("rk_test_abc"), KeyModeCheck::Restricted);
        assert_eq!(classify_secret_key("rk_live_abc"), KeyModeCheck::Restricted);
    }

    /// Malformed values (round-3 CodeRabbit): a publishable key, a webhook
    /// secret, and arbitrary text must all classify as `Invalid`, not fall
    /// through to `Ok { live: false }` the way a naive
    /// `starts_with("sk_live_")`-only check would.
    #[test]
    fn classify_secret_key_invalid_for_malformed_values() {
        assert_eq!(classify_secret_key(""), KeyModeCheck::Invalid);
        assert_eq!(classify_secret_key("pk_test_REPLACE_WITH_YOUR_KEY"), KeyModeCheck::Invalid);
        assert_eq!(classify_secret_key("pk_test_REPLACE_WITH_YOUR_KEY"), KeyModeCheck::Invalid);
        assert_eq!(classify_secret_key("whsec_abc"), KeyModeCheck::Invalid);
        assert_eq!(classify_secret_key("not-a-stripe-key"), KeyModeCheck::Invalid);
    }

    /// Bare prefixes with no key material after them — copied verbatim from
    /// a `.env.example`-style placeholder — must classify as `Invalid` for
    /// BOTH the unrestricted and restricted prefix families, matching
    /// `is_valid_stripe_secret_key`'s `key.len() > p.len()` requirement in
    /// lib.rs. `"sk_live_"` in particular must not silently classify as a
    /// live unrestricted key.
    #[test]
    fn classify_secret_key_invalid_for_bare_prefixes() {
        assert_eq!(classify_secret_key("sk_test_"), KeyModeCheck::Invalid);
        assert_eq!(classify_secret_key("sk_live_"), KeyModeCheck::Invalid);
        assert_eq!(classify_secret_key("rk_test_"), KeyModeCheck::Invalid);
        assert_eq!(classify_secret_key("rk_live_"), KeyModeCheck::Invalid);
    }

    #[test]
    fn defaults_are_all_false_dry_run() {
        let args = expect_run(parse_args(std::iter::empty()).expect("must parse"));
        assert!(!args.execute);
        assert!(!args.rewrite_price_ids);
        assert!(!args.live);
        assert!(!args.accept_api_version_mismatch);
    }

    #[test]
    fn the_api_version_pin_is_the_dahlia_the_webhook_fixtures_use() {
        assert_eq!(STRIPE_API_VERSION, "2026-07-29.dahlia");
    }

    #[test]
    fn webhook_url_defaults_to_production_and_follows_the_flag_then_the_env() {
        let prod = "https://api.coralswarm.example/v1/stripe/webhook";
        assert_eq!(resolve_webhook_url(None, None).unwrap(), prod);
        assert_eq!(resolve_webhook_url(Some(""), Some("  ")).unwrap(), prod, "blank is unset");
        assert_eq!(
            resolve_webhook_url(None, Some("https://coralswarm-staging.example/")).unwrap(),
            "https://coralswarm-staging.example/v1/stripe/webhook",
            "API_BASE_URL derives the path and drops a trailing slash"
        );
        assert_eq!(
            resolve_webhook_url(Some("https://x.example/hook"), Some("https://ignored.example")).unwrap(),
            "https://x.example/hook",
            "the flag wins over the env"
        );
        assert!(resolve_webhook_url(Some("ftp://x"), None).is_err());
        assert!(resolve_webhook_url(None, Some("not a url")).is_err());
    }

    #[test]
    fn parses_webhook_url_flag_both_spellings_and_requires_a_value() {
        let a = expect_run(parse_args(["--webhook-url", "https://s.example/h"].map(String::from).into_iter()).unwrap());
        assert_eq!(a.webhook_url.as_deref(), Some("https://s.example/h"));
        let a = expect_run(parse_args(["--webhook-url=https://s.example/h", "--execute"].map(String::from).into_iter()).unwrap());
        assert_eq!(a.webhook_url.as_deref(), Some("https://s.example/h"));
        assert!(a.execute);
        assert!(parse_args(["--webhook-url"].map(String::from).into_iter()).is_err());
        assert_eq!(expect_run(parse_args(std::iter::empty()).unwrap()).webhook_url, None);
    }

    #[test]
    fn webhook_api_version_check_pinned_null_and_drifted() {
        assert_eq!(check_webhook_api_version(Some("2026-07-29.dahlia"), false), Ok(ApiVersionCheck::Matches));
        assert_eq!(
            check_webhook_api_version(None, false),
            Ok(ApiVersionCheck::FollowsAccountDefault),
            "a null api_version follows the account default (the pin): no flag needed"
        );
        let err = check_webhook_api_version(Some("2025-03-31.basil"), false).unwrap_err();
        assert!(err.contains("2025-03-31.basil") && err.contains("--accept-api-version-mismatch"), "{err}");
        assert_eq!(
            check_webhook_api_version(Some("2025-03-31.basil"), true),
            Ok(ApiVersionCheck::MismatchAccepted("2025-03-31.basil".into()))
        );
    }

    #[test]
    fn parses_accept_api_version_mismatch() {
        let argv = vec!["--accept-api-version-mismatch".to_string()];
        let args = expect_run(parse_args(argv.into_iter()).expect("must parse"));
        assert!(args.accept_api_version_mismatch);
    }

    /// The finding this test exists for: `-h`/`--help` used to call
    /// `std::process::exit(0)` INSIDE `parse_args`, which is untestable (a
    /// unit test can't observe a process exit) and made the parser unsafe
    /// to call from anywhere but `main`. It must now return
    /// `ParseOutcome::Help` like any other parse result.
    #[test]
    fn dash_h_and_dash_dash_help_both_return_help_outcome() {
        assert_eq!(
            parse_args(vec!["-h".to_string()].into_iter()).expect("must parse"),
            ParseOutcome::Help
        );
        assert_eq!(
            parse_args(vec!["--help".to_string()].into_iter()).expect("must parse"),
            ParseOutcome::Help
        );
    }

    /// `-h`/`--help` short-circuits immediately — a flag before it must not
    /// change the outcome, and nothing after it is even inspected (`Help`
    /// carries no `Args`, so there is nothing for a later flag to set).
    #[test]
    fn help_short_circuits_even_with_a_preceding_flag() {
        let argv = vec!["--execute".to_string(), "--help".to_string()];
        assert_eq!(parse_args(argv.into_iter()).expect("must parse"), ParseOutcome::Help);
    }

    // ── payload_digest ───────────────────────────────────────────────────

    #[test]
    fn payload_digest_is_deterministic_and_order_independent() {
        let a = payload_digest(&[("b", "2"), ("a", "1")]);
        let b = payload_digest(&[("a", "1"), ("b", "2")]);
        assert_eq!(a, b, "canonicalization must sort by key, so construction order can't matter");
        assert_eq!(a.len(), 32, "digest must be exactly 32 hex chars (128 bits)");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()), "digest must be lowercase hex");
    }

    /// THE property this finding exists for: an edited body must produce a
    /// DIFFERENT digest, so `prov:product:{code}:{env}:{digest}` gets a
    /// fresh key automatically on a catalog edit instead of colliding with
    /// a stale key from before the edit.
    #[test]
    fn payload_digest_changes_when_the_body_changes() {
        let a = payload_digest(&[("name", "Coralswarm Pro")]);
        let b = payload_digest(&[("name", "Coralswarm Pro (renamed)")]);
        assert_ne!(a, b);
    }

    #[test]
    fn payload_digest_is_stable_for_an_unchanged_body() {
        let a = payload_digest(&[("id", "cs_plan_pro"), ("name", "Coralswarm Pro"), ("env", "test")]);
        let b = payload_digest(&[("id", "cs_plan_pro"), ("name", "Coralswarm Pro"), ("env", "test")]);
        assert_eq!(a, b, "a genuine retry (identical body) must get the identical key Stripe expects");
    }

    /// Widening the digest from 8 to 32 hex chars must not push any
    /// constructed idempotency key anywhere near Stripe's 255-char
    /// `Idempotency-Key` limit. Builds the actual longest key shape this
    /// binary constructs (`prov:product:{code}:{env}:{digest}` /
    /// `prov:price:{code}:{env}:{digest}`) from the real `PRODUCTS`/`PRICES`
    /// catalogs plus the longest possible `env` value, and asserts it stays
    /// comfortably under the limit.
    #[test]
    fn idempotency_keys_stay_well_under_stripes_255_char_limit() {
        const STRIPE_IDEMPOTENCY_KEY_MAX_LEN: usize = 255;
        let digest = payload_digest(&[("k", "v")]);
        assert_eq!(digest.len(), 32);

        let longest_env = "live"; // both "live" and "test" are 4 chars
        let longest_product_code =
            PRODUCTS.iter().map(|p| p.coralswarm_code).max_by_key(|c| c.len()).expect("PRODUCTS non-empty");
        let longest_price_code =
            PRICES.iter().map(|p| p.db_code).max_by_key(|c| c.len()).expect("PRICES non-empty");
        let longest_code = std::cmp::max_by_key(longest_product_code, longest_price_code, |c| c.len());

        let product_key = format!("prov:product:{longest_code}:{longest_env}:{digest}");
        let price_key = format!("prov:price:{longest_code}:{longest_env}:{digest}");
        let webhook_key = format!("prov:webhook:create:{digest}");

        for key in [&product_key, &price_key, &webhook_key] {
            assert!(
                key.len() < STRIPE_IDEMPOTENCY_KEY_MAX_LEN / 2,
                "key {key:?} ({} chars) should stay comfortably under Stripe's {STRIPE_IDEMPOTENCY_KEY_MAX_LEN}-char limit",
                key.len()
            );
        }
    }

    // ── should_run_downstream_phases (finding #9) ────────────────────────

    #[test]
    fn should_run_downstream_phases_gates_on_product_errors() {
        assert!(should_run_downstream_phases(0));
        assert!(!should_run_downstream_phases(1));
        assert!(!should_run_downstream_phases(4));
    }

    // ── dry_run_write_count (finding #8) ──────────────────────────────────

    /// THE regression this finding fixes: a first-ever dry run, where
    /// nothing exists in Stripe yet, has an empty `resolved` (no reused
    /// prices) but 6 `WouldCreate` prices — the buggy version reported
    /// "0 row(s) would be written" here.
    #[test]
    fn dry_run_write_count_includes_would_create_entries_on_a_first_ever_run() {
        assert_eq!(dry_run_write_count(0, 6), 6);
    }

    #[test]
    fn dry_run_write_count_sums_reused_and_would_create() {
        assert_eq!(dry_run_write_count(2, 4), 6);
        assert_eq!(dry_run_write_count(6, 0), 6);
    }

    #[test]
    fn an_unknown_flag_is_rejected() {
        let argv = vec!["--bogus".to_string()];
        let err = parse_args(argv.into_iter()).expect_err("must reject an unknown flag");
        assert!(err.contains("--bogus"), "{err:?}");
    }

    #[test]
    fn catalog_has_four_products_and_six_prices() {
        assert_eq!(PRODUCTS.len(), 4);
        assert_eq!(PRICES.len(), 6);
    }

    #[test]
    fn every_price_references_a_declared_product() {
        for price in PRICES {
            assert!(
                PRODUCTS.iter().any(|p| p.id == price.product_id),
                "price {} references undeclared product {}",
                price.lookup_key,
                price.product_id
            );
        }
    }

    #[test]
    fn catalog_amounts_match_the_registered_plan_and_migrations_0165_0166_0155() {
        // pro: $20.00/mo flat, 1500 credits — migration 0155.
        let pro = PRICES.iter().find(|p| p.lookup_key == "cs_pro_v1").unwrap();
        assert_eq!(pro.unit_amount_cents, 2000);
        assert_eq!(pro.credits, 1500);
        assert_eq!(pro.recurring_interval, Some("month"));

        // seat_tiers: standard $40.00/seat/mo 3,000cr, premium $175.00/seat/mo
        // 15,000cr — migration 0165.
        let standard = PRICES
            .iter()
            .find(|p| p.lookup_key == "cs_seat_standard_v1")
            .unwrap();
        assert_eq!(standard.unit_amount_cents, 4000);
        assert_eq!(standard.credits, 3000);
        let premium = PRICES
            .iter()
            .find(|p| p.lookup_key == "cs_seat_premium_v1")
            .unwrap();
        assert_eq!(premium.unit_amount_cents, 17500);
        assert_eq!(premium.credits, 15000);

        // credit_packs: 500/$11, 2,000/$44, 5,000/$110 — migration 0166.
        let pack_500 = PRICES.iter().find(|p| p.lookup_key == "cs_pack_500_v1").unwrap();
        assert_eq!(pack_500.unit_amount_cents, 1100);
        assert_eq!(pack_500.credits, 500);
        assert!(pack_500.recurring_interval.is_none());
        let pack_2000 = PRICES.iter().find(|p| p.lookup_key == "cs_pack_2000_v1").unwrap();
        assert_eq!(pack_2000.unit_amount_cents, 4400);
        assert_eq!(pack_2000.credits, 2000);
        let pack_5000 = PRICES.iter().find(|p| p.lookup_key == "cs_pack_5000_v1").unwrap();
        assert_eq!(pack_5000.unit_amount_cents, 11000);
        assert_eq!(pack_5000.credits, 5000);
    }

    #[test]
    fn price_matches_true_for_an_identical_recurring_price() {
        let spec = &PRICES[0]; // cs_pro_v1: 2000, month, product cs_plan_pro
        let existing = serde_json::json!({
            "unit_amount": 2000,
            "currency": "usd",
            "recurring": { "interval": "month", "usage_type": "licensed" },
            "product": "cs_plan_pro",
            "tax_behavior": "exclusive"
        });
        assert!(price_matches(&existing, spec));
    }

    #[test]
    fn price_matches_true_for_an_identical_one_time_price_with_null_recurring() {
        let spec = PRICES.iter().find(|p| p.lookup_key == "cs_pack_500_v1").unwrap();
        let existing = serde_json::json!({
            "unit_amount": 1100,
            "currency": "usd",
            "recurring": null,
            "product": "cs_pack_credits",
            "tax_behavior": "exclusive"
        });
        assert!(price_matches(existing_ref(&existing), spec));
    }

    fn existing_ref(v: &serde_json::Value) -> &serde_json::Value {
        v
    }

    #[test]
    fn price_matches_false_on_amount_drift() {
        let spec = &PRICES[0];
        let existing = serde_json::json!({
            "unit_amount": 2500,
            "currency": "usd",
            "recurring": { "interval": "month", "usage_type": "licensed" },
            "product": "cs_plan_pro",
            "tax_behavior": "exclusive"
        });
        assert!(!price_matches(&existing, spec));
    }

    #[test]
    fn price_matches_false_on_interval_drift() {
        let spec = &PRICES[0];
        let existing = serde_json::json!({
            "unit_amount": 2000,
            "currency": "usd",
            "recurring": { "interval": "year", "usage_type": "licensed" },
            "product": "cs_plan_pro",
            "tax_behavior": "exclusive"
        });
        assert!(!price_matches(&existing, spec));
    }

    #[test]
    fn price_matches_false_on_currency_drift() {
        let spec = &PRICES[0];
        let existing = serde_json::json!({
            "unit_amount": 2000,
            "currency": "eur",
            "recurring": { "interval": "month", "usage_type": "licensed" },
            "product": "cs_plan_pro",
            "tax_behavior": "exclusive"
        });
        assert!(!price_matches(&existing, spec));
    }

    /// Finding #2, round 2: a price attached to the WRONG product must not
    /// be `Reused` just because amount/currency/interval happen to match —
    /// that would silently point this catalog's price id at another
    /// product's price.
    #[test]
    fn price_matches_false_on_product_drift() {
        let spec = &PRICES[0]; // cs_pro_v1, product cs_plan_pro
        let existing = serde_json::json!({
            "unit_amount": 2000,
            "currency": "usd",
            "recurring": { "interval": "month", "usage_type": "licensed" },
            "product": "cs_seat_standard",
            "tax_behavior": "exclusive"
        });
        assert!(!price_matches(&existing, spec));
    }

    /// Finding #2, round 2: `tax_behavior` is a one-way door (Q3) — a price
    /// created `inclusive` must never be treated as interchangeable with
    /// this catalog's `exclusive` prices.
    #[test]
    fn price_matches_false_on_tax_behavior_drift() {
        let spec = &PRICES[0];
        let existing = serde_json::json!({
            "unit_amount": 2000,
            "currency": "usd",
            "recurring": { "interval": "month", "usage_type": "licensed" },
            "product": "cs_plan_pro",
            "tax_behavior": "inclusive"
        });
        assert!(!price_matches(&existing, spec));
    }

    /// Finding #2, round 2: a recurring price with `usage_type: metered`
    /// must not be `Reused` for a catalog spec that always creates
    /// `licensed` prices (seat/plan quantity is the subscription-item seat
    /// count, never metered usage).
    #[test]
    fn price_matches_false_on_recurring_usage_type_drift() {
        let spec = &PRICES[0];
        let existing = serde_json::json!({
            "unit_amount": 2000,
            "currency": "usd",
            "recurring": { "interval": "month", "usage_type": "metered" },
            "product": "cs_plan_pro",
            "tax_behavior": "exclusive"
        });
        assert!(!price_matches(&existing, spec));
    }
}
