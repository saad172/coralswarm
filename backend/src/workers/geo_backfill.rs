//! Batched geo resolution worker (platform-admin-metrics PR 4 of 7).
//!
//! `middleware::access_log::spawn_capture` (this same PR) inserts
//! `user_access_log` rows with `ip` set and `country`/`region`/`city` always
//! `NULL` — geolocation is deliberately NOT resolved on the request path.
//! This worker is the batch job that fills those columns in afterward,
//! modelled on `workers::access_log_purge` for structure (standalone loop —
//! `user_access_log` is CONTROL-only with no tenant-tree counterpart, so
//! there is no `workers::tenant_harness` dimension to loop over — see that
//! worker's own module doc), interval/timeout shape, and tracing
//! conventions.
//!
//! ## Why batched, not inline on the auth path
//!
//! Three deliberate reasons, all load-bearing for this design and none of
//! them a shortcut:
//!
//! 1. **Latency.** A geo lookup, if it ever needed an outbound network call
//!    (a third-party HTTP geo-IP API), would add a round trip to a call site
//!    that runs on EVERY authenticated request — `middleware::auth::
//!    require_auth`, which `spawn_capture` is dispatched from. Even a
//!    resolution that stayed off the critical path via `tokio::spawn` would
//!    still, per the request-path brief, be "measure nothing but keep it
//!    cheap" territory; a batch job with no per-request latency budget at
//!    all is strictly safer than trying to bound an inline call.
//! 2. **Privacy.** Resolving via a THIRD-PARTY HTTP API would mean
//!    transmitting every user's real IP address to an external service on
//!    every login — exactly the kind of egress this repo's org-egress-policy
//!    machinery (`routes::prefs`, `account_prefs.org_egress_policy`) exists
//!    to gate for CONTENT, and there is no equivalent consent gate for this
//!    operational/security-log data. A purely LOCAL, offline database
//!    lookup (MaxMind GeoLite2 City, `.mmdb`, read via the `maxminddb`
//!    crate) means the raw IP never leaves this process, let alone this
//!    database. **This is why no HTTP geo-IP API client is used here, even
//!    as a documented option** — do not add one.
//! 3. **Swappability.** The resolution strategy lives entirely in
//!    [`GeoLookup`]/[`GeoResolver`], with no caller (`middleware::auth`,
//!    `middleware::access_log`) aware that geo resolution exists at all.
//!    Swapping MaxMind for a different local database format, or for some
//!    future in-house IP-range table, is a change entirely local to this
//!    file.
//!
//! ## Resolver: local GeoLite2 City `.mmdb`, or nothing at all
//!
//! [`GeoResolver::from_env`] reads the file path from `GEOIP_DB_PATH`
//! ([`GEOIP_DB_PATH_ENV`]). When the var is unset, or the path doesn't open
//! as a valid MaxMind DB, this logs ONCE at `info`/`warn` and returns
//! `None` — the caller (`lib.rs::run`) then simply never spawns this
//! worker, `country`/`region`/`city` stay `NULL` forever, and nothing else
//! in the system depends on them being populated (a later PR's rollup logic
//! must treat their absence as "not yet resolved / never will be", not as an
//! error). No panic, no fallback to a network call — see point 2 above.
//!
//! **This repo never commits a GeoLite2 database file** — it is a large,
//! separately-licensed (MaxMind EULA) binary artifact. See `.env.example`
//! for how to point `GEOIP_DB_PATH` at a locally-downloaded copy.
//!
//! ## Retention interaction (Unit A's `workers::access_log_purge`)
//!
//! `access_log_purge::tick` deletes `user_access_log` rows older than
//! `ACCESS_LOG_RETENTION_DAYS` (30) on its own daily sweep, running
//! independently of this worker. This worker's batch UPDATE is keyed by
//! `id` (the row's own primary key, fetched in the same `SELECT` as the
//! candidate rows) rather than by `ip` or any other non-unique criterion, so
//! if the purge worker deletes a row THIS worker already selected into
//! memory (a race between the two independent loops), the subsequent
//! `UPDATE ... WHERE id = $1` simply matches zero rows — no error, no
//! resurrection, nothing logged. There is no ordering requirement between
//! the two workers: this one can never re-create a row the purge worker
//! removed, because it only ever `UPDATE`s existing rows, never `INSERT`s.
//!
//! A row whose IP resolves to no data at all (private/reserved ranges are
//! common in local dev; `MaxMindDBError::AddressNotFoundError`) is left
//! untouched — `country` stays `NULL`, so it remains a candidate and is
//! retried on every subsequent tick until either it resolves (it won't) or
//! the 30-day purge removes it. This is a deliberate, bounded, documented
//! trade (an unresolvable row costs one wasted lookup per tick for at most
//! 30 days) rather than inventing a sentinel "resolution attempted, found
//! nothing" value that would need its own column.

use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

use maxminddb::geoip2;
use sqlx::PgPool;
use uuid::Uuid;

use crate::shutdown::Shutdown;

// ── Tuning constants ──────────────────────────────────────────────────────────

/// How often the backfill sweep runs. Geo resolution has no correctness
/// deadline (nothing downstream requires it to be fresh), so a few minutes'
/// staleness is fine; short enough that most rows resolve well within the
/// 30-day retention window.
pub const GEO_BACKFILL_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Per-sweep timeout on the whole batch, mirroring
/// `workers::access_log_purge::PURGE_TIMEOUT`: a batch that hasn't finished
/// in a minute is stuck (pool exhaustion, a wedged mmdb read) and should
/// degrade to a logged failure rather than parking this worker's loop.
const BACKFILL_TIMEOUT: Duration = Duration::from_secs(60);

/// How often the inter-sweep sleep polls [`Shutdown`] — same value/rationale
/// as every other standalone sweep worker in this module
/// (`workers::access_log_purge::SHUTDOWN_POLL_INTERVAL`).
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Rows resolved per tick. Bounds one sweep's DB + mmdb-lookup work so a
/// sudden backlog (e.g. this worker disabled for a while, then re-enabled)
/// drains gradually across several ticks instead of one unbounded pass.
pub const GEO_BACKFILL_BATCH_SIZE: i64 = 500;

/// Env var naming the local GeoLite2 City `.mmdb` file path. See the module
/// doc's "Resolver" section and `.env.example`.
pub const GEOIP_DB_PATH_ENV: &str = "GEOIP_DB_PATH";

// ── Resolved fields ───────────────────────────────────────────────────────────

/// The three columns this worker ever writes. `None` on any field means
/// "the database had nothing at that level of granularity for this IP" —
/// e.g. a country-only match with no city — never "resolution failed"
/// (that case yields no [`GeoFields`] at all — see [`GeoLookup::resolve`]).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GeoFields {
    pub country: Option<String>,
    pub region: Option<String>,
    pub city: Option<String>,
}

/// Anything that can resolve an IP to geo fields. [`GeoResolver`] is the one
/// production implementation (local MaxMind GeoLite2 City lookup); test code
/// substitutes a deterministic stub so [`tick`]'s batch-selection/update
/// logic is provable end-to-end against real Postgres without this repo
/// ever holding a real (proprietary, separately-licensed) GeoLite2 file —
/// see the module doc.
pub trait GeoLookup {
    /// Resolve one IP to geo fields, or `None` when the database has no
    /// entry for it at all (as opposed to an entry with some fields unset —
    /// see [`GeoFields`]'s own doc comment for that distinction).
    fn resolve(&self, ip: IpAddr) -> Option<GeoFields>;
}

/// Local MaxMind GeoLite2 City database reader.
pub struct GeoResolver {
    reader: maxminddb::Reader<Vec<u8>>,
}

impl GeoResolver {
    /// Build from [`GEOIP_DB_PATH_ENV`]. `None` (logged once, `info` for
    /// "simply not configured", `warn` for "configured but unusable") when
    /// the var is unset or the file doesn't open as a valid MaxMind DB —
    /// see the module doc's "Resolver" section for why that is the whole
    /// contract, never a panic and never a network fallback.
    pub fn from_env() -> Option<Self> {
        let path = match std::env::var(GEOIP_DB_PATH_ENV) {
            Ok(p) => p,
            Err(_) => {
                tracing::info!(
                    "{GEOIP_DB_PATH_ENV} not set; geo backfill worker will not run \
                     (user_access_log.country/region/city stay NULL)"
                );
                return None;
            }
        };
        match Self::open(&path) {
            Ok(resolver) => {
                tracing::info!(path = %path, "geo backfill: GeoLite2 database loaded");
                Some(resolver)
            }
            Err(e) => {
                tracing::warn!(
                    path = %path,
                    error = %e,
                    "geo backfill: failed to open {GEOIP_DB_PATH_ENV}; geo backfill worker \
                     will not run (user_access_log.country/region/city stay NULL)"
                );
                None
            }
        }
    }

    /// Open a GeoLite2 City `.mmdb` file directly. `pub` (not
    /// `cfg(test)`-gated) so both [`from_env`](Self::from_env) and
    /// integration tests that point at a real database can share this one
    /// path — this repo never commits such a file (see module doc), so
    /// tests that need one are expected to supply their own via an
    /// operator-provided path, same as production.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        maxminddb::Reader::open_readfile(path)
            .map(|reader| Self { reader })
            .map_err(|e| e.to_string())
    }
}

impl GeoLookup for GeoResolver {
    fn resolve(&self, ip: IpAddr) -> Option<GeoFields> {
        let city: geoip2::City = self.reader.lookup(ip).ok()?;
        extract_geo_fields(&city)
    }
}

/// Pure field-extraction from a decoded `geoip2::City` record — factored out
/// of [`GeoResolver::resolve`] so it is unit-testable without a real `.mmdb`
/// file (construct a `geoip2::City` by hand instead of going through a
/// lookup). Prefers the English name for region/city, matching the
/// convention `resolve_clerk_context`'s no-analogue-here reasoning aside,
/// simply MaxMind's own most-populated locale.
///
/// Returns `None` only when NOTHING at all is available (no country, no
/// region, no city) — a genuinely info-free record, as opposed to a partial
/// one (country known, city unknown), which returns `Some` with some fields
/// `None`.
fn extract_geo_fields(city: &geoip2::City) -> Option<GeoFields> {
    let country = city
        .country
        .as_ref()
        .and_then(|c| c.iso_code)
        .map(str::to_string);
    let region = city
        .subdivisions
        .as_ref()
        .and_then(|subs| subs.first())
        .and_then(|sub| sub.names.as_ref())
        .and_then(|names| names.get("en"))
        .map(|s| s.to_string());
    let city_name = city
        .city
        .as_ref()
        .and_then(|c| c.names.as_ref())
        .and_then(|names| names.get("en"))
        .map(|s| s.to_string());

    if country.is_none() && region.is_none() && city_name.is_none() {
        return None;
    }
    Some(GeoFields {
        country,
        region,
        city: city_name,
    })
}

// ── Worker entry point ────────────────────────────────────────────────────────

/// Run the geo-backfill worker loop until `shutdown` is signalled. Only ever
/// spawned when [`GeoResolver::from_env`] returned `Some` — `lib.rs::run`
/// simply doesn't spawn this task at all otherwise, which is this worker's
/// entire "no database configured" story (see module doc).
pub async fn run<R>(pool: PgPool, resolver: R, shutdown: Shutdown)
where
    R: GeoLookup + Send + Sync + 'static,
{
    tracing::info!("geo-backfill worker started");

    loop {
        if shutdown.is_triggered() {
            tracing::info!("geo-backfill worker: shutdown signalled, stopping");
            return;
        }

        match tick(&pool, &resolver).await {
            Ok(0) => tracing::debug!("geo backfill: nothing to resolve"),
            Ok(n) => tracing::info!(resolved = n, "geo backfill: batch complete"),
            Err(e) => tracing::warn!(error = %e, "geo backfill failed; will retry next cycle"),
        }

        tokio::select! {
            _ = tokio::time::sleep(GEO_BACKFILL_INTERVAL) => {}
            _ = poll_until_triggered(&shutdown) => {
                tracing::info!("geo-backfill worker: shutdown signalled during inter-sweep sleep, stopping");
                return;
            }
        }
    }
}

/// Resolve once `shutdown` is triggered, polling every
/// [`SHUTDOWN_POLL_INTERVAL`]. Same pattern as
/// `workers::access_log_purge::poll_until_triggered`.
async fn poll_until_triggered(shutdown: &Shutdown) {
    loop {
        if shutdown.is_triggered() {
            return;
        }
        tokio::time::sleep(SHUTDOWN_POLL_INTERVAL).await;
    }
}

/// One backfill sweep: select up to [`GEO_BACKFILL_BATCH_SIZE`] candidate
/// rows and resolve/update each. `pub` (like `workers::access_log_purge::
/// tick`) so integration tests can drive one sweep against real Postgres
/// without spawning [`run`]'s infinite loop. Generic over [`GeoLookup`] so
/// tests can substitute a deterministic stub — see the module doc.
pub async fn tick<R: GeoLookup + ?Sized>(pool: &PgPool, resolver: &R) -> Result<u64, String> {
    match tokio::time::timeout(BACKFILL_TIMEOUT, run_batch(pool, resolver)).await {
        Err(_) => Err(format!(
            "geo backfill timed out after {}s",
            BACKFILL_TIMEOUT.as_secs()
        )),
        Ok(Err(e)) => Err(e.to_string()),
        Ok(Ok(n)) => Ok(n),
    }
}

async fn run_batch<R: GeoLookup + ?Sized>(
    pool: &PgPool,
    resolver: &R,
) -> Result<u64, sqlx::Error> {
    // `host(ip)` (not a bare `ip::text` cast!) avoids needing the `sqlx`
    // `ipnetwork` feature crate-wide for this one read path (mirrors
    // `middleware::access_log::spawn_capture`'s `$4::inet` write-side cast).
    // Postgres's `::text` cast on an `inet` column includes the netmask
    // suffix even for a full-width host address (`'203.0.113.7'::inet::text`
    // is `"203.0.113.7/32"`, NOT `"203.0.113.7"` — confirmed against a real
    // instance; std's `IpAddr::from_str` rejects the `/32` CIDR suffix
    // outright), whereas `host()` is Postgres's own "just the address, never
    // a mask" function and always round-trips through `IpAddr::from_str`.
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT id, host(ip) FROM user_access_log \
         WHERE ip IS NOT NULL AND country IS NULL \
         ORDER BY created_at \
         LIMIT $1",
    )
    .bind(GEO_BACKFILL_BATCH_SIZE)
    .fetch_all(pool)
    .await?;

    let mut resolved_count = 0u64;
    for (id, ip_text) in rows {
        let Ok(ip) = ip_text.parse::<IpAddr>() else {
            // Defensive only: a value that came out of a real Postgres
            // `inet` column via `::text` should always parse. Never a
            // reason to fail the whole batch.
            tracing::warn!(row_id = %id, "geo backfill: stored ip did not parse back as IpAddr; skipping row");
            continue;
        };
        let Some(fields) = resolver.resolve(ip) else {
            // No data at this IP (private/reserved range, or genuinely
            // absent from the database) — left NULL, retried next tick
            // until it ages out via the purge worker's retention window.
            // See the module doc's "Retention interaction" section.
            continue;
        };

        // Keyed by the row's OWN primary key, not by ip or any other
        // non-unique criterion — see the module doc's "Retention
        // interaction" section for why this can never resurrect or race
        // against `workers::access_log_purge`.
        let result = sqlx::query(
            "UPDATE user_access_log SET country = $2, region = $3, city = $4 WHERE id = $1",
        )
        .bind(id)
        .bind(&fields.country)
        .bind(&fields.region)
        .bind(&fields.city)
        .execute(pool)
        .await?;
        if result.rows_affected() > 0 {
            resolved_count += 1;
        }
    }
    Ok(resolved_count)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty `geoip2::City` record — every field `None` — as a base for
    /// the extraction tests below to override just what they need. Building
    /// this by hand (rather than through a real `.mmdb` lookup) is exactly
    /// why [`extract_geo_fields`] is factored out as a pure function: this
    /// repo never commits a real GeoLite2 database (module doc), so these
    /// tests exercise the SAME extraction code a real lookup would run,
    /// without needing one.
    fn empty_city<'a>() -> geoip2::City<'a> {
        geoip2::City {
            city: None,
            continent: None,
            country: None,
            location: None,
            postal: None,
            registered_country: None,
            represented_country: None,
            subdivisions: None,
            traits: None,
        }
    }

    #[test]
    fn extract_geo_fields_returns_none_for_a_fully_empty_record() {
        assert_eq!(extract_geo_fields(&empty_city()), None);
    }

    #[test]
    fn extract_geo_fields_reads_country_iso_code() {
        let mut city = empty_city();
        city.country = Some(geoip2::country::Country {
            geoname_id: None,
            is_in_european_union: None,
            iso_code: Some("US"),
            names: None,
        });
        let fields = extract_geo_fields(&city).expect("some fields resolved");
        assert_eq!(fields.country.as_deref(), Some("US"));
        assert_eq!(fields.region, None);
        assert_eq!(fields.city, None);
    }

    #[test]
    fn extract_geo_fields_reads_region_from_first_subdivision_english_name() {
        let mut city = empty_city();
        let mut names = std::collections::BTreeMap::new();
        names.insert("en", "Texas");
        city.subdivisions = Some(vec![geoip2::city::Subdivision {
            geoname_id: None,
            iso_code: Some("TX"),
            names: Some(names),
        }]);
        let fields = extract_geo_fields(&city).expect("some fields resolved");
        assert_eq!(fields.region.as_deref(), Some("Texas"));
    }

    #[test]
    fn extract_geo_fields_reads_city_english_name() {
        let mut city = empty_city();
        let mut names = std::collections::BTreeMap::new();
        names.insert("en", "Fort Worth");
        city.city = Some(geoip2::city::City {
            geoname_id: None,
            names: Some(names),
        });
        let fields = extract_geo_fields(&city).expect("some fields resolved");
        assert_eq!(fields.city.as_deref(), Some("Fort Worth"));
    }

    #[test]
    fn extract_geo_fields_combines_all_three_when_present() {
        let mut city = empty_city();
        city.country = Some(geoip2::country::Country {
            geoname_id: None,
            is_in_european_union: None,
            iso_code: Some("US"),
            names: None,
        });
        let mut region_names = std::collections::BTreeMap::new();
        region_names.insert("en", "Texas");
        city.subdivisions = Some(vec![geoip2::city::Subdivision {
            geoname_id: None,
            iso_code: Some("TX"),
            names: Some(region_names),
        }]);
        let mut city_names = std::collections::BTreeMap::new();
        city_names.insert("en", "Fort Worth");
        city.city = Some(geoip2::city::City {
            geoname_id: None,
            names: Some(city_names),
        });

        let fields = extract_geo_fields(&city).expect("some fields resolved");
        assert_eq!(
            fields,
            GeoFields {
                country: Some("US".to_string()),
                region: Some("Texas".to_string()),
                city: Some("Fort Worth".to_string()),
            }
        );
    }

    // ── GeoResolver::from_env / open — the "no database configured" contract ──

    #[test]
    fn from_env_is_none_when_env_var_unset() {
        // Isolated by construction: this test never sets GEOIP_DB_PATH, and
        // this crate's test suite does not otherwise set it globally.
        // (Deliberately not asserting via env mutation to avoid cross-test
        // races — see `open_rejects_a_file_that_is_not_a_valid_mmdb` for the
        // path-based equivalent that doesn't depend on ambient env state.)
        std::env::remove_var(GEOIP_DB_PATH_ENV);
        assert!(GeoResolver::from_env().is_none());
    }

    #[test]
    fn open_rejects_a_nonexistent_path() {
        assert!(GeoResolver::open("/nonexistent/path/does-not-exist.mmdb").is_err());
    }

    #[test]
    fn open_rejects_a_file_that_is_not_a_valid_mmdb() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("geo-backfill-test-not-a-db-{}.mmdb", Uuid::new_v4()));
        std::fs::write(&path, b"not a real maxmind database").unwrap();
        let result = GeoResolver::open(&path);
        std::fs::remove_file(&path).ok();
        assert!(result.is_err(), "a garbage file must not open as a valid GeoResolver");
    }
}
