//! `jobs_account_backfill` — the reusable core of the one-shot
//! `backfill_unattributed_job_accounts` binary (`backend/src/bin/
//! backfill_unattributed_job_accounts.rs` is a thin argument-parse +
//! DSN-resolution wrapper around [`backfill_database`]; every gate, counter
//! and write lives HERE so `backend/tests/jobs_account_backfill_pg.rs`
//! drives the same code an operator runs, not a re-implementation of it —
//! same split as [`crate::judgement_backfill`]).
//!
//! # What this fixes
//!
//! `workers::billing_derivation::tick` runs an alerting side-query every
//! pass:
//!
//! ```sql
//! SELECT count(*) FROM jobs
//!  WHERE status = 'done' AND kind = 'ExtractEntities' AND account_id IS NULL
//!    AND finished_at <= $1
//! ```
//!
//! and logs `alert = true` on a non-zero count — these are completed
//! `ExtractEntities` jobs migration `0085_jobs_account_id`'s NULLABLE
//! `account_id` column was added for, but that never got stamped (either a
//! pre-W.1 row predating the column entirely, or a row `jobs::queue::
//! dequeue`'s own legacy-backfill resolver could not resolve at DEQUEUE
//! time — see that function's `backfill_legacy_account_id`). Measured in
//! production: 3,493 such rows, `finished_at` spanning 2026-05-23 →
//! 2026-07-23, ZERO in the trailing 14 days — the bug that produced them is
//! already fixed, this is pure historical residue, and it was tripping the
//! alert on every single derivation pass, forever. A permanently-firing
//! alert is worse than none: it guarantees alert fatigue and would mask the
//! real thing when one actually recurs.
//!
//! This backfill closes the gap the same way `queue::dequeue` closes it for
//! `pending` rows — resolve the job's `payload->>'atom_id'` to an owning
//! account through `atoms.graph_id -> graphs.account_id`, then stamp
//! `jobs.account_id` — except this covers the `done` rows dequeue's own
//! resolver never got a chance to touch (they either predate it or already
//! finished before this PR). The complementary half of the fix —
//! bounding the alert itself so residue this backfill has not yet reached
//! (or genuinely cannot resolve — see below) stops tripping it forever — is
//! `workers::billing_derivation::UNATTRIBUTED_ALERT_WINDOW`, not this
//! module; a clean backfill alone cannot make the alert self-limiting,
//! because a handful of rows are unresolvable by construction (their atom
//! was since deleted) and would otherwise keep it firing at a lower count
//! forever.
//!
//! # RUN ONCE PER DATABASE — the control plane AND every active tenant
//!
//! `jobs`/`atoms`/`graphs` are shared tenant-tree tables (present, with the
//! identical shape, on the control plane — which doubles as the consumer
//! CONTENT plane — and on every org's own tenant database; see
//! `workers::billing_derivation`'s own module doc, "Shape" section). A
//! control-plane-only run therefore reports a falsely-drained queue for
//! every org tenant, the exact failure mode the `one-shot-backfills-run-
//! per-tenant` note exists to prevent. The DEFAULT selection here — no
//! flags at all — is control plane + every ACTIVE tenant, same as
//! [`crate::judgement_backfill`]/`backfill_reef_judgement_from_history`,
//! precisely so the common invocation cannot silently miss a plane.
//!
//! Re-running (any scope, any number of times) is SAFE — see "Idempotency"
//! below.
//!
//! # Idempotency
//!
//! The candidate query's `WHERE ... account_id IS NULL` clause is not only
//! the alert's own predicate, it is this backfill's entire idempotency
//! mechanism: once a row is stamped, it structurally no longer matches its
//! own selection query, on this run or any future one. There is no separate
//! "already processed" marker to maintain and no way for it to drift out of
//! sync with the write it is supposed to guard — the SAME clause is both
//! "what to select" and "what to never re-touch". The per-row `UPDATE`
//! additionally repeats `AND account_id IS NULL` in its own `WHERE` (belt
//! and suspenders against a hypothetical concurrent writer racing between
//! this run's SELECT and its UPDATE — see [`BackfillCounts::raced`]), but
//! that repetition is defense in depth, not what makes re-running safe: a
//! second full run finds ZERO candidates for every row the first run
//! resolved, before it would ever reach a write. Proved live by
//! `jobs_account_backfill_pg.rs`'s `rerun_changes_nothing`.
//!
//! # Why every write here is a single unscoped statement
//!
//! `jobs` carries no RLS on either plane — migration `0083_consumer_rls`'s
//! own header explicitly "flags, not covers" it (dequeue's `FOR UPDATE SKIP
//! LOCKED` claim must see every account's pending rows regardless of which
//! account is asking), and `billing_derivation::tick` already runs its
//! alerting count the same way, unscoped, against `t.pool` directly — this
//! backfill's `SELECT`/`UPDATE` against `jobs` match that on purpose, not by
//! oversight: there is no `app.account_id` scope to open here in the first
//! place, because the whole POINT of this table's rows, before this
//! backfill runs, is that nobody yet knows which account most of them
//! belong to.
//!
//! `atoms`/`graphs`, by contrast, ARE on the consumer-plane RLS roster. This
//! backfill never reads either table directly — it resolves
//! `atom_id -> account_id` exclusively through `resolve_atoms_account_id`,
//! the `SECURITY DEFINER` bootstrap function migrations `0084`/`0003`
//! (control/tenant trees respectively) exist FOR exactly this
//! chicken-and-egg shape ("which account owns this row, before any scope
//! can be opened to find out"). It is the SAME function
//! `jobs::queue::dequeue`'s `backfill_legacy_account_id` already calls for
//! this exact purpose on `pending` rows (see that function's doc) — this
//! module intentionally does not re-implement the join by hand. Two
//! practical benefits fall out of reusing it rather than a raw
//! `atoms JOIN graphs` query: it works correctly regardless of whether the
//! connecting role bypasses RLS (a raw join under the least-privilege
//! `coralswarm_app` role with no `app.account_id` GUC set would hard-error,
//! per `billing_derivation`'s own "an unset app.account_id GUC hard-errors
//! the ::uuid cast" note — SECURITY DEFINER sidesteps that entirely), and it
//! is granted to `coralswarm_app` on the control plane, so — like
//! `scripts/export_member_ocean.sh`'s own documented precedent — an
//! elevated `MIGRATOR_DATABASE_URL` is accepted and preferred for operator
//! consistency with the sibling one-shot tools in `bin/`, but is NOT
//! strictly required the way it is for `judgement_backfill` (whose target
//! table, `reef_judgement`, genuinely is RLS-covered).
//!
//! # Resolution outcomes
//!
//! Per candidate row, in order:
//!
//! 1. **Parse `payload->>'atom_id'`.** `ExtractEntities`'s payload carries
//!    exactly one key (`atom_id`) by construction (`jobs::JobKind::
//!    ExtractEntities { atom_id }`'s serialization) — a row with no
//!    parseable atom_id is a shape that should not exist and is counted
//!    separately ([`BackfillCounts::malformed_payload`]) rather than folded
//!    into the "genuinely unresolvable" bucket below, so a non-zero value
//!    here is a signal worth investigating on its own.
//! 2. **Resolve via `resolve_atoms_account_id`.** Zero rows back means the
//!    atom no longer exists (deleted since the job ran) —
//!    [`BackfillCounts::dead_atom`], the EXPECTED shape for the handful of
//!    genuinely unresolvable rows (measured: 20 of 3,493 in production).
//!    More than one distinct account is structurally impossible for a
//!    single atom_id (an atom belongs to exactly one graph, which belongs
//!    to exactly one account) but is handled defensively anyway
//!    ([`BackfillCounts::ambiguous_atom`]), matching `backfill_legacy_
//!    account_id`'s own defensive `many => Unresolvable` arm for the
//!    identical theoretical case.
//! 3. **Write.** `--dry-run` (the default) counts what WOULD be written
//!    without touching the database at all. `--execute` issues the `UPDATE`
//!    described above.
//!
//! [`BackfillCounts::unresolvable`] sums the three "could not attribute"
//! buckets — the number a founder should expect to see the alert's own
//! (separately bounded) window still catch, forever, for exactly the rows
//! whose atom is gone for good.

use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

/// How this run was invoked. `execute: false` (the default, same inversion
/// as `judgement_backfill::BackfillOpts`) performs the full candidate scan
/// and the full resolution walk against the real database — every counter
/// below predicts an `--execute` run exactly — but issues no `UPDATE`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillOpts {
    pub execute: bool,
}

impl BackfillOpts {
    /// `"execute"` / `"dry-run"` — the `mode=` field of [`summary_line`].
    pub fn mode(&self) -> &'static str {
        if self.execute { "execute" } else { "dry-run" }
    }
}

/// Per-database tallies. Every candidate row is accounted for by EXACTLY one
/// of the buckets below (see [`Self::accounted`]) — a dry run's `updated`
/// means "would update", same convention as `judgement_backfill::
/// BackfillCounts`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillCounts {
    /// Rows matching the alert's own predicate MINUS its `finished_at <=
    /// cutoff` recency bound — see this module's doc for why a backfill
    /// must reconcile every historical row regardless of age, while the
    /// alert itself stays separately (and deliberately) bounded.
    pub scanned: u64,
    /// Gate 1: `payload->>'atom_id'` missing or not a parseable UUID.
    /// Expected to be exactly 0 for `kind='ExtractEntities'` rows — a
    /// non-zero value here is its own finding, not ordinary backfill noise.
    pub malformed_payload: u64,
    /// Gate 2: `resolve_atoms_account_id` returned zero rows — the atom no
    /// longer exists. THE EXPECTED SHAPE for whatever remains unresolvable
    /// after this backfill runs (measured: 20 of 3,493 in production).
    pub dead_atom: u64,
    /// Gate 2: `resolve_atoms_account_id` returned more than one distinct
    /// account for a SINGLE atom_id — structurally impossible (an atom has
    /// exactly one graph, a graph has exactly one account), kept as its own
    /// counted bucket purely for defensive parity with `jobs::queue::
    /// backfill_legacy_account_id`'s identical defensive arm.
    pub ambiguous_atom: u64,
    /// Gate 3: written (execute) or would be written (dry run).
    pub updated: u64,
    /// Execute-only: the `UPDATE ... WHERE id = $2 AND account_id IS NULL`
    /// matched zero rows despite the row having just been selected as a
    /// NULL-`account_id` candidate — i.e. something else set `account_id`
    /// (or deleted the row) in the gap between this run's `SELECT` and its
    /// `UPDATE`. Not expected in practice (nothing else in this codebase
    /// writes `jobs.account_id` on a `done` row), but counted rather than
    /// silently treated as success, and never treated as an error — the row
    /// ended up attributed (or gone) either way.
    pub raced: u64,
    /// An unexpected database error at either the resolve or the write step
    /// for one candidate. The run continues past it (a one-shot must not be
    /// stranded by one bad row); non-zero makes the binary exit non-zero.
    pub errors: u64,
}

impl BackfillCounts {
    /// Every candidate is accounted for by exactly one gate — used by the
    /// unit test below and worth stating: a tally that doesn't balance means
    /// a candidate fell through [`backfill_database`]'s loop without being
    /// counted.
    pub fn accounted(&self) -> u64 {
        self.malformed_payload + self.dead_atom + self.ambiguous_atom + self.updated + self.raced + self.errors
    }

    /// The number the alert's OWN (separately bounded) window should expect
    /// to keep seeing after a clean backfill — every row this pass could not
    /// attribute, for any reason.
    pub fn unresolvable(&self) -> u64 {
        self.malformed_payload + self.dead_atom + self.ambiguous_atom
    }
}

/// Anything that stops the WHOLE database being processed. Per-CANDIDATE
/// problems never surface here — they increment [`BackfillCounts::errors`]
/// and the run continues (see that field's doc).
#[derive(Debug, thiserror::Error)]
pub enum BackfillError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),
}

/// One `jobs` row reduced to what resolution needs.
#[derive(Debug, sqlx::FromRow)]
struct JobRow {
    id: Uuid,
    payload: Value,
}

/// Parse `payload->>'atom_id'` — the only key `ExtractEntities`'s payload
/// carries (`jobs::JobKind::ExtractEntities { atom_id }`'s serialization; see
/// `jobs::queue`'s `job_account_ref`'s identical `"ExtractEntities" |
/// "ExtractRelationships"` arm, which this module's doc explains why it does
/// NOT call directly — same field, read independently so this module has no
/// dependency on that private function).
fn parse_atom_id(payload: &Value) -> Option<Uuid> {
    payload.get("atom_id").and_then(|v| v.as_str()).and_then(|s| Uuid::parse_str(s).ok())
}

/// Reconcile ONE already-connected database (control plane or one tenant).
///
/// Walks every `jobs` row matching the alert's own predicate minus its
/// recency bound (see this module's doc), resolving each through
/// `resolve_atoms_account_id` and writing (or, in a dry run, simulating) the
/// stamp. Order is `finished_at, id` purely so a `--dry-run`'s printed
/// report is stable across repeated invocations against an unchanged
/// database — the resolution itself has no cross-row dependency, so nothing
/// about correctness relies on this ordering.
pub async fn backfill_database(
    pool: &PgPool,
    opts: &BackfillOpts,
) -> Result<BackfillCounts, BackfillError> {
    let mut counts = BackfillCounts::default();

    // The alert's EXACT predicate (`workers::billing_derivation::tick`)
    // minus its `finished_at <= cutoff` upper bound — this backfill's job is
    // to reconcile every historical row regardless of how long ago it
    // finished, not just the ones a 3-minute-lagged "as of now" cutoff would
    // admit. `jobs` carries no RLS on either plane (0083's header: "flagged,
    // not covered") and this backfill runs as a database-wide maintenance
    // pass, not on behalf of any one account — a single unscoped read here
    // is therefore correct, not an oversight (see this module's doc, "Why
    // every write here is a single unscoped statement").
    let rows: Vec<JobRow> = sqlx::query_as(
        "SELECT id, payload FROM jobs \
         WHERE status = 'done' AND kind = 'ExtractEntities' AND account_id IS NULL \
         ORDER BY finished_at, id",
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        counts.scanned += 1;

        // ── Gate 1: parse the payload's only key.
        let atom_id = match parse_atom_id(&row.payload) {
            Some(id) => id,
            None => {
                counts.malformed_payload += 1;
                tracing::warn!(
                    job_id = %row.id,
                    "jobs_account_backfill: ExtractEntities job has no parseable payload->>'atom_id'"
                );
                continue;
            }
        };

        // ── Gate 2: resolve via the SECURITY DEFINER bootstrap function —
        // see this module's doc for why a raw `atoms JOIN graphs` query is
        // not safe to use here instead.
        let atom_ids = vec![atom_id];
        let accounts: Vec<Uuid> =
            match sqlx::query_scalar::<_, Uuid>("SELECT resolve_atoms_account_id($1)")
                .bind(&atom_ids)
                .fetch_all(pool)
                .await
            {
                Ok(a) => a,
                Err(e) => {
                    counts.errors += 1;
                    tracing::warn!(
                        job_id = %row.id, %atom_id, error = %e,
                        "jobs_account_backfill: resolve_atoms_account_id failed"
                    );
                    continue;
                }
            };
        let account_id = match accounts.as_slice() {
            [one] => *one,
            [] => {
                // Expected: the atom was deleted after this job ran. Not
                // logged at warn — this is the anticipated steady-state
                // residue this backfill cannot close by construction.
                counts.dead_atom += 1;
                continue;
            }
            many => {
                counts.ambiguous_atom += 1;
                tracing::warn!(
                    job_id = %row.id, %atom_id, distinct_accounts = many.len(),
                    "jobs_account_backfill: atom_id resolved to more than one account — should be \
                     structurally impossible for a single atom"
                );
                continue;
            }
        };

        // ── Gate 3: write.
        if !opts.execute {
            counts.updated += 1;
            continue;
        }
        // `AND account_id IS NULL` repeats the candidate SELECT's own
        // predicate — defense in depth against a hypothetical concurrent
        // writer, not what makes a re-run safe (see this module's doc,
        // "Idempotency": a second run never re-selects this row at all).
        match sqlx::query("UPDATE jobs SET account_id = $1 WHERE id = $2 AND account_id IS NULL")
            .bind(account_id)
            .bind(row.id)
            .execute(pool)
            .await
        {
            Ok(r) if r.rows_affected() == 1 => counts.updated += 1,
            Ok(_) => {
                counts.raced += 1;
                tracing::warn!(
                    job_id = %row.id,
                    "jobs_account_backfill: UPDATE matched no row — account_id was set (or the row \
                     was removed) between this run's scan and its write"
                );
            }
            Err(e) => {
                counts.errors += 1;
                tracing::warn!(
                    job_id = %row.id, error = %e,
                    "jobs_account_backfill: UPDATE jobs.account_id failed"
                );
            }
        }
    }

    Ok(counts)
}

/// ONE machine-greppable line, printed to stderr per database (and once more
/// for the grand total) — the `migrate_tenants`/`judgement_backfill`
/// convention: a run whose only record is an exit code nobody reads is a run
/// nobody can audit afterwards.
pub fn summary_line(label: &str, opts: &BackfillOpts, c: &BackfillCounts) -> String {
    format!(
        "summary: backfill_unattributed_job_accounts db={label} mode={} scanned={} updated={} \
         malformed_payload={} dead_atom={} ambiguous_atom={} raced={} errors={} unresolvable={}",
        opts.mode(),
        c.scanned,
        c.updated,
        c.malformed_payload,
        c.dead_atom,
        c.ambiguous_atom,
        c.raced,
        c.errors,
        c.unresolvable(),
    )
}

/// The human-readable block printed to stdout — what an operator reads
/// before authorising `--execute`. Says "WOULD update" in a dry run so no
/// one mistakes a rehearsal's tally for work already done.
pub fn report_block(label: &str, opts: &BackfillOpts, c: &BackfillCounts) -> String {
    const LABEL_W: usize = 34;
    fn line(s: &mut String, label: &str, value: String) {
        s.push_str(&format!("    {label:<LABEL_W$}{value}\n"));
    }
    let verb = if opts.execute { "updated" } else { "WOULD update" };

    let mut s = format!("-- {label} ({mode}) --\n", mode = opts.mode());
    line(&mut s, "matched (scanned):", c.scanned.to_string());
    line(&mut s, &format!("{verb}:"), c.updated.to_string());
    line(&mut s, "unresolvable, total:", c.unresolvable().to_string());
    line(
        &mut s,
        "  malformed payload (no atom_id):",
        c.malformed_payload.to_string(),
    );
    line(&mut s, "  dead atom (deleted since):", c.dead_atom.to_string());
    line(&mut s, "  ambiguous atom (>1 account):", c.ambiguous_atom.to_string());
    if c.raced > 0 {
        line(&mut s, "raced (concurrent writer):", c.raced.to_string());
    }
    line(&mut s, "errors:", c.errors.to_string());
    if !opts.execute {
        s.push_str("  DRY RUN -- nothing was written. Re-run with --execute to apply.\n");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_atom_id_accepts_the_documented_shape_and_rejects_everything_else() {
        let id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        assert_eq!(
            parse_atom_id(&serde_json::json!({"atom_id": id.to_string()})),
            Some(id)
        );
        assert_eq!(parse_atom_id(&serde_json::json!({})), None, "missing key");
        assert_eq!(
            parse_atom_id(&serde_json::json!({"atom_id": "not-a-uuid"})),
            None,
            "unparseable value"
        );
        assert_eq!(
            parse_atom_id(&serde_json::json!({"atom_id": 12345})),
            None,
            "wrong JSON type"
        );
        assert_eq!(parse_atom_id(&serde_json::json!(null)), None, "null payload");
    }

    fn sample() -> BackfillCounts {
        BackfillCounts {
            scanned: 10,
            malformed_payload: 1,
            dead_atom: 2,
            ambiguous_atom: 0,
            updated: 6,
            raced: 0,
            errors: 1,
        }
    }

    #[test]
    fn counts_accounted_matches_scanned_when_errors_are_the_only_slack() {
        let c = sample();
        assert_eq!(c.accounted(), c.scanned);
        assert_eq!(c.unresolvable(), 3);
    }

    #[test]
    fn mode_string_matches_the_flag() {
        assert_eq!(BackfillOpts { execute: false }.mode(), "dry-run");
        assert_eq!(BackfillOpts { execute: true }.mode(), "execute");
    }

    #[test]
    fn summary_line_is_one_line_and_carries_every_counter() {
        let c = sample();
        for execute in [false, true] {
            let opts = BackfillOpts { execute };
            let line = summary_line("control", &opts, &c);
            assert!(line.starts_with("summary: backfill_unattributed_job_accounts db=control "));
            assert!(!line.contains('\n'), "summary must be greppable as ONE line");
            assert!(line.contains(if execute { "mode=execute" } else { "mode=dry-run" }));
            for field in [
                "scanned=10",
                "updated=6",
                "malformed_payload=1",
                "dead_atom=2",
                "ambiguous_atom=0",
                "raced=0",
                "errors=1",
                "unresolvable=3",
            ] {
                assert!(line.contains(field), "summary missing {field}: {line}");
            }
        }
    }

    #[test]
    fn report_block_says_would_update_only_in_a_dry_run() {
        let c = sample();
        let dry = report_block("control", &BackfillOpts { execute: false }, &c);
        assert!(dry.contains("WOULD update"));
        assert!(dry.contains("DRY RUN -- nothing was written"));

        let real = report_block("control", &BackfillOpts { execute: true }, &c);
        assert!(real.contains("updated:"));
        assert!(!real.contains("WOULD update"));
        assert!(!real.contains("DRY RUN"));
    }
}
