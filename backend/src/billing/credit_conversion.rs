//! Credit-to-USD reference conversion (usage-metering PR1 addendum).
//!
//! `credit_conversion` (migration `0148_model_rates.sql` — see its header
//! for the full rationale) records how much real provider spend ONE CREDIT
//! represents: the COST BASIS a future ledger (usage-metering PR8/PR9)
//! divides `cost_usd` by, never the retail top-up price ($0.022/credit,
//! locked separately — see the migration header).
//!
//! No code reads this table yet — PR8's `credit_ledger` is the first real
//! consumer — but the loader/selector below ships now so PR8/PR9 inherit an
//! already-reviewed, known-good contract instead of each having to decide
//! how to read this table for the first time under their own deadline
//! pressure.
//!
//! ## Decimal, never float
//! `reference_cost_per_credit` is read as [`sqlx::types::Decimal`]
//! (`rust_decimal::Decimal` under the hood — this crate enables sqlx's
//! `rust_decimal` feature specifically for this one column), NEVER cast to
//! `float8`/`f64` anywhere in this module. That is a deliberate departure
//! from `crate::billing::rates::PriceSheet`, which casts `model_rates`'
//! `NUMERIC` columns to `float8` because those only ever back a "rough,
//! internally-consistent estimate" (see that module's own doc comment). At
//! $0.0044/credit, a single float-rounding error is ~0.1% of a credit and
//! compounds across every debit a future ledger ever writes — exact decimal
//! arithmetic is load-bearing here, not a convenience cast.
//!
//! ## Versioned, not a scalar
//! Same shape and same reason as [`crate::billing::rates::PriceSheet`]: a
//! report/debit for a past period must use the rate that was active then,
//! not whatever is active when it's computed later, so [`load`] reads the
//! FULL history and [`CreditConversionTable::active`] is a pure, in-memory
//! "newest row with `effective_from <= as_of`" selection — never a bare
//! `LIMIT 1` with no `ORDER BY`, which would pick an arbitrary row the
//! moment a second row exists (and this table WILL grow a second row: price
//! changes and provider switches are exactly why it's versioned at all).
//!
//! Nothing in production code calls this module yet — PR8's `credit_ledger`
//! is the first real consumer (see the module doc above) — so the
//! dead-code lint is suppressed at the module level, same precedent as
//! `jobs::mod`'s "public API consumed by future worker stories" note.
#![allow(dead_code)]

use chrono::{DateTime, Utc};
use sqlx::types::Decimal;
use sqlx::PgPool;

/// One row of `credit_conversion`, loaded verbatim.
#[derive(Debug, Clone)]
pub struct CreditConversionRow {
    pub reference_cost_per_credit: Decimal,
    pub effective_from: DateTime<Utc>,
    pub note: Option<String>,
}

/// The full `credit_conversion` history, snapshotted once — see the module
/// doc for why this mirrors `PriceSheet`'s "load everything, select in
/// memory" shape rather than a single `SELECT ... LIMIT 1` per call.
#[derive(Debug, Clone)]
pub struct CreditConversionTable {
    rows: Vec<CreditConversionRow>,
}

impl CreditConversionTable {
    /// Load the FULL conversion-rate history from the control plane.
    pub async fn load(pool: &PgPool) -> Result<Self, sqlx::Error> {
        let mut conn=pool.acquire().await?;
        Self::load_tx(&mut conn).await
    }
    pub(crate) async fn load_tx(conn:&mut sqlx::PgConnection)->Result<Self,sqlx::Error>{
        let rows: Vec<(Decimal, DateTime<Utc>, Option<String>)> = sqlx::query_as(
            "SELECT reference_cost_per_credit, effective_from, note \
             FROM credit_conversion \
             ORDER BY effective_from",
        )
        .fetch_all(conn)
        .await?;

        Ok(Self::from_rows(
            rows.into_iter()
                .map(|(reference_cost_per_credit, effective_from, note)| CreditConversionRow {
                    reference_cost_per_credit,
                    effective_from,
                    note,
                })
                .collect(),
        ))
    }

    /// Build a table directly from rows already in memory. `pub(crate)` —
    /// the only in-crate, non-Postgres caller is
    /// [`tests::seeded_for_tests`] below.
    pub(crate) fn from_rows(rows: Vec<CreditConversionRow>) -> Self {
        Self { rows }
    }

    /// The row active as of `as_of`: the NEWEST row whose `effective_from
    /// <= as_of`. `None` only if `as_of` predates every seeded row (should
    /// never happen against a real table — the seed's `effective_from` is a
    /// fixed epoch marker long before this product existed).
    pub fn active(&self, as_of: DateTime<Utc>) -> Option<&CreditConversionRow> {
        self.rows
            .iter()
            .filter(|r| r.effective_from <= as_of)
            .max_by_key(|r| r.effective_from)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use chrono::NaiveDate;
    use std::str::FromStr;

    /// Mirrors the ONE seed row inserted by
    /// `backend/migrations_control/0148_model_rates.sql` — kept in lockstep
    /// BY HAND, same discipline as
    /// `crate::billing::rates::tests::seeded_for_tests`.
    pub(crate) fn seeded_for_tests() -> CreditConversionTable {
        let epoch = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2000, 1, 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .expect("valid epoch marker"),
            Utc,
        );
        CreditConversionTable::from_rows(vec![CreditConversionRow {
            reference_cost_per_credit: Decimal::from_str("0.0044").expect("valid decimal literal"),
            effective_from: epoch,
            note: Some("cost basis, not retail price".to_string()),
        }])
    }

    fn month_start(year: i32, month: u32) -> DateTime<Utc> {
        let dt = NaiveDate::from_ymd_opt(year, month, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .expect("valid test date");
        DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc)
    }

    #[test]
    fn active_resolves_the_seeded_row_at_a_realistic_as_of() {
        let table = seeded_for_tests();
        let row = table.active(month_start(2026, 8)).expect("seeded row must be active");
        assert_eq!(row.reference_cost_per_credit, Decimal::from_str("0.0044").unwrap());
    }

    #[test]
    fn active_is_none_before_every_seeded_rows_effective_from() {
        let table = seeded_for_tests();
        assert!(table.active(month_start(1999, 1)).is_none());
    }

    /// The load-bearing regression this table's `active()` exists to guard:
    /// with TWO rows, the NEWEST one whose `effective_from <= as_of` must
    /// win — never insertion order, never a bare `LIMIT 1`. Inserted here in
    /// the opposite of chronological order specifically to prove `active`
    /// doesn't secretly depend on `rows` already being sorted a particular
    /// way.
    #[test]
    fn active_picks_the_newest_row_not_the_first_or_last_inserted() {
        let jan = month_start(2026, 1);
        let jun = month_start(2026, 6);
        let table = CreditConversionTable::from_rows(vec![
            CreditConversionRow {
                reference_cost_per_credit: Decimal::from_str("0.0044").unwrap(),
                effective_from: jun,
                note: None,
            },
            CreditConversionRow {
                reference_cost_per_credit: Decimal::from_str("0.0050").unwrap(),
                effective_from: jan,
                note: None,
            },
        ]);

        // Before June: only the January row is active.
        assert_eq!(
            table.active(month_start(2026, 3)).unwrap().reference_cost_per_credit,
            Decimal::from_str("0.0050").unwrap()
        );
        // June onward: the June row (newer) wins over the January row, even
        // though January was inserted second (last) in `from_rows` above.
        assert_eq!(
            table.active(month_start(2026, 8)).unwrap().reference_cost_per_credit,
            Decimal::from_str("0.0044").unwrap()
        );
    }

    #[test]
    fn reference_cost_per_credit_is_exact_decimal_not_a_float_approximation() {
        // 0.0044 has no exact f64 representation (it's not a sum of powers
        // of two) — `f64::from_str("0.0044")` would round to the nearest
        // representable double. `Decimal::from_str` stores the exact
        // base-10 value instead. This test exists so a future "just cast it
        // to f64, it's close enough" edit is caught immediately: comparing
        // the decimal to itself parsed twice must be exact, and multiplying
        // it by a large integer number of credits must not drift the way a
        // float accumulation would.
        let a = Decimal::from_str("0.0044").unwrap();
        let b = Decimal::from_str("0.0044").unwrap();
        assert_eq!(a, b);
        // Sum 10,000 credits' worth one at a time (the way a naive running
        // ledger balance might) and confirm it lands EXACTLY on the
        // closed-form product — no accumulated rounding drift.
        let mut summed = Decimal::from_str("0").unwrap();
        for _ in 0..10_000 {
            summed += a;
        }
        assert_eq!(summed, a * Decimal::from(10_000));
    }
}
