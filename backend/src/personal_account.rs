//! The caller's personal account-of-one, resolved against the control plane.

use uuid::Uuid;

/// The caller's own personal account-of-one, or `None` if they have none.
///
/// Deliberately a **read**, not `clerk_provision::ensure_personal_account`'s
/// find-or-create: callers on hot read paths must never provision an account
/// as a side effect.
///
/// This IS `ensure_personal_account`'s own find step, not a copy of it:
/// `ensure_personal_account` calls this exact function before ever
/// considering an INSERT, so the two structurally cannot answer "which row is
/// the personal account" differently — there is only one query to disagree
/// with. See that function's own doc for why the personal-org arm (below)
/// has to be tried before the plain `clerk_org_id IS NULL` arm: after
/// `adopt_personal_org_account` relabels a user's account, they have NO
/// `clerk_org_id IS NULL` row left at all, so a query that only knew the old
/// shape would silently answer "no personal account" for every adopted user.
///
/// Both arms are collapsed into ONE round trip via `UNION ALL` + a rank
/// column, rather than run as two separate queries with the first arm's
/// result short-circuiting the second in application code — that shape is
/// what let the two copies drift apart in the first place. The `ORDER BY
/// created_at` tie-break inside each arm is preserved.
pub async fn personal_account_id(
    pool: &sqlx::PgPool,
    user_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    // `me` is a one-row relation carrying the user id, so the shared
    // correlated subquery below can be the ONE copy of the predicate — see
    // `PERSONAL_ACCOUNT_SUBQUERY`.
    sqlx::query_scalar::<_, Option<Uuid>>(&format!(
        "SELECT ({PERSONAL_ACCOUNT_SUBQUERY}) FROM (SELECT $1::uuid AS id) me"
    ))
    .bind(user_id)
    .fetch_one(crate::tenancy::scope::request_read(pool))
    .await
}

/// The personal-account predicate [`personal_account_id`] runs, as a
/// correlated SCALAR subquery: it expects an outer relation aliased `me` with
/// a `uuid` column `id` (the user), and yields that user's personal
/// `accounts.id` or NULL.
///
/// `pub(crate)` so `middleware::auth`'s identity prefetch (the one
/// control-plane read every authenticated request makes) can embed it
/// against the `users` row it has just joined, instead of paying a second
/// round trip for `personal_account_id` — and WITHOUT a second copy of the
/// predicate, which is the drift [`personal_account_id`]'s doc warns
/// against. Any change to the arms, their order, or the tie-break happens
/// here, once, and both callers see it.
pub(crate) const PERSONAL_ACCOUNT_SUBQUERY: &str = "SELECT c.id FROM ( \
         SELECT a.id, 1 AS rank, a.created_at FROM accounts a \
             JOIN personal_orgs p ON p.clerk_org_id = a.clerk_org_id \
             JOIN users u ON u.clerk_user_id = p.clerk_user_id \
             WHERE u.id = me.id AND a.owner_user_id = me.id \
         UNION ALL \
         SELECT a.id, 2 AS rank, a.created_at FROM accounts a \
             WHERE a.owner_user_id = me.id AND a.clerk_org_id IS NULL \
     ) c \
     ORDER BY c.rank, c.created_at \
     LIMIT 1";

#[cfg(test)]
mod pg_tests {
    use super::*;
    use sqlx::PgPool;

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    /// Seed a user + a personal account, returning `(user_id, account_id)`.
    async fn seed_account(pool: &PgPool, tag: &str) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users (email) VALUES ($1) RETURNING id",
        )
        .bind(format!("personal-account-{tag}-{}@example.test", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .expect("seed user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id",
        )
        .bind(user_id)
        .fetch_one(pool)
        .await
        .expect("seed account");
        (user_id, account_id)
    }

    // ── `personal_account_id` must survive `adopt_personal_org_account` ────

    /// Seed a user in the shape `bin/adopt_personal_org_account` leaves an
    /// ADOPTED user in: their ONLY `accounts` row carries a `clerk_org_id`,
    /// and that id has a row in `personal_orgs` naming this user as the one
    /// it stands for. There is no `clerk_org_id IS NULL` row for this user at
    /// all — the exact shape the old `clerk_org_id IS NULL` query could never
    /// find.
    async fn seed_adopted_personal_org_user(pool: &PgPool, tag: &str) -> (Uuid, Uuid) {
        let clerk_user_id = format!("user_{tag}_{}", Uuid::new_v4());
        let clerk_org_id = format!("org_{tag}_{}", Uuid::new_v4());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
                .bind(format!("personal-account-{tag}-{}@example.test", Uuid::new_v4()))
                .bind(&clerk_user_id)
                .fetch_one(pool)
                .await
                .expect("seed user");
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(&clerk_org_id)
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("seed personal_orgs row");
        // `is_org_account` is not set explicitly: the trigger at migration
        // 0169:342 derives it from `clerk_org_id IS NOT NULL` regardless of
        // what is passed here — see this crate's CLAUDE.md note on
        // `is_org_account` not being an independent signal.
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
                .bind(user_id)
                .bind(&clerk_org_id)
                .fetch_one(pool)
                .await
                .expect("seed adopted (relabeled) account");
        (user_id, account_id)
    }

    /// An adopted user's only account
    /// carries a `clerk_org_id` — this must still resolve as "their personal
    /// account", both directly through [`personal_account_id`] and through
    /// [`crate::clerk_provision::ensure_personal_account`]'s find step, since
    /// the latter now calls the former for exactly this reason.
    #[tokio::test]
    async fn personal_account_id_finds_an_adopted_users_relabeled_account_pg() {
        let Some(pool) = pg_test_pool("personal_account_id_finds_adopted_pg").await else {
            return;
        };
        let (user_id, account_id) = seed_adopted_personal_org_user(&pool, "adopted").await;

        let found = personal_account_id(&pool, user_id).await.expect("query");
        assert_eq!(
            found,
            Some(account_id),
            "an adopted user's relabeled (clerk_org_id-carrying) account must still answer \
             to personal_account_id — this is what the `clerk_org_id IS NULL` query alone \
             cannot find"
        );
    }

    /// A user who holds BOTH a personal account-of-one AND a real
    /// organization account (Model B) must resolve to the PERSONAL one, never
    /// the org one — the personal-org arm's `personal_orgs` join must not
    /// accidentally match an ordinary org that happens to share an owner.
    #[tokio::test]
    async fn personal_account_id_does_not_return_a_real_org_account_pg() {
        let Some(pool) = pg_test_pool("personal_account_id_ignores_real_org_pg").await else {
            return;
        };
        let (user_id, personal_account) = seed_account(&pool, "real-org-member").await;
        let real_org_id = format!("org_real_{}", Uuid::new_v4());
        // A REAL org: `clerk_org_id` is set, and — critically — has NO row in
        // `personal_orgs`, unlike the adopted shape above.
        sqlx::query("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2)")
            .bind(user_id)
            .bind(&real_org_id)
            .execute(&pool)
            .await
            .expect("seed real org account");

        let found = personal_account_id(&pool, user_id).await.expect("query");
        assert_eq!(
            found,
            Some(personal_account),
            "a real org account must never be returned as the caller's personal account"
        );
    }

    /// A personal org's own account (control migration 0208 `personal_orgs`)
    /// — the end state `bin/adopt_personal_org_account.rs` leaves a user in.
    /// Returns `(user_id, account_id, clerk_org_id)`.
    async fn seed_personal_org_account(pool: &PgPool, tag: &str) -> (Uuid, Uuid, String) {
        let clerk_user_id = format!("clerk_user_{}", Uuid::new_v4().simple());
        let clerk_org_id = format!("org_{}", Uuid::new_v4().simple());
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(format!("personal-account-{tag}-{}@example.test", Uuid::new_v4()))
        .bind(&clerk_user_id)
        .fetch_one(pool)
        .await
        .expect("seed user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(&clerk_org_id)
        .fetch_one(pool)
        .await
        .expect("seed personal-org account");
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(&clerk_org_id)
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("seed personal_orgs row");
        (user_id, account_id, clerk_org_id)
    }

    /// An adopted user has NO `clerk_org_id IS NULL` account (adoption
    /// relabels their only personal account with a `clerk_org_id`), so the
    /// `personal_orgs` arm is the only one that can find it.
    #[tokio::test]
    async fn personal_account_id_resolves_an_adopted_personal_orgs_account_pg() {
        let Some(pool) =
            pg_test_pool("personal_account_id_resolves_an_adopted_personal_orgs_account_pg").await
        else {
            return;
        };
        let (user_id, account_id, clerk_org_id) = seed_personal_org_account(&pool, "adopted").await;

        let resolved = personal_account_id(&pool, user_id).await.expect("query must succeed");

        sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
        sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
        sqlx::query("DELETE FROM personal_orgs WHERE clerk_org_id = $1")
            .bind(&clerk_org_id)
            .execute(&pool)
            .await
            .ok();

        assert_eq!(
            resolved,
            Some(account_id),
            "an adopted user's personal_orgs-mapped account must resolve, same as a plain \
             clerk_org_id-IS-NULL personal account"
        );
    }
}
