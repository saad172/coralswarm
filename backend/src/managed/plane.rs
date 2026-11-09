//! The one liability-plane resolver (COR-114): which content plane an
//! account's managed work is admitted, receipted and billed on.
//!
//! Data shape (control tables, none on the 0083 RLS roster):
//! - `accounts(id, clerk_org_id, is_org_account)`;
//! - `personal_orgs(clerk_org_id)`: a Clerk org minted for ONE user
//!   (control 0208). Its account keeps living on the consumer plane, with a
//!   `clerk_org_id` by design (`tenancy::gate::TokenKind::PersonalOrg`);
//! - `tenant_databases(clerk_org_id, account_id, status)`: an org's own
//!   tenant database. Only `active` is servable (control 0078).
//!
//! The rule, in [`decide`]:
//! - consumer: no `clerk_org_id` and no tenant database, or a
//!   `personal_orgs` org with no tenant database;
//! - org: an ordinary org account whose one tenant database is `active` and
//!   maps this org;
//! - anything else is [`PlaneError::Unresolvable`], never a guess.
//!
//! Every managed admission check used to inline its own version of this,
//! each accepting a consumer-plane account only when `clerk_org_id IS NULL`.
//! A personal org has an org id, so all of them refused it
//! (`requester_access_lost`), and billing reconciliation refused the same
//! shape. Admission (`managed::gate`), principal selection
//! (`managed::embedding::executor`, `managed::enrichment`,
//! `managed::interactive_ask`) and billing
//! (`workers::billing_derivation::reconcile_managed_tx`) now read this one
//! function, so they cannot disagree. `scripts/check_managed_front_door.sh`
//! refuses a new inline plane decision in those modules.
//!
//! [`PlanePermit`] is the proof a call went through the resolver: only
//! [`PlanePermit::admit`] makes one, and every managed fence, the funded
//! reservation's plane label and the gate itself take it.

use coralswarm_core::send_gate::{Deny, HoldCode};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::tenancy::TenantId;

/// Where an account's managed work is admitted and billed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LiabilityPlane {
    /// The shared consumer plane (the control database).
    Consumer,
    /// This org's own tenant database.
    Org(String),
}

impl LiabilityPlane {
    pub(crate) fn tenant(&self) -> TenantId {
        match self {
            LiabilityPlane::Consumer => TenantId::Consumer,
            LiabilityPlane::Org(org) => TenantId::Org(org.clone()),
        }
    }

    /// The `content_plane` label receipts and credit reservations carry.
    pub(crate) fn label(&self) -> String {
        match self {
            LiabilityPlane::Consumer => "consumer".to_owned(),
            LiabilityPlane::Org(org) => format!("org:{org}"),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PlaneError {
    #[error("no such account")]
    UnknownAccount,
    /// The account's rows do not name exactly one plane. Never resolved by
    /// guessing: a wrong plane bills the wrong ledger.
    #[error("liability plane unresolvable: {0}")]
    Unresolvable(&'static str),
    #[error(transparent)]
    Db(#[from] sqlx::Error),
}

/// The rows [`decide`] reads for one account.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AccountShape {
    clerk_org_id: Option<String>,
    is_org_account: bool,
    personal: bool,
    /// `(clerk_org_id, status)` of every tenant database mapped to this
    /// account.
    tenants: Vec<(String, String)>,
    /// The account's org id is mapped to a DIFFERENT account's tenant
    /// database.
    org_mapped_elsewhere: bool,
}

fn decide(shape: &AccountShape) -> Result<LiabilityPlane, PlaneError> {
    use PlaneError::Unresolvable;
    let Some(org) = &shape.clerk_org_id else {
        if !shape.tenants.is_empty() {
            return Err(Unresolvable("an account with no org id has a tenant database"));
        }
        if shape.is_org_account {
            return Err(Unresolvable("an org account has no org id"));
        }
        return Ok(LiabilityPlane::Consumer);
    };
    if shape.org_mapped_elsewhere {
        return Err(Unresolvable("the org's tenant database belongs to another account"));
    }
    if shape.personal {
        return if shape.tenants.is_empty() {
            Ok(LiabilityPlane::Consumer)
        } else {
            Err(Unresolvable("a personal org has a tenant database"))
        };
    }
    match shape.tenants.as_slice() {
        [] => Err(Unresolvable("an org account has no tenant database")),
        [(mapped, _)] if mapped == org && !shape.is_org_account => {
            Err(Unresolvable("an account with a tenant database is not an org account"))
        }
        [(mapped, status)] if mapped == org && status == "active" => Ok(LiabilityPlane::Org(org.clone())),
        [(mapped, _)] if mapped == org => Err(Unresolvable("the org's tenant database is not active")),
        _ => Err(Unresolvable("the account's tenant databases do not map its org")),
    }
}

/// The columns [`AccountShape`] is read from, for the account row `a`.
macro_rules! shape_columns {
    () => {
        "a.clerk_org_id,a.is_org_account, \
         EXISTS(SELECT 1 FROM personal_orgs p WHERE p.clerk_org_id=a.clerk_org_id), \
         ARRAY(SELECT t.clerk_org_id FROM tenant_databases t WHERE t.account_id=a.id ORDER BY t.clerk_org_id), \
         ARRAY(SELECT t.status FROM tenant_databases t WHERE t.account_id=a.id ORDER BY t.clerk_org_id), \
         EXISTS(SELECT 1 FROM tenant_databases t WHERE t.clerk_org_id=a.clerk_org_id AND t.account_id<>a.id)"
    };
}
const RESOLVE_SQL: &str = concat!("SELECT ", shape_columns!(), " FROM accounts a WHERE a.id=$1");
const RESOLVE_LOCKED_SQL: &str = concat!("SELECT ", shape_columns!(), " FROM accounts a WHERE a.id=$1 FOR SHARE OF a");
const RESOLVE_ALL_SQL: &str = concat!("SELECT a.id,", shape_columns!(), " FROM accounts a ORDER BY a.id");

type ShapeRow = (Option<String>, bool, bool, Vec<String>, Vec<String>, bool);

fn shape_of(row: ShapeRow) -> AccountShape {
    let (clerk_org_id, is_org_account, personal, orgs, statuses, org_mapped_elsewhere) = row;
    AccountShape {
        clerk_org_id,
        is_org_account,
        personal,
        tenants: orgs.into_iter().zip(statuses).collect(),
        org_mapped_elsewhere,
    }
}

/// Resolve `account`'s liability plane. `lock` takes the account row
/// `FOR SHARE`, so a fence that re-resolves under its attempt's locks sees
/// no plane change (an org adoption rewrites `accounts.clerk_org_id`) until
/// it commits.
pub(crate) async fn resolve_liability_plane(
    conn: &mut PgConnection,
    account: Uuid,
    lock: bool,
) -> Result<LiabilityPlane, PlaneError> {
    let sql = if lock { RESOLVE_LOCKED_SQL } else { RESOLVE_SQL };
    let row: Option<ShapeRow> = sqlx::query_as(sql).bind(account).fetch_optional(conn).await?;
    decide(&shape_of(row.ok_or(PlaneError::UnknownAccount)?))
}

/// Every account [`decide`] places on the consumer plane, in id order: the
/// accounts a consumer-plane sweep may enumerate. Unresolvable accounts are
/// left out (and logged), never guessed onto a plane.
pub(crate) async fn consumer_plane_accounts(conn: &mut PgConnection) -> Result<Vec<Uuid>, sqlx::Error> {
    let rows: Vec<(Uuid, Option<String>, bool, bool, Vec<String>, Vec<String>, bool)> =
        sqlx::query_as(RESOLVE_ALL_SQL).fetch_all(conn).await?;
    let mut consumer = Vec::new();
    for (account, org, is_org, personal, orgs, statuses, elsewhere) in rows {
        match decide(&shape_of((org, is_org, personal, orgs, statuses, elsewhere))) {
            Ok(LiabilityPlane::Consumer) => consumer.push(account),
            Ok(LiabilityPlane::Org(_)) => {}
            Err(why) => tracing::warn!(%account, %why, "consumer-plane enumeration: account plane unresolvable; skipped"),
        }
    }
    Ok(consumer)
}

/// Proof that `account` resolved to the plane the managed work runs on.
/// Its fields are private and [`PlanePermit::admit`] is its only
/// constructor, so a managed fence, gate or funded reservation cannot be
/// reached without the resolver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlanePermit {
    account: Uuid,
    plane: LiabilityPlane,
    tenant: TenantId,
}

impl PlanePermit {
    /// The front door's plane check: resolve `account` and require the
    /// plane the worker or request runs on (`tenant`).
    ///
    /// - an account that no longer exists, or that now lives on another
    ///   plane, is `requester_access_lost`, as before;
    /// - an account whose plane cannot be resolved is held
    ///   `principal_unavailable` (no member of it can be charged) and
    ///   alerts. A dedicated code would have to join the hold-reason CHECK
    ///   lists first (control 0346, tenant 0178).
    pub(crate) async fn admit(
        conn: &mut PgConnection,
        account: Uuid,
        tenant: &TenantId,
    ) -> Result<Self, Deny> {
        match resolve_liability_plane(conn, account, true).await {
            Ok(plane) if plane.tenant() == *tenant => Ok(Self {
                account,
                tenant: plane.tenant(),
                plane,
            }),
            Ok(_) | Err(PlaneError::UnknownAccount) => Err(Deny::Hold(HoldCode::RequesterAccessLost)),
            Err(PlaneError::Unresolvable(why)) => {
                tracing::warn!(alert = true, %account, ?tenant, why,
                    "managed admission: account liability plane unresolvable; held");
                Err(Deny::Hold(HoldCode::PrincipalUnavailable))
            }
            Err(PlaneError::Db(error)) => {
                tracing::warn!(%error, %account, "managed admission: liability plane unavailable");
                Err(Deny::Unavailable)
            }
        }
    }

    /// Re-resolve under the caller's locks (`FOR SHARE` on the account) and
    /// require the same account and plane: the fresh fence every attempt
    /// runs before it reserves.
    pub(crate) async fn confirm_locked(&self, conn: &mut PgConnection, account: Uuid) -> Result<(), Deny> {
        if account != self.account {
            return Err(Deny::Unavailable);
        }
        Self::admit(conn, account, &self.tenant).await.map(|_| ())
    }

    pub(crate) fn account(&self) -> Uuid {
        self.account
    }

    pub(crate) fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    pub(crate) fn plane(&self) -> &LiabilityPlane {
        &self.plane
    }

    /// The `content_plane` label receipts and credit reservations carry.
    pub(crate) fn label(&self) -> String {
        self.plane.label()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(org: Option<&str>, is_org: bool, personal: bool, tenants: &[(&str, &str)]) -> AccountShape {
        AccountShape {
            clerk_org_id: org.map(str::to_owned),
            is_org_account: is_org,
            personal,
            tenants: tenants.iter().map(|(o, s)| (o.to_string(), s.to_string())).collect(),
            org_mapped_elsewhere: false,
        }
    }

    #[test]
    fn a_plain_personal_account_is_consumer() {
        assert_eq!(decide(&shape(None, false, false, &[])).unwrap(), LiabilityPlane::Consumer);
    }

    /// The COR-114 shape: prod account 78d0d2ba (`is_org_account` true, a
    /// `personal_orgs` row, no tenant database).
    #[test]
    fn a_personal_org_is_consumer_whatever_its_org_flag() {
        assert_eq!(decide(&shape(Some("org_p"), true, true, &[])).unwrap(), LiabilityPlane::Consumer);
        assert_eq!(decide(&shape(Some("org_p"), false, true, &[])).unwrap(), LiabilityPlane::Consumer);
    }

    #[test]
    fn an_org_with_its_active_tenant_database_is_that_org() {
        assert_eq!(
            decide(&shape(Some("org_a"), true, false, &[("org_a", "active")])).unwrap(),
            LiabilityPlane::Org("org_a".into())
        );
    }

    #[test]
    fn every_ambiguous_shape_is_refused_not_guessed() {
        let refused = [
            shape(Some("org_a"), true, false, &[]),
            shape(Some("org_a"), true, false, &[("org_a", "provisioning")]),
            shape(Some("org_a"), true, false, &[("org_a", "suspended")]),
            shape(Some("org_a"), true, false, &[("org_b", "active")]),
            shape(Some("org_a"), true, false, &[("org_a", "active"), ("org_b", "active")]),
            shape(Some("org_a"), false, false, &[("org_a", "active")]),
            shape(Some("org_p"), true, true, &[("org_p", "active")]),
            shape(None, false, false, &[("org_a", "active")]),
            shape(None, true, false, &[]),
            AccountShape { org_mapped_elsewhere: true, ..shape(Some("org_a"), true, false, &[]) },
            AccountShape { org_mapped_elsewhere: true, ..shape(Some("org_p"), true, true, &[]) },
        ];
        for s in refused {
            assert!(matches!(decide(&s), Err(PlaneError::Unresolvable(_))), "{s:?} must be refused");
        }
    }

    #[test]
    fn labels_match_the_receipt_and_reservation_planes() {
        assert_eq!(LiabilityPlane::Consumer.label(), "consumer");
        assert_eq!(LiabilityPlane::Org("org_a".into()).label(), "org:org_a");
        assert_eq!(LiabilityPlane::Org("org_a".into()).tenant(), TenantId::Org("org_a".into()));
    }
}
