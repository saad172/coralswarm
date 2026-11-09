//! Who may read a graph from a cached result (#1524 blocker 1).
//!
//! One boundary for every surface that serves a graph's facts from a cache
//! instead of recomputing them under the reader's own permissions. The
//! Library accelerated reader is the first caller; Ask, MCP and the
//! history-floor gaps of #1545 can route through the same boundary later.
//!
//! # The access class
//!
//! Production requests and workers connect as the table owner, which bypasses
//! RLS (tenancy/scope.rs, 0159). Member access is enforced in code: the
//! request path authorizes the graph, and `library_evidence_shareable` (0185)
//! requires the graph to be visible to the actor, the account to be the
//! graph's, and no history floor. Under today's policy inventory every reader
//! that passes those checks sees exactly the same rows of the graph (design
//! `docs/plans/library-members-busy-design/design.md` §1.3, §2.1). So a cache
//! is not tied to the role that computed it, but to the reader's *class*:
//!
//! ```text
//! {schema_version: 2, account_id, history_floor, correction_visible, policy_digest}
//! ```
//!
//! computed in SQL by `library_evidence_class(g)` (tenant 0206 / control
//! 0391). The cache stores the class it was certified under; a reader is
//! admitted when its own class, evaluated in the read's snapshot, is equal.
//! `policy_digest` is `library_access_contract_digest()`, a hash of the
//! policies, RLS flags and function bodies the equivalence argument rests
//! on. It must also equal the reviewed constant compiled in here, so a
//! migration that changes that contract sends every read to the exact
//! uncached path until a human re-reviews the argument.
//!
//! # In-process, in-snapshot
//!
//! Everything here is SQL text the caller embeds in its own statement, plus
//! pure Rust over values that statement returned. Admission must be decided
//! in the same REPEATABLE READ snapshot as the cached rows it admits, or a
//! revocation could interleave between the check and the read. There is no
//! separate service and no extra round trip.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

/// `schema_version` of the class. Schema 1 is 0185's role-bearing
/// `library_evidence_context` fingerprint, which no class ever equals.
pub const CLASS_SCHEMA_VERSION: i32 = 2;

/// The reviewed `library_access_contract_digest()` of a database migrated
/// through tenant 0206 / control 0391 (identical on PG16 and PG17). The class
/// equivalence was reviewed against exactly the objects this digest covers.
/// Change these only after re-reviewing that argument for the migration that
/// moved them; `scripts/check_library_access_contract.sh` recomputes both on
/// freshly migrated databases.
pub const TENANT_CONTRACT_DIGEST: &str = "f952bc469ad945a62db82f246aae4f58378e0869fbd90cd774ec032e6f115d48";
pub const CONTROL_CONTRACT_DIGEST: &str = "fdbdb7fdaf312dfc7bec22226b9ef957c64a288679dcd9ef7c291db7400886f5";

/// Fallback reason: the reader's RLS contract is not the reviewed one, or a
/// cache was certified under another contract.
pub const CONTRACT_CHANGED: &str = "access_contract_changed";
/// Fallback reason: the cache was certified for another access class (or is
/// still at schema 1). The wire name predates the class.
pub const CLASS_MISMATCH: &str = "context_mismatch";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plane { Tenant, Control }
impl Plane {
    pub fn reviewed_digest(self) -> &'static str {
        match self { Self::Tenant => TENANT_CONTRACT_DIGEST, Self::Control => CONTROL_CONTRACT_DIGEST }
    }
}

/// A reader's (or a cache's) access class for one graph.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccessClass {
    pub schema_version: i32,
    pub account_id: Option<String>,
    /// UTC, without zone, as `library_evidence_class` renders it.
    pub history_floor: Option<String>,
    pub correction_visible: bool,
    pub policy_digest: String,
}
impl AccessClass {
    /// A stored context at this schema, or None (schema 1, malformed, absent).
    pub fn from_stored(value: Option<&Value>) -> Option<Self> {
        value.and_then(|v| serde_json::from_value::<Self>(v.clone()).ok()).filter(|c| c.schema_version == CLASS_SCHEMA_VERSION)
    }
}

/// The admission decision for one graph.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict { Admitted, ContractChanged, ClassMismatch }
impl Verdict {
    pub fn fallback_reason(self) -> Option<&'static str> {
        match self { Self::Admitted => None, Self::ContractChanged => Some(CONTRACT_CHANGED), Self::ClassMismatch => Some(CLASS_MISMATCH) }
    }
    /// The verdict a fallback reason names, for callers that map reasons.
    pub fn from_reason(reason: &str) -> Option<Self> {
        match reason { CONTRACT_CHANGED => Some(Self::ContractChanged), CLASS_MISMATCH => Some(Self::ClassMismatch), _ => None }
    }
}

/// The Rust statement of the rule [`sql::contract_changed`] and
/// [`sql::class_mismatch`] embed, in the same order: the reader's digest must
/// be this plane's reviewed one, every class-2 cache must carry the reader's
/// digest, and every cache must be exactly the reader's class.
pub fn verdict(plane: Plane, reader: &AccessClass, stored: &[Option<&Value>]) -> Verdict {
    if reader.policy_digest != plane.reviewed_digest()
        || stored.iter().filter_map(|s| AccessClass::from_stored(*s)).any(|s| s.policy_digest != reader.policy_digest)
    {
        return Verdict::ContractChanged;
    }
    if reader.schema_version != CLASS_SCHEMA_VERSION || stored.iter().any(|s| AccessClass::from_stored(*s).as_ref() != Some(reader)) {
        return Verdict::ClassMismatch;
    }
    Verdict::Admitted
}

/// SQL fragments for a caller's admission statement. Arguments are SQL
/// expressions the caller already has in scope; nothing is interpolated from
/// a request.
pub mod sql {
    /// jsonb: the calling reader's class for graph expression `graph`.
    pub fn reader_class(graph: &str) -> String { format!("public.library_evidence_class({graph})") }

    /// text: this plane's reviewed digest. The plane test is 0204's run-time
    /// idiom: the tenant windows function exists only on tenant databases.
    pub fn reviewed_digest() -> String {
        format!("(CASE WHEN to_regprocedure('public.tenant_rls_visible_graph_windows()') IS NOT NULL THEN '{}' ELSE '{}' END)",
            super::TENANT_CONTRACT_DIGEST, super::CONTROL_CONTRACT_DIGEST)
    }

    /// boolean: [`super::Verdict::ContractChanged`] for one row with the
    /// reader class `reader` and stored contexts `stored` (jsonb expressions).
    pub fn contract_changed(reader: &str, stored: &[&str]) -> String {
        let mut arms = vec![format!("{reader}->>'policy_digest' IS DISTINCT FROM {}", reviewed_digest())];
        arms.extend(stored.iter().map(|s| format!("({s}->>'schema_version'='{v}' AND {s}->>'policy_digest' IS DISTINCT FROM {reader}->>'policy_digest')", v = super::CLASS_SCHEMA_VERSION)));
        format!("({})", arms.join("\n   OR "))
    }

    /// boolean: [`super::Verdict::ClassMismatch`]: a stored context is not
    /// exactly the reader's class (jsonb equality, so key order is moot).
    pub fn class_mismatch(reader: &str, stored: &[&str]) -> String {
        format!("({})", stored.iter().map(|s| format!("{s} IS DISTINCT FROM {reader}")).collect::<Vec<_>>().join(" OR "))
    }
}

/// The class a worker certifies graph `graph` under, in the worker's own
/// transaction (its `app.*` settings already applied). First upgrades, in
/// place, any schema-1 context the class makes safe to keep (see
/// [`restamp_schema_one`]), so certified graphs need no rebuild.
pub async fn certify(conn: &mut PgConnection, graph: Uuid) -> Result<Value, sqlx::Error> {
    let class: Value = sqlx::query_scalar("SELECT public.library_evidence_class($1)").bind(graph).fetch_one(&mut *conn).await?;
    restamp_schema_one(conn, graph, &class).await?;
    Ok(class)
}

/// SQL boolean: stored context column `column` is a schema-1 context that may
/// be re-stamped to class `$2`: same account, corrections visible, and a
/// class with no history floor. The facts never depended on the role or the
/// RLS flags the old fingerprint also carried: a worker sees every row of the
/// graph either way (the owner with RLS off, the app role as the system
/// actor). Anything else keeps schema 1, which no class equals, so it
/// rebuilds.
fn restampable(column: &str) -> String {
    format!("({column}->>'schema_version'='1' AND {column}->'account_id'=$2->'account_id' AND {column}->'correction_visible'='true'::jsonb \
AND $2->>'schema_version'='{v}' AND $2->'correction_visible'='true'::jsonb AND $2->'history_floor'='null'::jsonb)", v = CLASS_SCHEMA_VERSION)
}

/// One-shot upgrade of facts certified under 0185's schema-1 context. Rows
/// already at schema 2 are never touched, so on a steady graph each
/// statement matches nothing. An unfinished listing generation carries its
/// own input context; without the last statement it would be discarded and
/// restarted (one item, but needlessly).
async fn restamp_schema_one(conn: &mut PgConnection, graph: Uuid, class: &Value) -> Result<(), sqlx::Error> {
    let (node, backfill, input) = (restampable("computed_context"), restampable("backfill_context"), restampable("input_context"));
    sqlx::query(&format!("UPDATE library_evidence_state SET computed_context=CASE WHEN {node} THEN $2 ELSE computed_context END,\
backfill_context=CASE WHEN {backfill} THEN $2 ELSE backfill_context END WHERE graph_id=$1 AND ({node} OR {backfill})"))
        .bind(graph).bind(class).execute(&mut *conn).await?;
    sqlx::query(&format!("UPDATE library_listing_state SET computed_context=$2 WHERE graph_id=$1 AND {node}"))
        .bind(graph).bind(class).execute(&mut *conn).await?;
    sqlx::query(&format!("UPDATE library_listing_generations SET input_context=$2 WHERE graph_id=$1 AND phase<>'discard' AND {input}"))
        .bind(graph).bind(class).execute(conn).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn class(digest: &str) -> Value {
        json!({"schema_version":2,"account_id":"a","history_floor":null,"correction_visible":true,"policy_digest":digest})
    }
    #[test]
    fn verdict_requires_the_reviewed_digest_then_exact_class_equality() {
        let reader: AccessClass = serde_json::from_value(class(TENANT_CONTRACT_DIGEST)).unwrap();
        let same = class(TENANT_CONTRACT_DIGEST);
        assert_eq!(verdict(Plane::Tenant, &reader, &[Some(&same), Some(&same)]), Verdict::Admitted);
        // The control digest is not this plane's reviewed contract.
        assert_eq!(verdict(Plane::Control, &reader, &[Some(&same)]), Verdict::ContractChanged);
        let older = class(&"0".repeat(64));
        assert_eq!(verdict(Plane::Tenant, &reader, &[Some(&same), Some(&older)]), Verdict::ContractChanged);
        let v1 = json!({"schema_version":1,"role":"owner","account_id":"a","row_security":[false,false,false,false,false,false,false,false],"correction_visible":true});
        assert_eq!(verdict(Plane::Tenant, &reader, &[Some(&same), Some(&v1)]), Verdict::ClassMismatch);
        assert_eq!(verdict(Plane::Tenant, &reader, &[None]), Verdict::ClassMismatch);
        let mut hidden = same.clone(); hidden["correction_visible"] = json!(false);
        assert_eq!(verdict(Plane::Tenant, &reader, &[Some(&hidden)]), Verdict::ClassMismatch);
        let mut floored = same.clone(); floored["history_floor"] = json!("2026-01-01T00:00:00");
        assert_eq!(verdict(Plane::Tenant, &reader, &[Some(&floored)]), Verdict::ClassMismatch);
        for v in [Verdict::Admitted, Verdict::ContractChanged, Verdict::ClassMismatch] {
            assert_eq!(v.fallback_reason().and_then(Verdict::from_reason).unwrap_or(Verdict::Admitted), v);
        }
    }
    #[test]
    fn sql_fragments_check_the_reader_digest_and_every_stored_grain() {
        let changed = sql::contract_changed("r", &["a", "b"]);
        assert!(changed.contains(TENANT_CONTRACT_DIGEST) && changed.contains(CONTROL_CONTRACT_DIGEST));
        assert!(changed.contains("a->>'policy_digest'") && changed.contains("b->>'policy_digest'"));
        assert_eq!(sql::class_mismatch("r", &["a", "b"]), "(a IS DISTINCT FROM r OR b IS DISTINCT FROM r)");
        assert_eq!(sql::reader_class("ids.id"), "public.library_evidence_class(ids.id)");
        for digest in [TENANT_CONTRACT_DIGEST, CONTROL_CONTRACT_DIGEST] {
            assert!(digest.len() == 64 && digest.bytes().all(|b| b.is_ascii_hexdigit()));
        }
    }
    #[test]
    fn the_migrations_define_the_functions_this_module_names() {
        let tenant = include_str!("../../migrations_tenant/0206_library_access_class.sql");
        let control = include_str!("../../migrations_control/0391_library_access_class.sql");
        for sql in [tenant, control] {
            assert!(sql.contains("CREATE FUNCTION public.library_access_contract_digest()"));
            assert!(sql.contains("CREATE FUNCTION public.library_evidence_class(g UUID)"));
            assert!(sql.contains(&format!("jsonb_build_object('schema_version',{CLASS_SCHEMA_VERSION},")));
        }
        // The digest body is byte-identical on both planes.
        let body = |s: &str| s[s.find("CREATE FUNCTION public.library_access_contract_digest()").unwrap()..s.find("REVOKE ALL ON FUNCTION public.library_access_contract_digest()").unwrap()].to_owned();
        assert_eq!(body(tenant), body(control));
        // C4: the control class makes no tenant-only call.
        let class = &control[control.find("CREATE FUNCTION public.library_evidence_class").unwrap()..];
        assert!(!class.contains("tenant_rls_"));
    }
}
