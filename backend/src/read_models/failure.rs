//! Privacy-safe classification of a failed read-model compute.
//!
//! A compute error's text can carry private content: Postgres messages quote
//! values and identifiers, sqlx decode errors name columns and echo input,
//! serde errors echo the offending value. So neither the worker's log line nor
//! `read_models.last_error` may carry it. [`classify`] reduces an error to a
//! fixed [`FailureKind`] (a code literal) plus, for a database error, its
//! five-character SQLSTATE. Nothing else leaves.
use super::{Error, Unaddressable};
use std::error::Error as StdError;

/// Why a compute failed. [`FailureKind::as_str`] is the only text that is
/// logged or persisted for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureKind {
    /// `statement_timeout` cancelled a statement (SQLSTATE 57014).
    StatementTimeout,
    /// The whole compute ran past the worker's wall-clock budget.
    WallTimeout,
    /// `lock_timeout` or NOWAIT (SQLSTATE 55P03).
    LockTimeout,
    /// SQLSTATE 40001.
    Serialization,
    /// SQLSTATE 40P01.
    Deadlock,
    /// Any other error Postgres returned; its SQLSTATE says which.
    Database,
    /// No pool connection within the acquire timeout.
    PoolTimeout,
    /// Closed pool, I/O, TLS, protocol, a failed BEGIN or a crashed driver.
    Connection,
    /// A row or column did not decode into its Rust type.
    Decode,
    /// A bind parameter did not encode.
    Encode,
    /// A query that requires a row returned none.
    RowNotFound,
    /// Stored parameters or the computed payload did not (de)serialize.
    Json,
    /// A blob read from R2 failed.
    Blob,
    /// The claimed scope no longer addresses its actor's live identity.
    ScopeUnaddressable,
    /// The compute panicked.
    Panic,
    /// Anything else.
    Other,
}

impl FailureKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StatementTimeout => "statement_timeout",
            Self::WallTimeout => "wall_timeout",
            Self::LockTimeout => "lock_timeout",
            Self::Serialization => "serialization",
            Self::Deadlock => "deadlock",
            Self::Database => "database",
            Self::PoolTimeout => "pool_timeout",
            Self::Connection => "connection",
            Self::Decode => "decode",
            Self::Encode => "encode",
            Self::RowNotFound => "row_not_found",
            Self::Json => "json",
            Self::Blob => "blob",
            Self::ScopeUnaddressable => "scope_unaddressable",
            Self::Panic => "panic",
            Self::Other => "other",
        }
    }
}

/// A classified failure: its kind, the SQLSTATE of a database error and the
/// variant of a [`Unaddressable`] refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub kind: FailureKind,
    sqlstate: Option<String>,
    reason: Option<Unaddressable>,
}

impl Failure {
    pub const fn of(kind: FailureKind) -> Self {
        Self {
            kind,
            sqlstate: None,
            reason: None,
        }
    }
    pub fn sqlstate(&self) -> Option<&str> {
        self.sqlstate.as_deref()
    }
    /// Which identity check refused a `scope_unaddressable` failure.
    pub fn reason(&self) -> Option<Unaddressable> {
        self.reason
    }
}

/// Classify by the first recognised error in the chain. `StoreError::Db`
/// carries its cause without exposing it as `source()`, so it is unwrapped.
pub fn classify(error: &Error) -> Failure {
    classify_dyn(error.as_ref(), 0)
}

/// Bounds a pathological (cyclic) `source()` chain.
const MAX_DEPTH: usize = 16;

fn classify_dyn(mut e: &(dyn StdError + 'static), mut depth: usize) -> Failure {
    while depth < MAX_DEPTH {
        if let Some(sqlx) = e.downcast_ref::<sqlx::Error>() {
            return sqlx_failure(sqlx);
        }
        if let Some(crate::store::StoreError::Db(inner)) =
            e.downcast_ref::<crate::store::StoreError>()
        {
            return classify_dyn(inner.as_ref(), depth + 1);
        }
        if let Some(reason) = e.downcast_ref::<Unaddressable>() {
            return Failure {
                reason: Some(*reason),
                ..Failure::of(FailureKind::ScopeUnaddressable)
            };
        }
        if e.is::<serde_json::Error>() {
            return Failure::of(FailureKind::Json);
        }
        if e.is::<coralswarm_core::r2::R2Error>() {
            return Failure::of(FailureKind::Blob);
        }
        match e.source() {
            Some(source) => e = source,
            None => break,
        }
        depth += 1;
    }
    Failure::of(FailureKind::Other)
}

fn sqlx_failure(e: &sqlx::Error) -> Failure {
    use crate::request_timing::{PageReadFailure, QUERY_CANCELED};
    use FailureKind as K;
    // The page-read ledger's own reading of the two timeouts, so both count
    // the same errors (including a raw executor's recorded acquire timeout,
    // which arrives as `Io`).
    match PageReadFailure::of(e) {
        Some(PageReadFailure::StatementTimeout) => {
            return Failure {
                sqlstate: Some(QUERY_CANCELED.to_owned()),
                ..Failure::of(K::StatementTimeout)
            }
        }
        Some(PageReadFailure::AcquireTimeout) => return Failure::of(K::PoolTimeout),
        None => {}
    }
    let kind = match e {
        sqlx::Error::Database(db) => {
            let sqlstate = db.code().as_deref().and_then(sqlstate);
            let kind = match sqlstate.as_deref() {
                Some("55P03") => K::LockTimeout,
                Some("40001") => K::Serialization,
                Some("40P01") => K::Deadlock,
                _ => K::Database,
            };
            return Failure {
                sqlstate,
                ..Failure::of(kind)
            };
        }
        sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::BeginFailed => K::Connection,
        sqlx::Error::Decode(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::TypeNotFound { .. } => K::Decode,
        sqlx::Error::Encode(_) => K::Encode,
        sqlx::Error::RowNotFound => K::RowNotFound,
        _ => K::Other,
    };
    Failure::of(kind)
}

/// Keep a SQLSTATE only in its defined shape (five ASCII digits or uppercase
/// letters), so the field can never carry anything but the code.
fn sqlstate(code: &str) -> Option<String> {
    (code.len() == 5
        && code
            .bytes()
            .all(|b| b.is_ascii_digit() || b.is_ascii_uppercase()))
    .then(|| code.to_owned())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::borrow::Cow;

    /// A driver error carrying private text in its message, as Postgres does
    /// when it quotes a value, identifier or statement.
    #[derive(Debug)]
    pub(crate) struct FakeDbError {
        pub code: Option<&'static str>,
        pub message: &'static str,
    }
    impl std::fmt::Display for FakeDbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.message)
        }
    }
    impl StdError for FakeDbError {}
    impl sqlx::error::DatabaseError for FakeDbError {
        fn message(&self) -> &str {
            self.message
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            self.code.map(Cow::Borrowed)
        }
        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    pub(crate) fn db(code: Option<&'static str>, message: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(FakeDbError { code, message }))
    }

    fn of(e: impl Into<Error>) -> Failure {
        classify(&e.into())
    }

    #[test]
    fn sqlstates_map_to_kinds_and_keep_the_code() {
        for (code, kind) in [
            ("57014", FailureKind::StatementTimeout),
            ("55P03", FailureKind::LockTimeout),
            ("40001", FailureKind::Serialization),
            ("40P01", FailureKind::Deadlock),
            ("42P01", FailureKind::Database),
            ("25006", FailureKind::Database),
            ("53300", FailureKind::Database),
        ] {
            let f = of(db(Some(code), "private"));
            assert_eq!((f.kind, f.sqlstate()), (kind, Some(code)), "{code}");
        }
        // No code, or one not shaped like a SQLSTATE, is never echoed.
        for code in [
            None,
            Some("57 014"),
            Some("x'; --"),
            Some("5701"),
            Some("ab123"),
        ] {
            let f = of(db(code, "private"));
            assert_eq!(
                (f.kind, f.sqlstate()),
                (FailureKind::Database, None),
                "{code:?}"
            );
        }
    }

    #[test]
    fn sqlx_variants_map_to_kinds() {
        let io = || std::io::Error::other("private");
        for (e, kind) in [
            (sqlx::Error::PoolTimedOut, FailureKind::PoolTimeout),
            (sqlx::Error::PoolClosed, FailureKind::Connection),
            (sqlx::Error::Io(io()), FailureKind::Connection),
            (sqlx::Error::Tls(io().into()), FailureKind::Connection),
            (
                sqlx::Error::Protocol("private".into()),
                FailureKind::Connection,
            ),
            (sqlx::Error::WorkerCrashed, FailureKind::Connection),
            (sqlx::Error::BeginFailed, FailureKind::Connection),
            (sqlx::Error::Decode(io().into()), FailureKind::Decode),
            (
                sqlx::Error::ColumnDecode {
                    index: "\"private\"".into(),
                    source: io().into(),
                },
                FailureKind::Decode,
            ),
            (
                sqlx::Error::ColumnNotFound("private".into()),
                FailureKind::Decode,
            ),
            (
                sqlx::Error::ColumnIndexOutOfBounds { index: 3, len: 2 },
                FailureKind::Decode,
            ),
            (
                sqlx::Error::TypeNotFound {
                    type_name: "private".into(),
                },
                FailureKind::Decode,
            ),
            (sqlx::Error::Encode(io().into()), FailureKind::Encode),
            (sqlx::Error::RowNotFound, FailureKind::RowNotFound),
            (
                sqlx::Error::InvalidArgument("private".into()),
                FailureKind::Other,
            ),
            (sqlx::Error::Configuration(io().into()), FailureKind::Other),
        ] {
            let label = format!("{e:?}");
            assert_eq!(of(e), Failure::of(kind), "{label}");
        }
    }

    #[test]
    fn wrapped_and_non_sql_errors_classify_through_the_chain() {
        // Store helpers wrap the driver error without a `source()`.
        let store = crate::store::StoreError::Db(Box::new(db(Some("40P01"), "private")));
        assert_eq!(of(store).kind, FailureKind::Deadlock);
        let store = crate::store::StoreError::from(db(Some("57014"), "private"));
        assert_eq!(of(store).sqlstate(), Some("57014"));
        let json = serde_json::from_str::<u32>("\"private\"").unwrap_err();
        assert_eq!(of(json).kind, FailureKind::Json);
        for reason in [
            Unaddressable::ControlRevision,
            Unaddressable::Membership,
            Unaddressable::ScopeGraphs,
            Unaddressable::ViewerClass,
            Unaddressable::ClaimedGraphs,
        ] {
            let f = of(reason);
            assert_eq!(
                (f.kind, f.reason(), f.sqlstate()),
                (FailureKind::ScopeUnaddressable, Some(reason), None)
            );
        }
        assert_eq!(of(db(Some("40P01"), "private")).reason(), None);
        // An error the worker raises as a bare string is unknown, not guessed.
        assert_eq!(of("unknown model").kind, FailureKind::Other);
        let other = crate::store::StoreError::Conflict("private".into());
        assert_eq!(of(other).kind, FailureKind::Other);
    }

    #[test]
    fn kinds_are_distinct_snake_case_literals() {
        use FailureKind as K;
        let all = [
            K::StatementTimeout,
            K::WallTimeout,
            K::LockTimeout,
            K::Serialization,
            K::Deadlock,
            K::Database,
            K::PoolTimeout,
            K::Connection,
            K::Decode,
            K::Encode,
            K::RowNotFound,
            K::Json,
            K::Blob,
            K::ScopeUnaddressable,
            K::Panic,
            K::Other,
        ];
        let names: std::collections::BTreeSet<_> = all.iter().map(|k| k.as_str()).collect();
        assert_eq!(names.len(), all.len());
        assert!(names
            .iter()
            .all(|n| n.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')));
    }
}
