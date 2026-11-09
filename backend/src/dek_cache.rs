//! Short-lived in-process cache for Data Encryption Keys.
//!
//! Resolving a DEK requires a DB round-trip plus AES-KWP unwrapping.  For
//! accounts that submit many concurrent queries this can become a bottleneck.
//! `DekCache` stores the unwrapped DEK for up to `TTL` seconds so that
//! subsequent requests within the same window skip the DB entirely.
//!
//! Keys are evicted lazily on the next access after TTL expiry.  For
//! correctness after a key-rotation, the `rotate_dek` admin handler should
//! call `DekCache::invalidate` (or simply wait for TTL expiry).
//!
//! # Tenant-scoped keys (org-tenancy PR 1.3)
//! The cache key is `(TenantId, account_id)`, not just `account_id`. Keying by
//! `TenantId` as well makes a cross-plane cache hit (returning a personal
//! DEK for what should be an org lookup, or vice versa) structurally
//! impossible, rather than relying on every caller to never mix up which
//! plane it meant — the same discipline the DEK-resolution helpers in
//! `crypto.rs` apply at the DB layer.

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

use crate::tenancy::TenantId;

// ── Constants ─────────────────────────────────────────────────────────────────

const TTL: Duration = Duration::from_secs(300); // 5 minutes

// ── Internal entry ────────────────────────────────────────────────────────────

// Deliberately does NOT derive `Debug` — it holds unwrapped DEK bytes, and a
// stray `{:?}` (e.g. in a future log line) must not be able to leak them. See
// the KEK-hygiene audit note in `packages/core/src/kek.rs`.
#[derive(Clone)]
struct CachedDek {
    dek: [u8; 32],
    inserted_at: Instant,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Cloneable, `Send + Sync` DEK cache backed by a `DashMap`.
#[derive(Clone)]
pub struct DekCache {
    inner: Arc<DashMap<(TenantId, Uuid), CachedDek>>,
}

impl DekCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// Return the cached DEK for `(tenant, account_id)`, or `None` if absent /
    /// expired.
    pub fn get(&self, tenant: &TenantId, account_id: Uuid) -> Option<[u8; 32]> {
        let key = (tenant.clone(), account_id);
        // We need to check TTL while holding the ref, then potentially remove.
        let expired = self.inner.get(&key).map_or(false, |entry| {
            entry.inserted_at.elapsed() >= TTL
        });

        if expired {
            self.inner.remove(&key);
            return None;
        }

        self.inner.get(&key).map(|entry| entry.dek)
    }

    /// Insert or overwrite the DEK for `(tenant, account_id)`.
    pub fn insert(&self, tenant: &TenantId, account_id: Uuid, dek: [u8; 32]) {
        self.inner.insert(
            (tenant.clone(), account_id),
            CachedDek {
                dek,
                inserted_at: Instant::now(),
            },
        );
    }

    /// Remove a cached DEK immediately (e.g. after key rotation).
    pub fn invalidate(&self, tenant: &TenantId, account_id: Uuid) {
        self.inner.remove(&(tenant.clone(), account_id));
    }
}

impl Default for DekCache {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_dek(byte: u8) -> [u8; 32] {
        [byte; 32]
    }

    /// A freshly inserted DEK must be returned by `get`.
    #[test]
    fn hit_returns_cached_dek() {
        let cache = DekCache::new();
        let id = Uuid::new_v4();
        let dek = dummy_dek(0xAB);

        cache.insert(&TenantId::Consumer, id, dek);
        assert_eq!(cache.get(&TenantId::Consumer, id), Some(dek));
    }

    /// A DEK whose TTL has elapsed must not be returned.
    #[test]
    fn expired_entry_returns_none() {
        // Build a cache with a very short TTL by manipulating the inserted_at
        // field.  We can't override TTL directly, so we insert the entry with
        // an `inserted_at` that is already past TTL by inserting into the
        // inner DashMap directly.
        let cache = DekCache::new();
        let id = Uuid::new_v4();
        let dek = dummy_dek(0xCD);

        // Insert with an `inserted_at` that is already TTL + 1 second in the past.
        cache.inner.insert(
            (TenantId::Consumer, id),
            super::CachedDek {
                dek,
                inserted_at: Instant::now() - TTL - Duration::from_secs(1),
            },
        );

        assert_eq!(
            cache.get(&TenantId::Consumer, id),
            None,
            "expired entry should return None"
        );
        // Confirm lazy eviction removed the entry.
        assert!(
            !cache.inner.contains_key(&(TenantId::Consumer, id)),
            "expired entry should be evicted"
        );
    }

    /// `invalidate` must remove a cached DEK.
    #[test]
    fn invalidate_removes_entry() {
        let cache = DekCache::new();
        let id = Uuid::new_v4();
        cache.insert(&TenantId::Consumer, id, dummy_dek(0x01));
        cache.invalidate(&TenantId::Consumer, id);
        assert_eq!(cache.get(&TenantId::Consumer, id), None);
    }

    /// A missing account returns `None` without panicking.
    #[test]
    fn missing_account_returns_none() {
        let cache = DekCache::new();
        assert_eq!(cache.get(&TenantId::Consumer, Uuid::new_v4()), None);
    }

    /// The same `account_id` under two different tenants must be two distinct
    /// entries — the load-bearing invariant that makes a cross-plane cache hit
    /// structurally impossible.
    #[test]
    fn same_account_id_different_tenants_do_not_collide() {
        let cache = DekCache::new();
        let id = Uuid::new_v4();
        let personal_dek = dummy_dek(0x11);
        let org_dek = dummy_dek(0x22);

        cache.insert(&TenantId::Consumer, id, personal_dek);
        cache.insert(&TenantId::Org("org_a".into()), id, org_dek);

        assert_eq!(cache.get(&TenantId::Consumer, id), Some(personal_dek));
        assert_eq!(cache.get(&TenantId::Org("org_a".into()), id), Some(org_dek));
        // A different org, same account_id, has never been inserted.
        assert_eq!(cache.get(&TenantId::Org("org_b".into()), id), None);
    }

    /// Invalidating one tenant's entry must not affect another tenant's entry
    /// for the same `account_id`.
    #[test]
    fn invalidate_is_tenant_scoped() {
        let cache = DekCache::new();
        let id = Uuid::new_v4();
        cache.insert(&TenantId::Consumer, id, dummy_dek(0x33));
        cache.insert(&TenantId::Org("org_a".into()), id, dummy_dek(0x44));

        cache.invalidate(&TenantId::Consumer, id);

        assert_eq!(cache.get(&TenantId::Consumer, id), None);
        assert_eq!(
            cache.get(&TenantId::Org("org_a".into()), id),
            Some(dummy_dek(0x44)),
            "invalidating the consumer entry must not evict the org entry"
        );
    }
}
