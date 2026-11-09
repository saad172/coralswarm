use super::*;
use crate::store::{PostgresStore, Store};
use coralswarm_core::r2::R2Error;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
};
use tokio::sync::Barrier;

struct Fixture {
    admin: PgPool,
    owner: PgPool,
    app: PgPool,
    db: crate::test_db::TestDatabase,
    tenant: bool,
    account: Uuid,
    graph: Uuid,
}

impl Fixture {
    async fn new(tenant: bool) -> Self {
        let base = std::env::var("ORGT_TEST_BASE_URL")
            .expect("content replacement requires local PostgreSQL");
        let opts = PgConnectOptions::from_str(&base).unwrap();
        assert!(matches!(opts.get_host(), "localhost" | "127.0.0.1" | "::1"));
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(opts.clone())
            .await
            .unwrap();
        let db = crate::test_db::TestDatabase::new(&opts, "content_replacement");
        sqlx::query(&format!("CREATE DATABASE {db}"))
            .execute(&admin)
            .await
            .unwrap();
        let owner = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(opts.clone().database(&db))
            .await
            .unwrap();
        if tenant {
            sqlx::migrate!("./migrations_tenant")
                .run(&owner)
                .await
                .unwrap();
        } else {
            sqlx::migrate!("./migrations_control")
                .run(&owner)
                .await
                .unwrap();
        }
        let app = PgPoolOptions::new()
            .max_connections(5)
            .after_connect(move |conn, _| {
                Box::pin(async move {
                    sqlx::query(if tenant {
                        "SET ROLE coralswarm_tenant_app"
                    } else {
                        "SET ROLE coralswarm_app"
                    })
                    .execute(conn)
                    .await?;
                    Ok(())
                })
            })
            .connect_with(opts.database(&db))
            .await
            .unwrap();
        let restricted: bool = sqlx::query_scalar("SELECT NOT rolsuper AND NOT rolbypassrls AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication FROM pg_roles WHERE rolname=current_user").fetch_one(&app).await.unwrap();
        assert!(restricted);
        let account = Uuid::new_v4();
        if !tenant {
            let user = Uuid::new_v4();
            sqlx::query("INSERT INTO users(id,email) VALUES($1,$2)")
                .bind(user)
                .bind(format!("{user}@replacement.test"))
                .execute(&owner)
                .await
                .unwrap();
            sqlx::query("INSERT INTO accounts(id,owner_user_id) VALUES($1,$2)")
                .bind(account)
                .bind(user)
                .execute(&owner)
                .await
                .unwrap();
        }
        let graph = sqlx::query_scalar(
            "INSERT INTO graphs(account_id,name) VALUES($1,'replacement') RETURNING id",
        )
        .bind(account)
        .fetch_one(&owner)
        .await
        .unwrap();
        Self {
            admin,
            owner,
            app,
            db,
            tenant,
            account,
            graph,
        }
    }

    fn scope(&self) -> ContentScope<'_> {
        self.scope_on(&self.app)
    }

    fn scope_on<'a>(&self, pool: &'a PgPool) -> ContentScope<'a> {
        if self.tenant {
            ContentScope::Org {
                pool,
                account_id: self.account,
            }
        } else {
            ContentScope::Account {
                pool,
                account_id: self.account,
            }
        }
    }

    async fn seed(&self, inline: bool, blobs: &Blobs) -> ContentSnapshot {
        self.seed_texts(inline.then_some("before"), b"before", "note", blobs)
            .await
    }

    async fn seed_texts(
        &self,
        inline: Option<&str>,
        bytes: &[u8],
        source_type: &str,
        blobs: &Blobs,
    ) -> ContentSnapshot {
        let id = Uuid::new_v4();
        let key = coralswarm_core::r2::blob_key_for(
            self.tenant.then_some("org_replacement"),
            self.graph,
            id,
        )
        .unwrap();
        sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,header_len,embedding,embed_model,embedding_next,embed_model_next) VALUES($1,$2,$6,$3,$4,$5,now(),0,array_fill(0.1::real,ARRAY[384])::vector,'local-old',array_fill(0.2::real,ARRAY[384])::vector,'cloud-old')")
            .bind(id).bind(self.graph).bind(id.to_string()).bind(&key).bind(inline).bind(source_type)
            .execute(&self.owner).await.unwrap();
        blobs.objects.lock().unwrap().insert(key, bytes.to_vec());
        self.snapshot(id).await
    }

    async fn snapshot(&self, atom: Uuid) -> ContentSnapshot {
        let (graph_id, revision, blob_key, inline, header_len, timestamp) = sqlx::query_as("SELECT graph_id,content_revision,content_blob_key,content_inline,header_len,timestamp FROM atoms WHERE atom_id=$1").bind(atom).fetch_one(&self.owner).await.unwrap();
        ContentSnapshot {
            graph_id,
            atom_id: atom,
            revision,
            blob_key,
            inline,
            header_len,
            timestamp,
        }
    }

    async fn counts(&self, atom: Uuid) -> (i64, i64) {
        sqlx::query_as("SELECT (SELECT count(*) FROM op_log WHERE entity_id=$1::text),(SELECT count(*) FROM jobs WHERE kind='EmbedAtoms' AND payload->'atom_ids' @> jsonb_build_array($1::text))")
            .bind(atom.to_string()).fetch_one(&self.owner).await.unwrap()
    }

    async fn cloud_state(&self, atom: Uuid) -> (bool, Option<String>, bool, Option<String>) {
        sqlx::query_as("SELECT embedding_next IS NULL,embed_model_next,embedding IS NOT NULL,embed_model FROM atoms WHERE atom_id=$1").bind(atom).fetch_one(&self.owner).await.unwrap()
    }

    async fn superseded(&self, atom: Uuid) -> Vec<String> {
        sqlx::query_scalar("SELECT superseded_blob_keys FROM atoms WHERE atom_id=$1")
            .bind(atom).fetch_one(&self.owner).await.unwrap()
    }

    async fn finish(self) {
        self.app.close().await;
        self.owner.close().await;
        self.admin.close().await;
        eprintln!("retained content replacement fixture database {}", self.db);
    }
}

#[derive(Default)]
struct Blobs {
    objects: Mutex<HashMap<String, Vec<u8>>>,
    puts: Mutex<Vec<String>>,
    deletes: Mutex<Vec<String>>,
    uploaded: Option<Arc<Barrier>>,
    resume: Option<Arc<Barrier>>,
    read_fault: Option<&'static str>,
}

#[async_trait::async_trait]
impl PromotionBlobStore for Blobs {
    async fn get_blob(&self, key: &str, _: &[u8; 32]) -> Result<Vec<u8>, R2Error> {
        match self.read_fault {
            Some("transport") => return Err(R2Error::S3("local fixture transport failure".into())),
            Some("decrypt") => {
                return Err(R2Error::Crypto(
                    coralswarm_core::crypto::CryptoError::TagMismatch,
                ))
            }
            None => {}
            other => panic!("unknown blob fixture fault: {other:?}"),
        }
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| R2Error::NotFound(key.into()))
    }

    async fn put_blob(&self, key: &str, bytes: &[u8], _: &[u8; 32]) -> Result<(), R2Error> {
        assert!(
            self.objects
                .lock()
                .unwrap()
                .insert(key.into(), bytes.to_vec())
                .is_none(),
            "replacement overwrote an existing object"
        );
        self.puts.lock().unwrap().push(key.into());
        if let Some(barrier) = &self.uploaded {
            barrier.wait().await;
        }
        if let Some(barrier) = &self.resume {
            barrier.wait().await;
        }
        Ok(())
    }

    async fn delete_blob(&self, key: &str) -> Result<(), R2Error> {
        self.deletes.lock().unwrap().push(key.into());
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

async fn resolved_text(snapshot: &ContentSnapshot, blobs: &Blobs) -> String {
    coralswarm_core::r2::validate_blob_key(&snapshot.blob_key).unwrap();
    crate::workers::embed::atom_text_from_r2_or_inline(
        blobs.get_blob(&snapshot.blob_key, &[7; 32]).await,
        snapshot.inline.as_deref(),
    )
    .unwrap()
}

async fn replace_with(
    f: &Fixture,
    snapshot: &ContentSnapshot,
    blobs: &Blobs,
    content: &str,
    device: &str,
) -> Result<bool, String> {
    replace(
        f.scope(),
        snapshot,
        content,
        Some(3),
        device,
        Some(blobs),
        Some(&[7; 32]),
    )
    .await
}

async fn fresh_and_atomic(f: &Fixture) {
    for inline in [false, true] {
        let blobs = Blobs::default();
        let old = f.seed(inline, &blobs).await;
        assert!(replace_with(f, &old, &blobs, "α\nreplacement 🪸", "fresh")
            .await
            .unwrap());
        let new = f.snapshot(old.atom_id).await;
        assert_eq!(new.revision, old.revision + 1);
        assert_eq!(new.header_len, Some(3));
        assert_eq!(f.counts(old.atom_id).await, (1, 1));
        let superseded: Vec<String> =
            sqlx::query_scalar("SELECT superseded_blob_keys FROM atoms WHERE atom_id=$1")
                .bind(old.atom_id)
                .fetch_one(&f.owner)
                .await
                .unwrap();
        assert_eq!(superseded, vec![old.blob_key.clone()]);
        assert_eq!(
            f.cloud_state(old.atom_id).await,
            (true, None, true, Some("local-old".into()))
        );
        assert_eq!(
            blobs.objects.lock().unwrap().get(&old.blob_key).unwrap(),
            b"before"
        );
        assert_eq!(blobs.puts.lock().unwrap().as_slice(), &[new.blob_key.clone()]);
        assert_eq!(resolved_text(&new, &blobs).await, "α\nreplacement 🪸");
        if inline {
            assert_eq!(new.inline.as_deref(), Some("α\nreplacement 🪸"));
            assert_ne!(new.blob_key, old.blob_key);
            assert_eq!(resolved_text(&new, &blobs).await, "α\nreplacement 🪸");
        } else {
            assert_ne!(new.blob_key, old.blob_key);
            assert_eq!(
                new.blob_key.rsplit_once('/').unwrap().0,
                old.blob_key.rsplit_once('/').unwrap().0
            );
            assert_eq!(
                blobs.objects.lock().unwrap().get(&new.blob_key).unwrap(),
                "α\nreplacement 🪸".as_bytes()
            );
        }
        assert!(!replace_with(f, &old, &blobs, "stale", "stale")
            .await
            .unwrap());
        assert_eq!(f.snapshot(old.atom_id).await.revision, new.revision);
        assert_eq!(f.counts(old.atom_id).await, (1, 1));
    }
}

async fn upload_races(f: &Fixture) {
    for delete in [false, true] {
        let uploaded = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let blobs = Blobs {
            uploaded: Some(uploaded.clone()),
            resume: Some(resume.clone()),
            ..Blobs::default()
        };
        let old = f.seed(false, &blobs).await;
        let replacement = replace_with(f, &old, &blobs, "loser", "race");
        let change = async {
            uploaded.wait().await;
            let mut tx = f.scope().begin().await.unwrap();
            sqlx::query(if delete {
                "DELETE FROM atoms WHERE atom_id=$1"
            } else {
                "UPDATE atoms SET header_len=7 WHERE atom_id=$1"
            })
            .bind(old.atom_id)
            .execute(&mut *tx)
            .await
            .unwrap();
            tx.commit().await.unwrap();
            resume.wait().await;
        };
        let (result, ()) = tokio::join!(replacement, change);
        assert!(!result.unwrap());
        assert_eq!(f.counts(old.atom_id).await, (0, 0));
        assert_eq!(blobs.deletes.lock().unwrap().len(), 1);
        assert_eq!(blobs.objects.lock().unwrap().len(), 1);
        assert_eq!(
            blobs.objects.lock().unwrap().get(&old.blob_key).unwrap(),
            b"before"
        );
    }
    let uploaded = Arc::new(Barrier::new(2));
    let blobs = Blobs {
        uploaded: Some(uploaded),
        ..Blobs::default()
    };
    let old = f.seed(false, &blobs).await;
    let (first, second) = tokio::join!(
        replace_with(f, &old, &blobs, "one", "concurrent"),
        replace_with(f, &old, &blobs, "two", "concurrent")
    );
    assert_ne!(first.unwrap(), second.unwrap());
    assert_eq!(f.counts(old.atom_id).await, (1, 1));
    assert_eq!(blobs.puts.lock().unwrap().len(), 2);
    assert_eq!(blobs.deletes.lock().unwrap().len(), 1);
    assert_eq!(blobs.objects.lock().unwrap().len(), 2);
}

async fn rollback_is_atomic(f: &Fixture) {
    let blobs = Blobs::default();
    let old = f.seed(false, &blobs).await;
    sqlx::query("ALTER TABLE op_log ADD CONSTRAINT replacement_fail_sync CHECK(device_id<>'reject-sync') NOT VALID").execute(&f.owner).await.unwrap();
    assert!(
        replace_with(f, &old, &blobs, "not committed", "reject-sync")
            .await
            .is_err()
    );
    let unchanged = f.snapshot(old.atom_id).await;
    assert_eq!(unchanged.blob_key, old.blob_key);
    assert_eq!(unchanged.revision, old.revision);
    assert_eq!(f.cloud_state(old.atom_id).await.0, false);
    assert_eq!(f.counts(old.atom_id).await, (0, 0));
    assert_eq!(blobs.deletes.lock().unwrap().len(), 1);
    let inline = f.seed(true, &blobs).await;
    assert!(replace(f.scope(), &inline, "inline rollback", Some(3),
        "reject-sync", None, None).await.is_err());
    assert_eq!(f.snapshot(inline.atom_id).await.blob_key, inline.blob_key);
    assert_eq!(f.snapshot(inline.atom_id).await.revision, inline.revision);
    assert_eq!(f.superseded(inline.atom_id).await, Vec::<String>::new());
    assert_eq!(f.counts(inline.atom_id).await, (0, 0));
    assert_eq!(blobs.deletes.lock().unwrap().len(), 1);
    sqlx::query("ALTER TABLE op_log DROP CONSTRAINT replacement_fail_sync")
        .execute(&f.owner)
        .await
        .unwrap();
    sqlx::query("ALTER TABLE jobs ADD CONSTRAINT replacement_fail_enqueue CHECK(payload->'atom_ids' IS NULL) NOT VALID").execute(&f.owner).await.unwrap();
    assert!(replace_with(f, &old, &blobs, "not queued", "enqueue-fail")
        .await
        .is_err());
    assert_eq!(f.snapshot(old.atom_id).await.blob_key, old.blob_key);
    assert_eq!(f.counts(old.atom_id).await, (0, 0));
    assert_eq!(blobs.deletes.lock().unwrap().len(), 2);
    sqlx::query("ALTER TABLE jobs DROP CONSTRAINT replacement_fail_enqueue")
        .execute(&f.owner)
        .await
        .unwrap();
}

async fn scope_mismatch(f: &Fixture) {
    let blobs = Blobs::default();
    let old = f.seed(false, &blobs).await;
    let scope = if f.tenant {
        ContentScope::Org {
            pool: &f.app,
            account_id: Uuid::new_v4(),
        }
    } else {
        ContentScope::Account {
            pool: &f.app,
            account_id: Uuid::new_v4(),
        }
    };
    assert!(!replace(
        scope,
        &old,
        "wrong account",
        Some(3),
        "scope",
        Some(&blobs),
        Some(&[7; 32])
    )
    .await
    .unwrap());
    assert_eq!(f.snapshot(old.atom_id).await.blob_key, old.blob_key);
    assert_eq!(f.counts(old.atom_id).await, (0, 0));
    assert_eq!(blobs.deletes.lock().unwrap().len(), 1);

    for wrong_graph in [false, true] {
        let mut invalid = f.snapshot(old.atom_id).await;
        if wrong_graph {
            invalid.graph_id = Uuid::new_v4();
        } else {
            invalid.blob_key = "malformed-pointer".into();
        }
        let before_puts = blobs.puts.lock().unwrap().len();
        assert!(replace_with(f, &invalid, &blobs, "bad pointer", "pointer")
            .await
            .is_err());
        assert_eq!(blobs.puts.lock().unwrap().len(), before_puts);
        assert_eq!(f.counts(old.atom_id).await, (0, 0));
    }
}

async fn current_byte_transformations(f: &Fixture, corrections: bool) {
    let tenant = if f.tenant {
        crate::tenancy::TenantId::Org("org_replacement".into())
    } else {
        crate::tenancy::TenantId::Consumer
    };
    for case in [
        "readable",
        "not-found",
        "transport",
        "decrypt",
        "invalid-utf8",
        "inline-only",
    ] {
        let blobs = Blobs {
            read_fault: matches!(case, "transport" | "decrypt").then_some(case),
            ..Blobs::default()
        };
        let bytes: &[u8] = if case == "invalid-utf8" {
            b"Speaker 2: invalid \xff"
        } else {
            b"Speaker 2: current B."
        };
        let old = f
            .seed_texts(Some("Speaker 2: stale A."), bytes, "transcript", &blobs)
            .await;
        if case == "not-found" {
            blobs.objects.lock().unwrap().remove(&old.blob_key);
        }
        let transport = (case != "inline-only").then_some(&blobs as &dyn PromotionBlobStore);
        let refused = matches!(case, "transport" | "decrypt" | "invalid-utf8");
        let expected_body = if case == "readable" {
            "Speaker 2: current B."
        } else {
            "Speaker 2: stale A."
        };
        let final_text = if corrections {
            sqlx::query("INSERT INTO transcript_corrections(graph_id,atom_id,source_id,surface_form,canonical,status,kind,origin) VALUES($1,$2,$3,'Speaker 2','Alice','applied','speaker','user')")
                .bind(f.graph).bind(Uuid::nil()).bind(old.atom_id.to_string()).execute(&f.owner).await.unwrap();
            let result = crate::services::corrections::propagate(
                &f.app,
                transport,
                Some([7; 32]),
                "current-bytes",
                f.graph,
                &old.atom_id.to_string(),
            )
            .await
            .unwrap();
            if refused {
                assert!(result.rewritten_atoms.is_empty(), "case {case}");
                assert!(!result.embed_job_enqueued);
                None
            } else {
                assert_eq!(
                    result.rewritten_atoms,
                    vec![(f.graph, old.atom_id)],
                    "case {case}"
                );
                Some(expected_body.replace("Speaker 2", "Alice"))
            }
        } else {
            let result = crate::workers::reembed::prepare_current_content_for_test(
                &tenant,
                &f.app,
                f.account,
                old.atom_id,
                transport,
            )
            .await;
            if refused {
                assert!(result.is_err(), "case {case}");
                None
            } else {
                let prepared = result.unwrap();
                assert!(prepared.ends_with(expected_body), "case {case}: {prepared}");
                assert!(prepared.starts_with("Meeting on "));
                Some(prepared)
            }
        };
        let current = f.snapshot(old.atom_id).await;
        if let Some(text) = final_text {
            assert_eq!(resolved_text(&current, &blobs).await, text, "case {case}");
            assert_eq!(current.revision, old.revision + 1);
            assert_eq!(f.counts(old.atom_id).await, (1, 1));
            assert!(f.cloud_state(old.atom_id).await.0);
            let sync: String = sqlx::query_scalar("SELECT payload->>'content_inline' FROM op_log WHERE entity_id=$1 AND entity='atom' ORDER BY seq DESC LIMIT 1")
                .bind(old.atom_id.to_string()).fetch_one(&f.owner).await.unwrap();
            assert_eq!(sync, text);
            if !corrections {
                assert_eq!(
                    current.header_len,
                    Some((text.len() - expected_body.len()) as i32)
                );
            }
            assert_ne!(current.blob_key, old.blob_key);
        } else {
            assert_eq!(current.revision, old.revision);
            assert_eq!(current.blob_key, old.blob_key);
            assert_eq!(current.inline, old.inline);
            assert_eq!(f.counts(old.atom_id).await, (0, 0));
            assert!(!f.cloud_state(old.atom_id).await.0);
        }
        let puts = blobs.puts.lock().unwrap();
        if refused || case == "inline-only" {
            assert!(puts.is_empty(), "case {case}");
        } else {
            assert_eq!(puts.as_slice(), &[current.blob_key.clone()], "case {case}");
            assert_eq!(blobs.objects.lock().unwrap().get(&current.blob_key).unwrap(),
                current.inline.as_ref().unwrap().as_bytes());
        }
        assert!(!puts.contains(&old.blob_key));
        drop(puts);
        if !refused {
            assert_eq!(f.superseded(old.atom_id).await, vec![old.blob_key.clone()]);
            if case == "inline-only" {
                assert!(!blobs.objects.lock().unwrap().contains_key(&current.blob_key));
            }
        }
        if case != "not-found" {
            assert_eq!(
                blobs.objects.lock().unwrap().get(&old.blob_key).unwrap(),
                bytes
            );
        }
    }
}

async fn owned_header_and_policy(f: &Fixture) {
    let tenant = if f.tenant {
        crate::tenancy::TenantId::Org("org_replacement".into())
    } else {
        crate::tenancy::TenantId::Consumer
    };
    for transport in [false, true] {
        let blobs = Blobs::default();
        let seed = f.seed_texts(Some("body"), b"body", "transcript", &blobs).await;
        let header = coralswarm_core::chunker::build_meeting_header(
            None, Some(seed.timestamp), &[],
        ).unwrap();
        let text = format!("{header}\n\nbody");
        let old_key = if transport { seed.blob_key.clone() } else { "legacy-inline-key".into() };
        sqlx::query("UPDATE atoms SET content_inline=$2,content_blob_key=$3 WHERE atom_id=$1")
            .bind(seed.atom_id).bind(&text).bind(&old_key).execute(&f.owner).await.unwrap();
        blobs.objects.lock().unwrap().insert(old_key, text.as_bytes().to_vec());
        let old = f.snapshot(seed.atom_id).await;
        assert_eq!(old.header_len, Some(0));
        let prepared = crate::workers::reembed::prepare_current_content_for_test(
            &tenant, &f.app, f.account, old.atom_id,
            transport.then_some(&blobs as &dyn PromotionBlobStore),
        ).await.unwrap();
        let current = f.snapshot(old.atom_id).await;
        assert_eq!(prepared, text);
        assert_eq!(resolved_text(&current, &blobs).await, text);
        assert_eq!(current.inline.as_deref(), Some(text.as_str()));
        assert_eq!(current.revision, old.revision + 1);
        assert_eq!(current.header_len, Some((header.len() + 2) as i32));
        assert_ne!(current.blob_key, old.blob_key);
        assert_eq!(f.superseded(old.atom_id).await, vec![old.blob_key.clone()]);
        assert_eq!(f.counts(old.atom_id).await, (1, 1));
        assert_eq!(f.cloud_state(old.atom_id).await,
            (true, None, true, Some("local-old".into())));
        let synced: String = sqlx::query_scalar("SELECT payload->>'content_inline' FROM op_log WHERE entity_id=$1 AND entity='atom' ORDER BY seq DESC LIMIT 1")
            .bind(old.atom_id.to_string()).fetch_one(&f.owner).await.unwrap();
        assert_eq!(synced, text);
        assert_eq!(blobs.objects.lock().unwrap().get(&old.blob_key).unwrap(), text.as_bytes());
        if transport {
            assert_eq!(blobs.puts.lock().unwrap().as_slice(), &[current.blob_key.clone()]);
            assert_eq!(blobs.objects.lock().unwrap().get(&current.blob_key).unwrap(), text.as_bytes());
        } else {
            assert!(blobs.puts.lock().unwrap().is_empty());
            assert!(!blobs.objects.lock().unwrap().contains_key(&current.blob_key));
        }
        assert!(blobs.deletes.lock().unwrap().is_empty());
    }

    for marker in ["skipped:local-only", "blocked:managed_purpose_unsupported"] {
        let blobs = Blobs::default();
        let old = f.seed(true, &blobs).await;
        sqlx::query("UPDATE atoms SET embed_model_next=$2,superseded_blob_keys=ARRAY[content_blob_key] WHERE atom_id=$1")
            .bind(old.atom_id).bind(marker).execute(&f.owner).await.unwrap();
        assert!(replace_with(f, &old, &blobs, &format!("preserve {marker}"), "policy")
            .await.unwrap());
        assert_eq!(f.cloud_state(old.atom_id).await,
            (true, Some(marker.into()), true, Some("local-old".into())));
        assert_eq!(f.superseded(old.atom_id).await, vec![old.blob_key.clone()]);
        assert_eq!(blobs.objects.lock().unwrap().get(&old.blob_key).unwrap(), b"before");
    }

    let blobs = Blobs::default();
    let old = f.seed(true, &blobs).await;
    let store = PostgresStore::new(f.app.clone());
    let mut tx = f.scope().begin().await.unwrap();
    store.set_atom_embeddings_scoped(&mut tx, &[old.atom_id], &[vec![0.3; 384]])
        .await.unwrap();
    tx.commit().await.unwrap();
    let current = f.snapshot(old.atom_id).await;
    assert_eq!(current.revision, old.revision);
    assert_eq!(current.blob_key, old.blob_key);
    assert_eq!(resolved_text(&current, &blobs).await, "before");
    assert!(f.superseded(old.atom_id).await.is_empty());
    assert_eq!(f.cloud_state(old.atom_id).await,
        (false, Some("cloud-old".into()), true,
            Some(coralswarm_core::embed::EMBED_MODEL_NAME.into())));
    assert!(blobs.puts.lock().unwrap().is_empty());
}

async fn lost_commit_ack(f: &Fixture) {
    use sqlx::postgres::PgSslMode;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    let options = f.app.connect_options().as_ref().clone();
    let remote = (options.get_host().to_string(), options.get_port());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let committed = Arc::new(AtomicBool::new(false));
    let observed = committed.clone();
    let relay = tokio::spawn(async move {
        let (client, _) = listener.accept().await.unwrap();
        let server = TcpStream::connect(remote).await.unwrap();
        let (mut client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) = server.into_split();
        let outgoing = async {
            let mut buffer = [0u8; 16384];
            loop {
                let size = client_read.read(&mut buffer).await.unwrap();
                if size == 0 {
                    return;
                }
                if server_write.write_all(&buffer[..size]).await.is_err() {
                    return;
                }
            }
        };
        let incoming = async {
            let mut buffer = Vec::new();
            let mut scratch = [0u8; 16384];
            loop {
                let size = server_read.read(&mut scratch).await.unwrap();
                if size == 0 {
                    return;
                }
                buffer.extend_from_slice(&scratch[..size]);
                loop {
                    if buffer.len() < 5 {
                        break;
                    }
                    let size = u32::from_be_bytes(buffer[1..5].try_into().unwrap()) as usize + 1;
                    if buffer.len() < size {
                        break;
                    }
                    if buffer[0] == b'C' && &buffer[5..size] == b"COMMIT\0" {
                        observed.store(true, Ordering::SeqCst);
                        return;
                    }
                    if client_write.write_all(&buffer[..size]).await.is_err() {
                        return;
                    }
                    buffer.drain(..size);
                }
            }
        };
        tokio::select! { _=outgoing=>{}, _=incoming=>{} }
    });
    let tenant = f.tenant;
    let proxy = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            Box::pin(async move {
                sqlx::query(if tenant {
                    "SET ROLE coralswarm_tenant_app"
                } else {
                    "SET ROLE coralswarm_app"
                })
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect_with(
            options
                .host("127.0.0.1")
                .port(port)
                .ssl_mode(PgSslMode::Disable),
        )
        .await
        .unwrap();
    let blobs = Blobs::default();
    let old = f.seed(false, &blobs).await;
    let result = replace(
        f.scope_on(&proxy),
        &old,
        "committed despite lost ACK",
        Some(4),
        "lost-ack",
        Some(&blobs),
        Some(&[7; 32]),
    )
    .await;
    assert!(result.is_err());
    assert!(
        committed.load(Ordering::SeqCst),
        "relay must observe the actual successful COMMIT command tag"
    );
    let new = f.snapshot(old.atom_id).await;
    assert_ne!(new.blob_key, old.blob_key);
    assert_eq!(new.header_len, Some(4));
    assert_eq!(new.revision, old.revision + 1);
    assert_eq!(f.superseded(old.atom_id).await, vec![old.blob_key.clone()]);
    assert_eq!(f.counts(old.atom_id).await, (1, 1));
    assert_eq!(blobs.deletes.lock().unwrap().len(), 0);
    assert_eq!(
        blobs.objects.lock().unwrap().get(&new.blob_key).unwrap(),
        b"committed despite lost ACK"
    );
    assert_eq!(
        blobs.objects.lock().unwrap().get(&old.blob_key).unwrap(),
        b"before"
    );
    proxy.close().await;
    relay.await.unwrap();
}

async fn exercise(tenant: bool) {
    let f = Fixture::new(tenant).await;
    fresh_and_atomic(&f).await;
    upload_races(&f).await;
    rollback_is_atomic(&f).await;
    scope_mismatch(&f).await;
    owned_header_and_policy(&f).await;
    current_byte_transformations(&f, false).await;
    if !tenant {
        current_byte_transformations(&f, true).await;
    }
    lost_commit_ack(&f).await;
    f.finish().await;
}

async fn promotion_candidate(
    f: &Fixture,
    blobs: &Blobs,
    blank: bool,
    inline: Option<&str>,
) -> crate::promoted_blob_backfill::Candidate {
    let old = f
        .seed_texts(inline, b"unavailable target", "note", blobs)
        .await;
    blobs.objects.lock().unwrap().remove(&old.blob_key);
    let placeholder = if blank {
        String::new()
    } else {
        format!("{}/{}", f.graph, old.atom_id)
    };
    sqlx::query("UPDATE atoms SET content_blob_key=$2 WHERE atom_id=$1")
        .bind(old.atom_id)
        .bind(&placeholder)
        .execute(&f.owner)
        .await
        .unwrap();
    let source_graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id,name) VALUES($1,'promotion source') RETURNING id",
    )
    .bind(f.account)
    .fetch_one(&f.owner)
    .await
    .unwrap();
    let source_key = coralswarm_core::r2::blob_key_for(
        f.tenant.then_some("org_replacement"),
        source_graph,
        Uuid::new_v4(),
    )
    .unwrap();
    sqlx::query("INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,timestamp) VALUES($1,'note',$2,$3,now())")
        .bind(source_graph).bind(old.atom_id.to_string()).bind(&source_key)
        .execute(&f.owner).await.unwrap();
    blobs
        .objects
        .lock()
        .unwrap()
        .insert(source_key, b"source bytes".to_vec());
    crate::promoted_blob_backfill::Candidate {
        atom_id: old.atom_id,
        graph_id: f.graph,
        account_id: f.account,
        source_type: "note".into(),
        source_id: old.atom_id.to_string(),
        content_blob_key: placeholder,
        candidate_source_graphs: vec![source_graph],
        has_inline: inline.is_some(),
    }
}

async fn promotion_repairs(tenant: bool) {
    use crate::{
        promoted_blob_backfill::{process_candidate, OutcomeKind},
        tenancy::TenantId,
    };
    let f = Fixture::new(tenant).await;
    let tenant_id = if tenant {
        TenantId::Org("org_replacement".into())
    } else {
        TenantId::Consumer
    };
    let org = tenant.then_some("org_replacement");
    let dek_cache = tokio::sync::Mutex::new(HashMap::from([(f.account, [7; 32])]));
    let existing = Blobs::default();
    let existing_candidate = promotion_candidate(&f, &existing, false, None).await;
    existing.objects.lock().unwrap().insert(
        existing_candidate.content_blob_key.clone(),
        b"existing target".to_vec(),
    );
    let skipped = process_candidate(
        &f.owner,
        &existing,
        org,
        &tenant_id,
        &dek_cache,
        &existing_candidate,
        true,
    )
    .await;
    assert_eq!(skipped.kind, OutcomeKind::SkippedExisting);
    assert!(existing.puts.lock().unwrap().is_empty());
    assert_eq!(
        existing
            .objects
            .lock()
            .unwrap()
            .get(&existing_candidate.content_blob_key)
            .unwrap(),
        b"existing target"
    );
    assert_eq!(f.counts(existing_candidate.atom_id).await, (0, 0));
    for (blank, inline) in [
        (false, None),
        (true, None),
        (false, Some("current inline differs")),
    ] {
        let blobs = Blobs::default();
        let candidate = promotion_candidate(&f, &blobs, blank, inline).await;
        let before = f.snapshot(candidate.atom_id).await;
        let result = process_candidate(
            &f.owner, &blobs, org, &tenant_id, &dek_cache, &candidate, true,
        )
        .await;
        assert_eq!(result.kind, OutcomeKind::PutOk, "{}", result.report);
        let after = f.snapshot(candidate.atom_id).await;
        assert_ne!(after.blob_key, before.blob_key);
        assert_ne!(
            after.blob_key,
            coralswarm_core::r2::blob_key_for(org, f.graph, candidate.atom_id).unwrap()
        );
        assert_eq!(after.revision, before.revision + 1);
        assert_eq!(after.inline.as_deref(), inline);
        assert_eq!(after.header_len, before.header_len);
        assert_eq!(f.counts(candidate.atom_id).await, (1, 1));
        assert_eq!(
            f.cloud_state(candidate.atom_id).await,
            (true, None, true, Some("local-old".into()))
        );
        assert_eq!(
            blobs.puts.lock().unwrap().as_slice(),
            &[after.blob_key.clone()]
        );
        assert_eq!(
            blobs.objects.lock().unwrap().get(&after.blob_key).unwrap(),
            inline.unwrap_or("source bytes").as_bytes()
        );
        let replay = process_candidate(
            &f.owner, &blobs, org, &tenant_id, &dek_cache, &candidate, true,
        )
        .await;
        assert_eq!(replay.kind, OutcomeKind::SkippedExisting);
        assert_eq!(blobs.puts.lock().unwrap().len(), 1);
    }
    for pointer_edit in [false, true] {
        let uploaded = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let blobs = Blobs {
            uploaded: Some(uploaded.clone()),
            resume: Some(resume.clone()),
            ..Default::default()
        };
        let candidate = promotion_candidate(&f, &blobs, false, Some("target inline")).await;
        let competing_key =
            coralswarm_core::r2::blob_key_for(org, f.graph, Uuid::new_v4()).unwrap();
        let repair = process_candidate(
            &f.owner, &blobs, org, &tenant_id, &dek_cache, &candidate, true,
        );
        let edit = async {
            uploaded.wait().await;
            blobs.objects.lock().unwrap().insert(
                candidate.content_blob_key.clone(),
                b"concurrent old object".to_vec(),
            );
            if pointer_edit {
                blobs
                    .objects
                    .lock()
                    .unwrap()
                    .insert(competing_key.clone(), b"winning body".to_vec());
                sqlx::query("UPDATE atoms SET content_blob_key=$2,content_inline='winning body' WHERE atom_id=$1")
                    .bind(candidate.atom_id).bind(&competing_key).execute(&f.owner).await.unwrap();
            } else {
                sqlx::query("UPDATE atoms SET content_inline='winning inline' WHERE atom_id=$1")
                    .bind(candidate.atom_id)
                    .execute(&f.owner)
                    .await
                    .unwrap();
            }
            resume.wait().await;
        };
        let (result, ()) = tokio::join!(repair, edit);
        assert_eq!(
            result.kind,
            OutcomeKind::SkippedExisting,
            "{}",
            result.report
        );
        let after = f.snapshot(candidate.atom_id).await;
        assert_eq!(
            after.inline.as_deref(),
            Some(if pointer_edit {
                "winning body"
            } else {
                "winning inline"
            })
        );
        assert_eq!(
            after.blob_key,
            if pointer_edit {
                competing_key
            } else {
                candidate.content_blob_key.clone()
            }
        );
        assert_eq!(f.counts(candidate.atom_id).await, (0, 0));
        assert_eq!(
            blobs
                .objects
                .lock()
                .unwrap()
                .get(&candidate.content_blob_key)
                .unwrap(),
            b"concurrent old object"
        );
        let puts = blobs.puts.lock().unwrap();
        assert_eq!(puts.len(), 1);
        assert_ne!(puts[0], candidate.content_blob_key);
        assert_eq!(blobs.deletes.lock().unwrap().as_slice(), puts.as_slice());
        assert!(!blobs.objects.lock().unwrap().contains_key(&puts[0]));
    }
    f.finish().await;
}

#[tokio::test]
async fn consumer_promotion_repair_uses_fresh_pointer_and_cas() {
    promotion_repairs(false).await;
}

#[tokio::test]
async fn tenant_promotion_repair_uses_fresh_pointer_and_cas() {
    promotion_repairs(true).await;
}

#[tokio::test]
async fn consumer_owned_content_replacement() {
    exercise(false).await;
}

#[tokio::test]
async fn tenant_owned_content_replacement() {
    exercise(true).await;
}
