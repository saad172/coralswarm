//! Bound real title/receipt work without truncating admitted-text search.
use super::*;

async fn check(org: bool) {
    let f = Fixture::new(org).await;
    sqlx::query("INSERT INTO conversations(id,account_id,graph_id,user_id,title,pinned,shared,updated_at) SELECT md5('bounded-'||i)::uuid,$1,$2,CASE WHEN i<=600 THEN $3 ELSE $4 END,'Uncertified',i=600,i>600,'2100-01-01'::timestamptz-i*interval '1 minute' FROM generate_series(1,1000) i")
        .bind(f.account).bind(f.graph).bind(f.user).bind(f.other)
        .execute(&f.owner).await.unwrap();
    sqlx::query("INSERT INTO conversation_messages(conversation_id,seq,role,text) SELECT md5('bounded-'||i)::uuid,1,'user',CASE WHEN i=599 THEN 'Bounded Needle' ELSE 'Bounded question '||i END FROM generate_series(1,1000) i")
        .execute(&f.owner).await.unwrap();

    // Only the owner enables transaction-local function statistics. Actual
    // listing runs as the real nonowner/NOBYPASSRLS application login.
    let role: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&f.app)
        .await
        .unwrap();
    assert!(role.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    let mut tx = f.owner.begin().await.unwrap();
    sqlx::query("SET LOCAL track_functions='all'")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(&format!("SET LOCAL ROLE {role}"))
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SELECT set_config('app.account_id',$1,true),set_config('app.user_id',$2,true)")
        .bind(f.account.to_string())
        .bind(f.user.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    let attrs: (bool, bool) =
        sqlx::query_as("SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(attrs, (false, false));
    async fn title_calls(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>) -> i64 {
        // The SQL title wrapper can inline; its real receipt authority function
        // is PL/pgSQL and runs once for each projected title.
        sqlx::query_scalar("SELECT COALESCE(pg_stat_get_xact_function_calls('public.library_answer_permitted(uuid)'::regprocedure),0)")
            .fetch_one(&mut **tx).await.unwrap()
    }
    let before = title_calls(&mut tx).await;
    let list: Value = sqlx::query_scalar(rm::chats::LIST_SQL)
        .bind(vec![f.graph])
        .bind(f.user)
        .bind(Some(f.graph))
        .bind(None::<String>)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(list["conversations"].as_array().unwrap().len(), 200);
    assert_eq!(list["shared"].as_array().unwrap().len(), 200);
    assert_eq!(list["conversations"][0]["title"], "Bounded question 600");
    assert_eq!(
        title_calls(&mut tx).await - before,
        400,
        "ordinary listing must project only its 200 owned and 200 shared rows"
    );

    let search: Value = sqlx::query_scalar(rm::chats::LIST_SQL)
        .bind(vec![f.graph])
        .bind(f.user)
        .bind(Some(f.graph))
        .bind(Some("%Bounded Needle%"))
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(search["conversations"].as_array().unwrap().len(), 1);
    assert_eq!(
        search["conversations"][0]["title"], "Bounded Needle",
        "search must retain admitted matches beyond the ordinary history window"
    );
    assert!(search["shared"].as_array().unwrap().is_empty());
    tx.rollback().await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn ordinary_history_bounds_titles_consumer() {
    check(false).await
}

#[tokio::test]
async fn ordinary_history_bounds_titles_tenant() {
    check(true).await
}
