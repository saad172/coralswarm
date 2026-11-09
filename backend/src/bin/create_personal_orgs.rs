//! `create_personal_orgs` — mint the Clerk organization that stands for one
//! person's own ocean, and record it in `personal_orgs`.
//!
//! ## Why this exists
//!
//! Clerk draws the OAuth consent screen, and that screen lists ONLY
//! organizations — it has no row for a person's own ocean. So a user
//! connecting an MCP client for their own ocean reaches a picker that cannot
//! express what they want. The founder's fix (2026-09-07) is to give every
//! user a Clerk organization that STANDS FOR their own ocean, purely so the
//! picker has a row for it. Migration
//! `0208_personal_orgs.sql` is the record of which organization stands for
//! whom; `tenancy::personal_org` is the request-path half that translates
//! such an org id back to the caller's ordinary personal path. This binary is
//! the third piece: the thing that actually creates one.
//!
//! A personal org gets NO tenant database and NO routing row — that is
//! migration `0208`'s stated design, not an omission here. This binary
//! therefore writes exactly two things per user: one Clerk organization, and
//! one `personal_orgs` row. It never touches `accounts`, `tenant_databases`,
//! or any tenant plane.
//!
//! ## This is a mechanism, not a backfill
//!
//! Explicit founder constraint, 2026-09-07: build the mechanism, create
//! nothing. Two things enforce that here rather than leaving it to
//! discipline:
//!
//!   * **Dry run is the default.** `--execute` opts in, exactly as in
//!     `grant_plan`, `backfill_billing_enforcement` and
//!     `backfill_reef_judgement_from_history`. A mistyped invocation prints a
//!     report; it does not create organizations in a production identity
//!     provider.
//!   * **There is no default target.** Exactly one of `--user <clerk_user_id>`
//!     or `--all` is REQUIRED. Every sibling one-shot in this directory
//!     treats "no narrowing flag" as "the whole table", which is the right
//!     default for a backfill and the wrong one here: this tool's unnarrowed
//!     form IS a backfill, and nobody has asked for one. Making it
//!     unreachable without typing `--all` means the whole-instance run can
//!     only ever be deliberate.
//!
//! ## What one pass does, per user
//!
//! 1. **Skip if already recorded.** A `personal_orgs` row for this
//!    `clerk_user_id` means the job is done — reported `already-present`,
//!    with zero calls to Clerk and zero writes. This is the ordinary
//!    idempotency path; see "Idempotent" below.
//! 2. **Read the user from Clerk** (`get_user`) and derive a display name via
//!    [`personal_org_name`]. Skipped when the user object is already in hand,
//!    which it is on the `--all` path (that listing returns full user
//!    objects), so `--all` costs one Clerk read per PAGE, not per user.
//! 3. **Reconcile before creating.** `list_user_organizations` is scanned for
//!    an organization this binary already minted for this user — recognised
//!    by the [`MARKER_KEY`] tag in its Clerk `private_metadata`. If one is
//!    found, it is ADOPTED (step 4 records it) instead of a second one being
//!    created. This step is what closes the ordering hazard below; on a
//!    healthy first run it simply finds nothing.
//! 4. **Create**, if step 3 found nothing: `create_organization(name,
//!    clerk_user_id, marker)`. Clerk's `created_by` makes that user the org's
//!    administrator in the same request — see
//!    [`coralswarm_backend::clerk_admin::ClerkAdminClient::create_organization`]'s
//!    doc comment for what was verified against Clerk's own API spec — so no
//!    separate membership call follows.
//! 5. **Record it:** `INSERT INTO personal_orgs (clerk_org_id, clerk_user_id)
//!    VALUES ($1,$2) ON CONFLICT DO NOTHING`.
//!
//! ## The ordering hazard, and the approach chosen for it
//!
//! Step 4 creates state in a system this database does not control, and step
//! 5 is what records it. If step 5 fails — a dropped connection, a killed
//! process, anything — a Clerk organization exists that nothing points at.
//! The naive consequence is worse than the orphan itself: on the next run,
//! step 1 finds no row, and the tool mints a SECOND organization for that
//! user. Two identically-named rows in their picker, one of which is a ghost.
//!
//! **Chosen: reconcile-before-create, keyed by a marker this binary writes
//! into Clerk itself (step 3).** Every organization created here carries
//! `private_metadata = {"coralswarm_personal_org": "<clerk_user_id>"}`.
//! Before creating anything, the user's own organizations are scanned for
//! that exact tag. So the orphan from an interrupted run is not garbage to be
//! cleaned up — it is the answer, found and adopted. `private_metadata` is
//! Backend-API-only (invisible to, and unwritable from, the Frontend API), so
//! the marker cannot be forged from a browser to make this tool adopt an
//! attacker-controlled organization.
//!
//! Why not the two alternatives:
//!
//!   * **A local reservation row first.** `personal_orgs` has three columns
//!     and `clerk_org_id` is the primary key, so reserving the user's slot
//!     before Clerk answers means writing a placeholder id (`pending:...`)
//!     into the column the auth spine reads. That inverts the failure: a
//!     crash now leaves a row that claims an organization exists when none
//!     does, and step 1 would report that user `already-present` forever. It
//!     also puts a value that is not a Clerk organization id into a table
//!     whose migration header states that a row means "org X denotes the
//!     personal ocean of user Y". Rejected.
//!   * **Compensating delete on insert-conflict.** Deleting the
//!     just-created organization when step 5 fails only helps when this
//!     process survives to run the compensation. `SIGKILL`, a machine dying,
//!     or the delete itself failing all leave the same orphan — so the
//!     re-run path still has to be safe, and once it is, the delete is
//!     redundant. It is also strictly more dangerous: a mis-targeted delete
//!     destroys a real organization, whereas a mis-targeted adopt is
//!     prevented by the marker carrying the user's own id.
//!
//! What reconcile-before-create does NOT cover, stated plainly: if step 5
//! fails and the user's row is later written by something else pointing at a
//! DIFFERENT organization, the orphan is stranded — step 1 short-circuits
//! before step 3 ever scans. That needs two writers, which this
//! founder-operated one-shot does not have. The insert path detects the
//! conflict, reports the orphan's id as `STRANDED` with the remediation
//! spelled out, and forces a non-zero exit rather than letting it pass
//! silently.
//!
//! ## Idempotent
//!
//! A second pass over the same users writes nothing and reports every one of
//! them `already-present`. Three independent mechanisms hold that up, in
//! order of which one fires first:
//!
//!   * step 1's `SELECT` (the ordinary path — no Clerk call at all);
//!   * step 3's marker reconcile (the interrupted-run path — finds the
//!     existing organization instead of creating another);
//!   * the `clerk_user_id` UNIQUE constraint plus `ON CONFLICT DO NOTHING`
//!     (the last-resort path — two concurrent runs cannot both record a row).
//!
//! ## Creating an organization CHANGES THE USER'S ACTIVE ORGANIZATION
//!
//! This is not a side note. Clerk's own Backend API documentation for
//! `CreateOrganization` states that the `created_by` user
//!
//! > will see this as their **active organization the next time they create a
//! > session**, presuming they don't explicitly set a different organization
//! > as active before then.
//!
//! So running this tool is visible to the user. Every account it touches has
//! its next web session start in the newly-created personal organization.
//! For a personal user that resolves back to their own ocean — which is where
//! they already were — but it is a real tenancy change made on somebody's
//! behalf, and `--all` makes it for EVERY user at once.
//!
//! Run `--user` for one account first, sign in, and look at what happened
//! before considering `--all`.
//!
//! ## Dry run
//!
//! `--execute` is required to write. Without it this binary issues only
//! `SELECT`s against Postgres and only `GET`s against Clerk, and prints, per
//! user, exactly which branch an `--execute` run would take —
//! `already-present`, `would-adopt` (naming the organization it found), or
//! `would-create` (naming the display name it would use). No `POST`, no
//! `INSERT`.
//!
//! ## Usage
//!
//! ```text
//! create_personal_orgs (--user <clerk_user_id> | --all) [--execute]
//! ```
//!
//! `--user` takes the CLERK user id (`user_...`), not our internal
//! `users.id` UUID — `personal_orgs.clerk_user_id` stores the Clerk id, for
//! the reasons migration `0208`'s header gives. The prefix is validated
//! ([`validate_clerk_user_id`]) precisely because a pasted UUID would
//! otherwise sail through, find no row, fail `get_user` at Clerk, and look
//! like a Clerk outage rather than a typo.
//!
//! ## Env
//!
//!   MIGRATOR_DATABASE_URL / DATABASE_URL   control-plane DSN, resolved via
//!       [`coralswarm_backend::migrator_database_url`] (`MIGRATOR_DATABASE_URL`
//!       preferred, `DATABASE_URL` as fallback — the same precedence
//!       `migrate_control`/`backfill_billing_enforcement` use). `personal_orgs`
//!       carries no RLS policy (migration `0208`'s header says why), so an
//!       elevated role is not required; it is honored when set since it is a
//!       strict superset of what this needs.
//!   CLERK_SECRET_KEY   required. Without it there is no Clerk client and
//!       this binary exits 2 — including in dry-run mode, which reads Clerk
//!       and would otherwise report a fabricated plan.
//!   CLERK_API_BASE_URL   optional override (`clerk_admin::api_base_url`).
//!
//! ## Exit codes
//!
//!   0  every selected user was processed with zero errors and zero stranded
//!      organizations.
//!   1  a database/connection error, at least one per-user error, or at least
//!      one STRANDED organization (created in Clerk, not recorded here). The
//!      run does not stop at the first failure — one bad user must not strand
//!      the rest — and re-running is safe.
//!   2  usage/argument error, no control DSN, or no `CLERK_SECRET_KEY`.

use std::process::ExitCode;

use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use coralswarm_backend::clerk_admin::{ClerkAdminClient, ClerkUser, OrganizationInfo};
use coralswarm_backend::migrator_database_url;

const USAGE: &str = "usage: create_personal_orgs (--user <clerk_user_id> | --all) [--execute]";

/// The `private_metadata` key every organization this binary creates is
/// tagged with, whose value is the `clerk_user_id` the organization stands
/// for. This is the durable, Clerk-side record that survives a crash between
/// "created the org" and "recorded the org" — see the module doc's ordering-
/// hazard section.
///
/// Deliberately defined HERE and not in `clerk_admin`: the marker is a fact
/// about this feature, not about Clerk, and `clerk_admin`'s own module doc
/// describes it as a thin general-purpose typed client. It stays a plain
/// `private_metadata` bag there.
const MARKER_KEY: &str = "coralswarm_personal_org";

/// Appended to the identity in [`personal_org_name`], so a picker row reads
/// `"Alex Example's Organization"` rather than `"Alex Example"`.
///
/// Always `'s`, including after a name already ending in `s` ("Chris's
/// Organization"). Style guides disagree about that case; picking one rule and
/// applying it everywhere beats a branch whose output nobody can predict.
const ORG_NAME_SUFFIX: &str = "'s Organization";

/// Clerk's documented maximum for an organization `name`
/// (`CreateOrganization`: "Max length: 256"). [`personal_org_name`] truncates
/// to this so a pathological Clerk profile cannot turn a create into a 422.
const MAX_ORG_NAME_CHARS: usize = 256;

/// Who this run is about. There is no `All`-by-default: see the module doc's
/// "This is a mechanism, not a backfill".
#[derive(Debug, PartialEq, Eq)]
enum Target {
    /// One Clerk user id.
    OneUser(String),
    /// Every user in the Clerk instance.
    AllUsers,
}

#[derive(Debug)]
struct Args {
    execute: bool,
    target: Target,
}

/// Takes `impl Iterator<Item = String>` (rather than reading
/// `std::env::args()` itself) so the tests below exercise this EXACT function
/// against a synthetic argv, instead of a hand-copied restatement of the flag
/// loop that could drift from the real one silently — the same reason
/// `backfill_billing_enforcement::parse_args` takes an iterator.
fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut execute = false;
    let mut user: Option<String> = None;
    let mut all = false;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--execute" => execute = true,
            "--all" => all = true,
            "--user" => {
                let v = args.next().ok_or("--user requires a value")?;
                validate_clerk_user_id(&v)?;
                user = Some(v);
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let target = match (user, all) {
        (Some(_), true) => {
            return Err("--user and --all are mutually exclusive; pass exactly one".to_string())
        }
        (Some(u), false) => Target::OneUser(u),
        (None, true) => Target::AllUsers,
        (None, false) => {
            return Err(
                "exactly one of --user <clerk_user_id> or --all is required — this tool has no \
                 default target, because its default would be a backfill nobody asked for"
                    .to_string(),
            )
        }
    };
    Ok(Args { execute, target })
}

/// A Clerk user id is `user_...`. Rejecting anything else at the argument
/// boundary is not cosmetic: `personal_orgs.clerk_user_id` stores the CLERK
/// id, not our internal `users.id` UUID (migration `0208`'s header is
/// explicit about it), and a pasted UUID is the obvious mistake. Unvalidated,
/// it would find no `personal_orgs` row, then fail `get_user` at Clerk, and
/// present as a Clerk problem instead of a typo.
fn validate_clerk_user_id(v: &str) -> Result<(), String> {
    if v.starts_with("user_") && v.len() > "user_".len() {
        Ok(())
    } else {
        Err(format!(
            "--user expects a CLERK user id (`user_...`), got {v:?} — note this is the Clerk \
             subject, not our internal users.id UUID"
        ))
    }
}

/// The display name a personal organization gets in Clerk's picker.
///
/// This string is what the person sees in an OAuth consent screen next to
/// their real organizations, so it should read like a row about them.
///
/// The identity below is suffixed with [`ORG_NAME_SUFFIX`], giving
/// `"Alex Example's Organization"` rather than a bare `"Alex Example"`
/// (founder decision, 2026-09-08). The picker lists organizations, so a row
/// showing only a person's name reads as a person, not as a place their
/// content lives — next to real org rows like "Acme" that is a confusing
/// thing to click.
///
/// The identity chain, first non-blank wins:
///
///   1. `"<first> <last>"` from Clerk's `first_name`/`last_name` — either
///      alone is fine (a profile with only a first name yields just that).
///   2. The local part of their primary email address (`ada` from
///      `ada@example.com`). Chosen over the full address because a picker row
///      reading `ada@example.com` looks like an invitation, not an ocean; and
///      the address itself is PII this call would otherwise push into an
///      organization name that other people can be shown.
///   3. The Clerk user id. Never pretty, but never blank — and a blank name
///      is a 422 from Clerk, i.e. a user this tool could never serve.
///
/// `username` was considered as a step between 1 and 2 and left out: it is
/// unset on this instance's sign-in configuration, so it would add a branch
/// no test could exercise against real data.
///
/// The IDENTITY is trimmed and truncated first, to
/// `MAX_ORG_NAME_CHARS - ORG_NAME_SUFFIX.chars().count()` CHARACTERS (not
/// bytes — `&s[..256]` panics mid-codepoint on a non-ASCII name, and
/// non-ASCII names are ordinary), and only then is the suffix appended. Doing
/// it the other way round — truncating the finished string — would silently
/// eat the suffix off a long name, and truncating to the full 256 before
/// appending would push the result PAST Clerk's documented limit and 422.
fn personal_org_name(user: &ClerkUser) -> String {
    let names: Vec<&str> = [user.first_name.as_deref(), user.last_name.as_deref()]
        .into_iter()
        .flatten()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    let candidate = if !names.is_empty() {
        names.join(" ")
    } else if let Some(local) = user.primary_email().map(email_local_part) {
        local.to_string()
    } else {
        String::new()
    };

    let candidate = candidate.trim();
    let identity = if candidate.is_empty() {
        user.id.clone()
    } else {
        let cap = MAX_ORG_NAME_CHARS - ORG_NAME_SUFFIX.chars().count();
        let truncated: String = candidate.chars().take(cap).collect();
        let truncated = truncated.trim().to_string();
        if truncated.is_empty() {
            user.id.clone()
        } else {
            truncated
        }
    };
    format!("{identity}{ORG_NAME_SUFFIX}")
}

/// `ada` from `ada@example.com`. Splits on the LAST `@`, not the first: a
/// quoted local part may legally contain one, and a domain never can. An
/// address with no `@` at all is not something to be clever about — it is
/// returned whole, and [`personal_org_name`]'s blank check then decides
/// whether it was usable.
fn email_local_part(email: &str) -> &str {
    match email.rsplit_once('@') {
        Some((local, _domain)) => local,
        None => email,
    }
}

/// The `private_metadata` written on create.
fn marker(clerk_user_id: &str) -> Value {
    json!({ MARKER_KEY: clerk_user_id })
}

/// Whether `org` is an organization THIS binary minted for `clerk_user_id`.
///
/// The marker's VALUE must equal the user id, not merely exist: that is what
/// makes a stray or unrelated tag unable to cause a wrong adoption, and it is
/// also why `list_user_organizations` needs no defense of its own against a
/// Clerk-side filter being ignored (see that function's doc comment).
/// `created_by` is deliberately NOT used as the key — a user who creates
/// their own real organization through Clerk's UI is its `created_by` too.
fn is_personal_org_for(org: &OrganizationInfo, clerk_user_id: &str) -> bool {
    org.private_metadata
        .get(MARKER_KEY)
        .and_then(|v| v.as_str())
        .is_some_and(|v| v == clerk_user_id)
}

/// The `clerk_org_id` already recorded for this user, if any.
async fn recorded_org(pool: &PgPool, clerk_user_id: &str) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar("SELECT clerk_org_id FROM personal_orgs WHERE clerk_user_id = $1")
        .bind(clerk_user_id)
        .fetch_optional(pool)
        .await
}

/// Rows inserted: `1` on success, `0` when either unique constraint
/// (`clerk_org_id` PK or `clerk_user_id` UNIQUE) already held a row.
/// `ON CONFLICT DO NOTHING` with no conflict target on purpose — both
/// constraints mean "someone got here first", and the caller distinguishes
/// which by re-reading.
async fn record_org(
    pool: &PgPool,
    clerk_org_id: &str,
    clerk_user_id: &str,
) -> Result<u64, sqlx::Error> {
    let res = sqlx::query(
        "INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2) \
         ON CONFLICT DO NOTHING",
    )
    .bind(clerk_org_id)
    .bind(clerk_user_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Tallies across the whole run, printed as the final summary — the same
/// "counts, not just a pass/fail" discipline as
/// `backfill_billing_enforcement::Totals`.
#[derive(Default)]
struct Totals {
    users_seen: u64,
    /// A `personal_orgs` row already existed. The idempotent no-op.
    already_present: u64,
    /// Dry run only: would have created a new Clerk organization.
    would_create: u64,
    /// Dry run only: would have adopted an existing marked organization left
    /// by an interrupted run.
    would_adopt: u64,
    /// `--execute`: a Clerk organization was created by this pass.
    created: u64,
    /// `--execute`: a marked organization from an earlier interrupted run was
    /// reused instead of creating a second one.
    adopted: u64,
    /// `--execute`: a `personal_orgs` row was written.
    recorded: u64,
    /// `--execute`: a Clerk organization exists that this run could NOT
    /// record. Forces a non-zero exit — see the module doc's ordering-hazard
    /// section. A plain insert FAILURE is recoverable on re-run (step 3
    /// adopts); this counter is for the case the re-run cannot fix, plus the
    /// failure that produced it, so the operator sees the org id either way.
    stranded: u64,
    errors: u64,
}

/// One user, one pass. Never returns `Err`: a per-user failure is reported,
/// tallied, and the caller continues to the next user — same posture as
/// `backfill_billing_enforcement::process_account` ("one bad account must not
/// strand the rest").
///
/// `prefetched` is the already-fetched Clerk user object when the caller has
/// one (`--all` lists full user objects), so the per-user `get_user` is
/// skipped there. It is fetched lazily and only AFTER the `personal_orgs`
/// lookup, so an already-recorded user costs exactly one `SELECT` and no
/// Clerk traffic at all.
async fn process_user(
    pool: &PgPool,
    clerk: &ClerkAdminClient,
    clerk_user_id: &str,
    prefetched: Option<&ClerkUser>,
    execute: bool,
    totals: &mut Totals,
) {
    totals.users_seen += 1;

    // Step 1 — already recorded?
    match recorded_org(pool, clerk_user_id).await {
        Ok(Some(org_id)) => {
            println!("user={clerk_user_id}: already-present org={org_id}");
            totals.already_present += 1;
            return;
        }
        Ok(None) => {}
        Err(e) => {
            eprintln!("ERROR user={clerk_user_id}: personal_orgs lookup failed: {e}");
            totals.errors += 1;
            return;
        }
    }

    // Step 2 — the display name.
    let fetched;
    let user = match prefetched {
        Some(u) => u,
        None => match clerk.get_user(clerk_user_id).await {
            Ok(u) => {
                fetched = u;
                &fetched
            }
            Err(e) => {
                eprintln!("ERROR user={clerk_user_id}: Clerk get_user failed: {e}");
                totals.errors += 1;
                return;
            }
        },
    };
    let name = personal_org_name(user);

    // Step 3 — reconcile before creating. See the module doc's ordering-
    // hazard section: this is the whole reason a crash between "created" and
    // "recorded" cannot mint a second organization on the next run.
    let existing = match clerk.list_user_organizations(clerk_user_id).await {
        Ok(orgs) => orgs.into_iter().find(|o| is_personal_org_for(o, clerk_user_id)),
        Err(e) => {
            eprintln!("ERROR user={clerk_user_id}: Clerk list_user_organizations failed: {e}");
            totals.errors += 1;
            return;
        }
    };

    if !execute {
        match &existing {
            Some(org) => {
                println!(
                    "user={clerk_user_id}: would-adopt org={} name={:?} — already carries this \
                     tool's marker, so an earlier run created it and did not record it",
                    org.id, org.name
                );
                totals.would_adopt += 1;
            }
            None => {
                println!(
                    "user={clerk_user_id}: would-create org name={name:?}, then record it \
                     — NOTE: this becomes that user's ACTIVE organization at their next sign-in"
                );
                totals.would_create += 1;
            }
        }
        return;
    }

    // Step 4 — adopt or create.
    let org = match existing {
        Some(org) => {
            println!(
                "user={clerk_user_id}: adopting existing marked org={} (no second org created)",
                org.id
            );
            totals.adopted += 1;
            org
        }
        None => match clerk
            .create_organization(&name, clerk_user_id, Some(marker(clerk_user_id)))
            .await
        {
            Ok(org) => {
                totals.created += 1;
                org
            }
            Err(e) => {
                eprintln!("ERROR user={clerk_user_id}: Clerk create_organization failed: {e}");
                totals.errors += 1;
                return;
            }
        },
    };

    // Step 5 — record it. From here on a Clerk organization EXISTS, so every
    // failure path below has to say its id out loud.
    match record_org(pool, &org.id, clerk_user_id).await {
        Ok(1) => {
            println!(
                "user={clerk_user_id}: OK org={} name={:?}",
                org.id, org.name
            );
            totals.recorded += 1;
        }
        Ok(_) => {
            // Zero rows: one of the two unique constraints already held a
            // row, even though step 1 read none. Re-read to find out which,
            // because only one of the two answers is a problem.
            match recorded_org(pool, clerk_user_id).await {
                Ok(Some(existing_id)) if existing_id == org.id => {
                    println!(
                        "user={clerk_user_id}: OK org={} (recorded concurrently; nothing to do)",
                        org.id
                    );
                    totals.recorded += 1;
                }
                Ok(Some(other_id)) => {
                    // The `clerk_user_id` UNIQUE constraint fired: this user
                    // now has a row, pointing somewhere else. THE one case
                    // the marker reconcile cannot repair — step 1 will
                    // short-circuit on that row before step 3 ever scans, so
                    // a re-run will not adopt this org. Needs a human.
                    eprintln!(
                        "STRANDED user={clerk_user_id}: Clerk org {} exists but the \
                         personal_orgs row for this user now points at {other_id} — a \
                         concurrent writer won the insert. Re-running will NOT adopt {} (the \
                         row lookup short-circuits first); delete it in Clerk by hand, or \
                         repoint the row deliberately.",
                        org.id, org.id
                    );
                    totals.stranded += 1;
                    totals.errors += 1;
                }
                Ok(None) => {
                    // No row for this user, yet the insert conflicted: the
                    // `clerk_org_id` PRIMARY KEY fired instead, so this org
                    // id is already recorded as SOMEBODY ELSE's personal
                    // ocean. Unreachable for a freshly created org (the id
                    // is brand new); reachable only for an adopted one whose
                    // marker and whose recorded owner disagree, which means
                    // the table was edited by hand. Never paper over it.
                    eprintln!(
                        "STRANDED user={clerk_user_id}: Clerk org {} carries this user's \
                         marker, but personal_orgs already records that org id against a \
                         DIFFERENT user and this user still has no row. The marker and the \
                         table disagree about who owns {} — resolve by hand; this tool will \
                         not guess.",
                        org.id, org.id
                    );
                    totals.stranded += 1;
                    totals.errors += 1;
                }
                Err(e) => {
                    eprintln!(
                        "STRANDED user={clerk_user_id}: Clerk org {} exists, the insert wrote no \
                         row, and the follow-up read failed too: {e}. Re-run this tool for this \
                         user — it will find {} by its marker and adopt it.",
                        org.id, org.id
                    );
                    totals.stranded += 1;
                    totals.errors += 1;
                }
            }
        }
        Err(e) => {
            // The hazard, realised. Recoverable: the org carries the marker,
            // so the next run's step 3 finds and adopts it rather than
            // creating a second one. Say the id anyway — a log line is
            // cheaper than a Clerk dashboard search.
            eprintln!(
                "STRANDED user={clerk_user_id}: Clerk org {} was created but recording it \
                 failed: {e}. Re-run this tool for this user — step 3 will find {} by its \
                 marker and adopt it instead of creating a second org.",
                org.id, org.id
            );
            totals.stranded += 1;
            totals.errors += 1;
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    let dsn = match migrator_database_url() {
        Some(u) => u,
        None => {
            eprintln!("MIGRATOR_DATABASE_URL (preferred) or DATABASE_URL must be set");
            return ExitCode::from(2);
        }
    };
    // `from_env` before connecting: a missing CLERK_SECRET_KEY is a usage
    // error (exit 2), and finding that out after opening a pool would report
    // it behind whatever the connection did.
    let Some(clerk) = ClerkAdminClient::from_env() else {
        eprintln!("CLERK_SECRET_KEY must be set — this tool reads Clerk even in dry-run mode");
        return ExitCode::from(2);
    };
    let pool = match PgPoolOptions::new().max_connections(4).connect(&dsn).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("failed to connect: {e}");
            return ExitCode::FAILURE;
        }
    };

    println!(
        "create_personal_orgs — mode={}",
        if args.execute { "EXECUTE" } else { "DRY RUN" }
    );
    // Stated on EVERY run, in the binary itself — not only in
    // `scripts/create_personal_orgs.sh`. Anyone invoking `cargo run --bin
    // create_personal_orgs` directly skips the wrapper entirely, and the
    // wrapper's banner was the only place this appeared (CodeRabbit review,
    // PR #814). A consequence this size has to be where the operator's eyes
    // already are.
    println!(
        "  NOTE: Clerk makes a newly-created organization the `created_by` user's ACTIVE\n         \x20       organization at their next sign-in. Every account this touches has its next\n         \x20       web session start there."
    );
    if args.execute && matches!(args.target, Target::AllUsers) {
        println!(
            "  WARNING: --all --execute applies that change to EVERY user in the instance at\n             \x20         once. Run --user for one account and sign in as them first."
        );
    }

    let mut totals = Totals::default();
    match &args.target {
        Target::OneUser(id) => {
            println!("  narrowed to Clerk user {id}");
            process_user(&pool, &clerk, id, None, args.execute, &mut totals).await;
        }
        Target::AllUsers => {
            println!("  every user in the Clerk instance");
            let users = match clerk.list_all_users().await {
                Ok(u) => u,
                Err(e) => {
                    eprintln!("failed to enumerate Clerk users: {e}");
                    return ExitCode::FAILURE;
                }
            };
            if users.is_empty() {
                println!("no Clerk users found");
            }
            for user in &users {
                let id = user.id.clone();
                process_user(&pool, &clerk, &id, Some(user), args.execute, &mut totals).await;
            }
        }
    }

    println!(
        "summary: users={} already_present={} would_create={} would_adopt={} created={} \
         adopted={} recorded={} stranded={} errors={}",
        totals.users_seen,
        totals.already_present,
        totals.would_create,
        totals.would_adopt,
        totals.created,
        totals.adopted,
        totals.recorded,
        totals.stranded,
        totals.errors
    );
    if !args.execute {
        println!(
            "Nothing was written — no Clerk organizations were created and no rows were \
             inserted. Read the tallies above, then re-run with --execute to apply."
        );
    } else if totals.stranded > 0 {
        println!(
            "{} Clerk organization(s) were created but not recorded (see STRANDED lines above). \
             Re-running is safe and is the fix for all but the concurrent-writer case: each \
             org carries this tool's marker and will be adopted, never duplicated.",
            totals.stranded
        );
    }

    if totals.errors > 0 || totals.stranded > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(first: Option<&str>, last: Option<&str>, email: Option<&str>) -> ClerkUser {
        let email_addresses = match email {
            Some(e) => vec![coralswarm_backend::clerk_admin::ClerkEmailAddress {
                id: "idn_1".into(),
                email_address: e.into(),
                verification: None,
            }],
            None => vec![],
        };
        ClerkUser {
            id: "user_fallback".into(),
            last_sign_in_at: None,
            email_addresses,
            primary_email_address_id: email.map(|_| "idn_1".to_string()),
            first_name: first.map(str::to_string),
            last_name: last.map(str::to_string),
        }
    }

    // ── parse_args ──────────────────────────────────────────────────────

    #[test]
    fn user_and_execute_parse_together() {
        let argv = vec![
            "--execute".to_string(),
            "--user".to_string(),
            "user_abc".to_string(),
        ];
        let args = parse_args(argv.into_iter()).expect("must parse");
        assert!(args.execute);
        assert_eq!(args.target, Target::OneUser("user_abc".to_string()));
    }

    /// Dry run is the DEFAULT. The single most important argument-shape fact
    /// about this binary: `--user` alone must never write.
    #[test]
    fn execute_defaults_to_false() {
        let argv = vec!["--user".to_string(), "user_abc".to_string()];
        let args = parse_args(argv.into_iter()).expect("must parse");
        assert!(
            !args.execute,
            "dry run must be the default — a run without --execute may not write"
        );
    }

    #[test]
    fn all_parses_on_its_own() {
        let argv = vec!["--all".to_string()];
        let args = parse_args(argv.into_iter()).expect("must parse");
        assert_eq!(args.target, Target::AllUsers);
        assert!(!args.execute);
    }

    /// No target is a usage ERROR, not an implied `--all`. See the module
    /// doc: the unnarrowed run is a backfill nobody asked for.
    #[test]
    fn no_target_is_a_usage_error() {
        let err = parse_args(std::iter::empty()).expect_err("must require a target");
        assert!(
            err.contains("--user") && err.contains("--all"),
            "the error must name both flags: {err:?}"
        );
    }

    /// ...and `--execute` alone must not become "execute against everyone".
    #[test]
    fn execute_without_a_target_is_a_usage_error() {
        let argv = vec!["--execute".to_string()];
        let err = parse_args(argv.into_iter()).expect_err("must require a target");
        assert!(err.contains("--all"), "{err:?}");
    }

    #[test]
    fn user_and_all_together_are_rejected() {
        let argv = vec![
            "--user".to_string(),
            "user_abc".to_string(),
            "--all".to_string(),
        ];
        let err = parse_args(argv.into_iter()).expect_err("must reject both at once");
        assert!(err.contains("mutually exclusive"), "{err:?}");
    }

    #[test]
    fn an_unknown_flag_is_rejected() {
        let argv = vec!["--bogus".to_string()];
        let err = parse_args(argv.into_iter()).expect_err("must reject an unknown flag");
        assert!(err.contains("--bogus"), "{err:?}");
    }

    #[test]
    fn user_without_a_value_is_a_usage_error() {
        let argv = vec!["--user".to_string()];
        let err = parse_args(argv.into_iter()).expect_err("must reject a missing value");
        assert!(err.contains("--user"), "{err:?}");
    }

    // ── validate_clerk_user_id ──────────────────────────────────────────

    #[test]
    fn a_clerk_user_id_is_accepted() {
        validate_clerk_user_id("user_2abcDEF").expect("a real Clerk id must be accepted");
    }

    /// The mistake this guard exists for: `personal_orgs.clerk_user_id` holds
    /// the CLERK id, not our internal UUID.
    #[test]
    fn an_internal_uuid_is_rejected_as_a_user_id() {
        let err = parse_args(
            vec![
                "--user".to_string(),
                "00000000-0000-0000-0000-000000000001".to_string(),
            ]
            .into_iter(),
        )
        .expect_err("a users.id UUID must not be accepted as a Clerk user id");
        assert!(err.contains("user_"), "{err:?}");
    }

    #[test]
    fn a_bare_prefix_with_no_id_after_it_is_rejected() {
        validate_clerk_user_id("user_").expect_err("`user_` alone is not an id");
    }

    #[test]
    fn an_org_id_is_rejected_as_a_user_id() {
        validate_clerk_user_id("org_2abc").expect_err("an org id is not a user id");
    }

    // ── personal_org_name ───────────────────────────────────────────────

    #[test]
    fn a_full_name_wins() {
        assert_eq!(
            personal_org_name(&user(Some("Ada"), Some("Lovelace"), Some("ada@example.com"))),
            "Ada Lovelace's Organization"
        );
    }

    #[test]
    fn a_first_name_alone_is_used() {
        assert_eq!(
            personal_org_name(&user(Some("Ada"), None, Some("ada@example.com"))),
            "Ada's Organization"
        );
    }

    #[test]
    fn a_last_name_alone_is_used() {
        assert_eq!(
            personal_org_name(&user(None, Some("Lovelace"), Some("ada@example.com"))),
            "Lovelace's Organization"
        );
    }

    /// A Clerk profile with `first_name: ""` (or whitespace) is common and
    /// must NOT produce a blank-ish name — it has to fall through.
    #[test]
    fn whitespace_only_names_fall_through_to_the_email_local_part() {
        assert_eq!(
            personal_org_name(&user(Some("  "), Some("\t"), Some("ada@example.com"))),
            "ada's Organization"
        );
    }

    #[test]
    fn with_no_names_the_email_local_part_is_used() {
        assert_eq!(
            personal_org_name(&user(None, None, Some("ada.lovelace@example.com"))),
            "ada.lovelace's Organization"
        );
    }

    #[test]
    fn with_no_names_and_no_email_the_clerk_user_id_is_used() {
        let u = user(None, None, None);
        assert_eq!(personal_org_name(&u), "user_fallback's Organization");
    }

    /// A blank email cannot yield a blank organization name — Clerk rejects
    /// those, so it has to keep falling through to the id.
    #[test]
    fn a_blank_email_still_falls_through_to_the_clerk_user_id() {
        assert_eq!(
            personal_org_name(&user(None, None, Some(""))),
            "user_fallback's Organization"
        );
    }

    /// The FINISHED name — identity plus suffix — must fit Clerk's limit, and
    /// the suffix must survive. Truncating after appending would silently eat
    /// the suffix off a long name; truncating the identity to the full 256
    /// before appending would push the result past the limit and 422.
    #[test]
    fn an_overlong_name_is_truncated_to_clerks_limit_with_the_suffix_intact() {
        let long = "a".repeat(400);
        let name = personal_org_name(&user(Some(&long), None, None));
        assert_eq!(
            name.chars().count(),
            MAX_ORG_NAME_CHARS,
            "Clerk's CreateOrganization documents a 256-character maximum for `name`"
        );
        assert!(
            name.ends_with(ORG_NAME_SUFFIX),
            "truncation must not eat the suffix — got: {name}"
        );
        // The identity really was cut, rather than the whole thing happening
        // to fit: 400 'a's cannot survive alongside the suffix.
        assert_eq!(
            name.chars().filter(|c| *c == 'a').count(),
            MAX_ORG_NAME_CHARS - ORG_NAME_SUFFIX.chars().count()
                + ORG_NAME_SUFFIX.matches('a').count(),
            "the identity half must be cut to exactly the room the suffix leaves"
        );
    }

    /// `&s[..256]` would panic here. Multi-byte names are ordinary, not an
    /// edge case, so truncation counts characters.
    #[test]
    fn truncation_does_not_split_a_multibyte_character() {
        let long = "é".repeat(400);
        let name = personal_org_name(&user(Some(&long), None, None));
        assert_eq!(name.chars().count(), MAX_ORG_NAME_CHARS);
        assert!(name.ends_with(ORG_NAME_SUFFIX));
        assert!(
            name.trim_end_matches(ORG_NAME_SUFFIX).chars().all(|c| c == 'é'),
            "no replacement/partial chars in the identity half: {name}"
        );
    }

    // ── email_local_part ────────────────────────────────────────────────

    #[test]
    fn the_local_part_is_everything_before_the_last_at() {
        assert_eq!(email_local_part("ada@example.com"), "ada");
        // A quoted local part may legally contain `@`; the domain never can,
        // so splitting from the right is the correct direction.
        assert_eq!(email_local_part("\"a@b\"@example.com"), "\"a@b\"");
    }

    #[test]
    fn a_string_with_no_at_is_returned_whole() {
        assert_eq!(email_local_part("nonsense"), "nonsense");
    }

    // ── the marker ──────────────────────────────────────────────────────

    #[test]
    fn the_marker_carries_the_user_id_under_the_documented_key() {
        assert_eq!(marker("user_ada"), json!({"coralswarm_personal_org": "user_ada"}));
    }

    fn org_with(private_metadata: Value) -> OrganizationInfo {
        OrganizationInfo {
            id: "org_1".into(),
            name: "Ada".into(),
            slug: None,
            private_metadata,
        }
    }

    #[test]
    fn an_org_marked_for_this_user_is_recognised() {
        let org = org_with(marker("user_ada"));
        assert!(is_personal_org_for(&org, "user_ada"));
    }

    /// THE security-relevant case. `list_user_organizations` returns whatever
    /// Clerk hands back; if a marker naming a DIFFERENT user could be adopted,
    /// this tool would record someone else's organization as this person's
    /// ocean — and migration 0208's whole point is that the row is a
    /// selector for exactly one person.
    #[test]
    fn an_org_marked_for_a_different_user_is_not_recognised() {
        let org = org_with(marker("user_someone_else"));
        assert!(
            !is_personal_org_for(&org, "user_ada"),
            "a marker naming another user must never match"
        );
    }

    #[test]
    fn an_unmarked_org_is_not_recognised() {
        // A real organization the user created themselves: no marker at all.
        assert!(!is_personal_org_for(&org_with(json!({})), "user_ada"));
        // `private_metadata` absent from the response entirely.
        assert!(!is_personal_org_for(&org_with(Value::Null), "user_ada"));
        // Present under the right key but not a string — must not match, and
        // must not panic.
        assert!(!is_personal_org_for(
            &org_with(json!({MARKER_KEY: {"nested": "user_ada"}})),
            "user_ada"
        ));
        assert!(!is_personal_org_for(
            &org_with(json!({MARKER_KEY: Value::Null})),
            "user_ada"
        ));
    }

    /// The marker must be a whole-value match, never a prefix — otherwise
    /// `user_ada` would match an org marked for `user_ada2`.
    #[test]
    fn the_marker_match_is_exact_not_a_prefix() {
        let org = org_with(marker("user_ada2"));
        assert!(!is_personal_org_for(&org, "user_ada"));
    }
}
