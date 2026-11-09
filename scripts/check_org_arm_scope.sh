#!/usr/bin/env bash
#
# check_org_arm_scope.sh — the "an Org match-arm may not reach the database
# without opening a scope" grep-guard (tenant RLS Phase 2, after slices 1-4).
#
# ── THE DEFECT THIS EXISTS TO CATCH ─────────────────────────────────────────
#
# A worker function matches on a scope-discriminating enum. The Consumer arm
# opens a scoped transaction. The Org arm issues its query with no scope at
# all:
#
#     match scope {
#         JobScope::Consumer { pool, account_id } => {
#             let mut tx = begin_account_scoped(pool, *account_id).await?;
#             let out = store.get_node_digest_scoped(&mut tx, node_id).await?;
#             tx.commit().await?;
#             Ok(out)
#         }
#         JobScope::Org(_) => store.get_node_digest(node_id).await,   // <- BUG
#     }
#
# Every table those arms touch carries `migrations_tenant/0009_tenant_rls`'s
# `member_ocean_isolation` policy, resolved through
# `tenant_rls_visible_graph_ids()`. Without `app.tenant_actor` or
# `app.user_id`, that function does NOT return the empty set. It hides every
# `member` and `team` graph and leaves `org` and `personal` graphs visible:
# `tenant_rls_actor_user_id()` is NULL when unset (`0009:1084`), which makes
# the member conjunct NULL and the team `EXISTS` false, while both conjuncts
# are trivially true for the other two kinds. Both GUCs are
# `set_config(.., true)` — transaction-local — so a statement issued straight
# against a pooled `&PgPool` cannot carry either, whatever pool it came from.
#
# So the symptom at OPS-4-T (the non-BYPASSRLS `coralswarm_tenant_app` role)
# is PARTIAL, not empty: those reads see the org and personal oceans and skip
# every member and team ocean, those writes touch only the rows they can see —
# AND RETURN `Ok`. Silent, not loud, and a partial result is harder to notice
# than a zero one. `summary::resolve_meeting_attribution`, one of the real
# instances, decides who gets BILLED for a meeting, and a member whose atoms
# are invisible reads as "no attributable member".
#
# PRs #840 and #843 fixed 88 instances of exactly this. Nothing in the repo
# could see them, and that was demonstrated, not assumed: reverting
# `summary.rs::scoped_set_summary_status` to the ambient form left
# `cargo check` green, `scripts/check_tenant_rls_coverage.sh` green, and the
# `workers_maintenance_rls_pg` suite green.
#
# WHY THE EXISTING GATE CANNOT SEE IT. `check_tenant_rls_coverage.sh` asks a
# per-FILE question — "does this file still contain residual
# `begin_account_scoped(` calls, given its manifest status". This defect is an
# arm that calls NO scope primitive, in a file the manifest already classifies
# `exempt` or `converted`. There is nothing for that gate to grep. The two
# guards are complementary and neither subsumes the other.
#
# ── WHAT IT CHECKS ──────────────────────────────────────────────────────────
#
# For every `JobScope::Org(..) =>` and `TenantId::Org(..) =>` match arm under
# backend/src (BOTH enums — `workers/tombstone_purge.rs` used `TenantId::Org`
# and every `JobScope` grep missed it), the arm body is extracted by BALANCED
# DELIMITER MATCHING, not by line. Arms come in at least four syntactic
# shapes, and a line-anchored `=> store\.` pattern sees one of them. Measured
# on `workers/entity_resolution.rs` at the pre-fix tree: 20 `=> store.`,
# 5 `=> async {`, 5 `=> {`, 1 `=> pool`. The block forms call the store INSIDE
# the block, several lines down. Counting with a line-anchored grep gave 49
# where the truth was 63.
#
# An arm FAILS in either of two ways, and the verdict column says which.
#
#   UNSCOPED       — it reaches the database and its body names no scope
#                    opener at all.
#   SCOPE-BYPASSED — it names an opener AND still has a statement that runs
#                    outside the transaction that opener returns.
#
# BOTH VERDICTS FAIL THE GATE. Both are baselineable, with a reason. The
# report names the file, the arm, and — for SCOPE-BYPASSED — the LINE of each
# statement that escaped, together with the source text of that line.
#
# "Reaches the database" — any of:
#   * `sqlx::query` / `query_as` / `query_scalar` (fn or macro)
#   * `.fetch_one(` `.fetch_all(` `.fetch_optional(` `.fetch(` `.fetch_many(`
#     `.execute(`
#   * `.acquire(`
#   * a non-`_scoped` method call on a `store`-named receiver (`store.foo(`,
#     `t.store.foo(`, `self.store.foo(`) — deliberately NOT conditioned on the
#     `_scoped` twin already existing, because "the twin has not been written
#     yet" is the state the defect ships in, not a reason to allow it
#   * a non-`_scoped` method call whose `_scoped` twin IS declared in
#     `pub trait Store:` — catches store delegation through a receiver that is
#     not spelled `store`
#   * the arm's OWN pool binding used with a `*`/`&` sigil, i.e. handed to
#     something as an executor: `JobScope::Org(pool) => Ok(load_atom(*pool,
#     atom_id).await?)`
#
# EVERY store call in the arm is examined, not just the first one. Reading
# only the first match reports clean on precisely the arm the second verdict
# exists for — `store.thing_scoped(&mut tx, ..)` on one line and
# `store.other(..)` three lines below it.
#
# "Names an opener" — the arm body mentions one of `begin_tenant_scoped`,
# `begin_tenant_system_scoped`, `begin_tenant_system_scoped_without_account`
# or `begin_member_scoped` (all defined in backend/src/tenancy/scope.rs).
#
# ── WHY SCOPE-BYPASSED EXISTS ───────────────────────────────────────────────
#
# Naming an opener is a WHOLE-ARM property. On its own it is satisfied by the
# exact non-fix this guard exists to catch, so on its own it is not enough:
#
#     JobScope::Org(pool) => {
#         let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
#         let a = store.thing_scoped(&mut tx, id).await?;
#         tx.commit().await?;
#         sqlx::query(OTHER).execute(*pool).await          // <- STILL ambient
#     }
#
# That is what a person produces when they fix a flagged arm by adding an
# opener and not moving the statement inside it. Adding the opener changed the
# verdict; it did not change what the last statement does. Two per-statement
# detectors ask, of an arm that already names an opener, whether a statement
# still runs outside it:
#
#   * an executor call whose executor ARGUMENT is the pool this arm binds:
#     `.execute(*pool)`, `.fetch_all(pool)`, `.acquire(&**pool)`. The argument
#     is taken by BALANCED PARENS, so `sqlx::query(f(pool)).execute(&mut *tx)`
#     is not mistaken for one, and `self.pool` / `t.pool` is not read as the
#     bare binding. WHERE the statement sits does not matter: before the
#     opener and after the commit are both outside the transaction.
#   * a non-`_scoped` store call. The store carries its own handle, so to run
#     inside this transaction the call must be handed `&mut tx`, which is what
#     the `_scoped` twin exists to take. Every one of the eight live defects
#     this baseline used to track was that shape, so it is the shape a hurried
#     fix leaves behind.
#
# THE MEASURED COST, re-measured on today's tree rather than inherited: ZERO
# false positives. Making the verdict fail, and widening the store-call scan
# from the first call to all of them, moved no arm off SCOPED and produced no
# new finding; the gate exits 0 on an unmodified tree.
#
# That claim held only after one false positive was REMOVED. An arm whose
# guard NEGATES Org — `A if !matches!(d, TenancyDecision::Org(_)) => body` —
# was read as an Org arm and its body reported UNSCOPED, though that body is
# the one thing that never runs for an org tenant. `arm_arrow` no longer
# reads a pattern that sits in a macro argument list. Cost of that
# narrowing, measured: zero arms. 168 recognised arms before and after, the
# same list, the same report.
#
# A wider variant was measured too, and rejected — but NOT because it scores
# badly today, where it also scores zero. Default-deny on the executor
# argument (flag every executor call whose argument is not visibly a
# `tx`/`conn` handle) passes every arm that is SCOPED today, because every one
# of them happens to spell its transaction `tx`. It is a NAME allowlist, and
# it fails on correct code the moment a name leaves the list. Both of these
# correct arms are flagged by it and by neither detector above:
#
#     let mut tenant_tx = begin_tenant_system_scoped_without_account(pool)..;
#     sqlx::query(Q).execute(&mut *tenant_tx).await?;   // `\btx\b` misses it
#
#     let owner = sqlx::query_scalar(SQL).fetch_one(control_pool).await?;
#     // a CONTROL-plane read beside a scoped tenant one; tenant RLS does not
#     // reach the control DB, so scoping it would be inert
#
# The detectors above key on the pool binding the ARM ITSELF declares, which
# comes from the match pattern rather than from a naming convention, so they
# have nothing to fall off.
#
# Comments and string-literal CONTENTS are blanked before any of this runs, so
# a scope primitive named only in a doc comment or inside an SQL string does
# not count as opening one, and a `//` inside a `"postgres://…"` literal does
# not blank the rest of the line. Rust lifetimes (`&'a str`) are distinguished
# from char literals; getting that wrong swallows the remainder of a file and
# turns the guard into a silent pass.
#
# ── THE ACCOUNTING, AND WHY IT IS ASSERTED RATHER THAN PRINTED ──────────────
#
# Every arm the scanner reports lands in EXACTLY ONE bucket, assigned in
# `classify_arms` and nowhere else:
#
#   SCOPED        opens a scope, no per-statement detector fired      pass
#   BASELINED     fails, and scripts/org_arm_scope.baseline says so   pass
#   REPORTED      fails, and nothing accounts for it                  FAIL
#   STALE-SCOPED  passes, and a baseline entry still claims it        FAIL
#   ORPHAN        a baseline entry matching no arm at all             FAIL
#
# `classify_arms` knows a CLOSED SET of verdicts and treats anything outside
# it as a hard error. That is not defensiveness for its own sake. The first
# draft of the SCOPE-BYPASSED verdict computed the word, printed it under
# `--list`, and then selected failures with `$4 == "UNSCOPED"`. A bypassed arm
# therefore fell out of every bucket: the gate exited 0, and the summary line
# — whose last field was the literal `0` — went on claiming `0 unaccounted
# for` while 16 + 16 did not add up to 33. An arm silently vanishing from the
# accounting while the summary asserts full coverage is worse than the defect
# the verdict was added to catch, because the summary line is what a reader
# trusts INSTEAD of counting.
#
# So: the total is counted from the scanner's output, the buckets are counted
# from the classifier's, `unaccounted = total - SCOPED - BASELINED` is
# COMPUTED rather than typed, and a non-zero value is an internal error that
# exits non-zero before any "ok:" line is printed. The two numbers come from
# two pipelines, so a verdict no bucket handles cannot move both by the same
# amount and stay hidden.
#
# ── WHAT A GREEN RUN DOES AND DOES NOT PROVE (read before trusting it) ──────
#
# A green run means exactly this: no `JobScope::Org` or `TenantId::Org` MATCH
# ARM under backend/src textually reaches the database without naming an
# opener, and no arm that names one also shows one of two specific escape
# shapes. It is NOT a statement that org code paths are scoped. Every one of
# the following is invisible to it:
#
#   * AN ORG BRANCH THAT IS NOT A MATCH ARM. This scanner reads match ARMS on
#     three enums, `TenancyDecision` now included. It does NOT read an `if`.
#     So a helper that computes "the scope for this request" and answers
#     `None` for org tenants through an EARLY RETURN is invisible, after which
#     every caller queries ambiently through ordinary un-matched code:
#         if matches!(decision, Some(TenancyDecision::Org(_))) { return None; }
#     That is not a hypothetical. It is the exact line that shipped the
#     `services/transcripts.rs::transcript_scope` defect, whose callers read
#     and write `meeting_transcripts` and `transcript_corrections` — both
#     carrying `member_ocean_isolation` (0009_tenant_rls.sql:1217-1226),
#     neither on §3f's deliberately-uncovered list.
#
#     MEASURED, so nobody has to re-derive it: adding `TenancyDecision` to the
#     arm scan does NOT catch that shape. Checking out the pre-fix file and
#     re-running this guard still exits 0. The arm scan widened coverage to
#     thirteen nested `Some(TenancyDecision::Org(..)) => ..` arms that were
#     invisible before, at a cost of one baselined false positive — that is
#     its whole value, and catching the early-return shape is not part of it.
#     Do not read "0 unaccounted for" as "org reads are scoped".
#   * AN ARM WHOSE GUARD TESTS FOR ORG THROUGH A MACRO.
#         A if matches!(decision, TenancyDecision::Org(_)) => body
#     That body does run only on an org tenant, and this scanner does not
#     read it. The arm pattern is `A`; the `Org(..)` sits in a MACRO
#     ARGUMENT LIST, and `arm_arrow` refuses that whole position rather than
#     guess the polarity of the guard around it. The negated twin —
#     `A if !matches!(..Org..) => body`, whose body runs on everything
#     EXCEPT an org tenant — is textually the same shape, and reading it as
#     an Org arm was a live defect: see the note on `arm_arrow`. Zero sites
#     of either form in the tree today. Refusing both loses nothing that is
#     there and cannot misattribute a body that is not.
#   * THE ESCAPING POOL. A binding lifted out of the match and used after it:
#         let pool: &PgPool = match scope {
#             JobScope::Consumer { pool, .. } => pool,
#             JobScope::Org(pool) => pool,
#         };
#         ...
#         sqlx::query(Q).execute(pool).await         // outside every arm
#     The arms themselves reach nothing, so they are correctly not findings,
#     and whatever happens to the pool afterwards is outside the region this
#     guard analyses. `workers/conflict_detection.rs:639` and
#     `workers/infer_expertise.rs:699` are both this shape today; nothing here
#     has an opinion on either.
#   * IT IS INTRAPROCEDURAL TEXT ANALYSIS, not a data-flow proof. An arm that
#     hands its pool to a helper which scopes internally is textually
#     identical to one that hands its pool to a helper which does not.
#     `workers/embed_retry.rs` is the worked example: the defect was
#     `find_retry_eligible_atoms(pool, LIMIT, None)` and the fix is
#     `claim_org_scoped(pool, LIMIT)` — the same shape. This guard therefore
#     does NOT flag either, and misses that one instance of the 88. A variant
#     that flags any pool-valued call argument was measured: it catches that
#     88th instance and adds eleven false positives on the fixed tree,
#     including the FIXED embed_retry arm. Not worth it. Reviewers still own
#     "does the callee scope".
#   * A SCOPE OPENED OUTSIDE THE MATCH and used inside the arm reads as
#     unscoped here. No such site exists today; if one lands it is a baseline
#     entry, not a matcher change.
#   * "OPENS A SCOPE" IS NOT "OPENS THE RIGHT SCOPE".
#     `begin_tenant_system_scoped*` is a reviewed bypass of the member-ocean
#     backstop; whether a given arm deserves the system principal or should
#     thread a real user is a reviewer's call, recorded in
#     docs/tenant-rls-cutover-runbook.md §2, and no grep can make it.
#   * SCOPE-BYPASSED IS TWO SHAPES, NOT A THEOREM. An arm can name an opener,
#     dodge both detectors, and still run a statement outside the transaction
#     — by cloning the pool into a local first, or by calling a free function
#     that takes `impl Executor`. SCOPED means "neither detector fired", not
#     "every statement in this arm is inside the transaction".
#   * THE BASELINE IS KEYED BY ARM, NOT BY VERDICT. An entry written for an
#     UNSCOPED arm keeps that arm green after it turns SCOPE-BYPASSED, and a
#     reason that argued "this table carries no member-ocean policy" may no
#     longer describe what the arm touches. The key surviving a rewrite is a
#     feature for line drift and a hazard for behaviour drift: re-read the
#     reason whenever the arm changes.
#   * A BASELINED ARM IS NOT A CORRECT ARM. It shipped with sixteen entries,
#     eight of them live instances of this same defect, recorded so they were
#     tracked rather than silent; the ninth slice converted those eight and
#     deleted their entries, so the eight that remain are all genuinely fine.
#     Read the reasons rather than
#     trusting a count — the file is allowed to grow by hand, and the reason
#     line is where a new entry says which kind it is.
#
# ── THE BASELINE ────────────────────────────────────────────────────────────
#
# scripts/org_arm_scope.baseline lists the arms that are known and allowed to
# stay as they are, one per line, each with a `# reason:` line directly above
# it. Same discipline as scripts/scoped_shape.baseline: the file may shrink
# and may not grow without a hand edit and a reviewer. A stale entry — one
# whose arm no longer fires — is a FAILURE, not a no-op, because a baseline
# that is too big is headroom the next defect lands in without anyone
# deciding anything.
#
# The `# reason:` line must carry at least one NON-SPACE character after the
# colon. A bare `# reason:` satisfies a prefix test and says nothing, which is
# the "a rationale nobody wrote is a rationale nobody can check" failure this
# check exists to prevent, wearing the check's own uniform. Duplicate keys are
# an error too: two entries for one arm are two reasons, one of which nobody
# will ever read.
#
# Entries are keyed `<path>::<enclosing fn>#<n>`, not by line number, so
# unrelated edits above an arm do not invalidate the file.
#
# NEVER silence a finding by loosening a matcher. Add a baseline entry with a
# reason, or fix the arm.
#
# ── USAGE ───────────────────────────────────────────────────────────────────
#   bash scripts/check_org_arm_scope.sh              # the gate
#   bash scripts/check_org_arm_scope.sh --list       # every Org arm + verdict
#   bash scripts/check_org_arm_scope.sh --self-test  # the guard's own fixtures
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE=""
MODE="check"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --list)      MODE="list"; shift ;;
    --self-test) MODE="self-test"; shift ;;
    # --root/--baseline exist for the self-test's throwaway fixture trees.
    # They are not part of the normal contract.
    --root)      ROOT="$2"; shift 2 ;;
    --baseline)  BASELINE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Derived AFTER parsing so `--root` alone can never point the gate at a
# fixture tree while reading the real repo's baseline.
[[ -n "$BASELINE" ]] || BASELINE="$ROOT/scripts/org_arm_scope.baseline"

# ── The scanner ─────────────────────────────────────────────────────────────
# ONE definition, used by the gate, by --list and by the self-test. Two copies
# would drift, and a drifted scanner reports OK for the wrong reason.
#
# Emits one TAB-separated record per Org arm that reaches the DB:
#   <key> <path>:<line> <enum>::Org(<binding>) <VERDICT> <signals> <escapes>
# where <VERDICT> is one of SCOPED / UNSCOPED / SCOPE-BYPASSED and <escapes>
# is a comma-separated `<line>|<detector>` list, empty unless SCOPE-BYPASSED.
scan_tree() {
  local root="$1"
  local trait_file="$root/backend/src/store/mod.rs"
  [[ -d "$root/backend/src" ]] || { echo "ORG-ARM-ERROR: $root/backend/src does not exist" >&2; return 1; }
  [[ -f "$trait_file" ]] || { echo "ORG-ARM-ERROR: $trait_file not found — the Store trait moved. Fix this scanner before trusting a green run." >&2; return 1; }

  # The `_scoped` twin roster, read from the `Store` trait body. Same parse as
  # scripts/check_scoped_shape_ratchet.sh, and it fails CLOSED for the same
  # reason: a rename that makes this report an EMPTY roster would quietly
  # disable one of the six detectors while still printing OK.
  local twins
  twins="$(perl -e '
    my $f = shift;
    open(my $fh, "<", $f) or die "ORG-ARM-ERROR: cannot open $f\n";
    my ($in, $seen, @n) = (0, 0);
    while (my $l = <$fh>) {
      if (!$in && !$seen && $l =~ /^pub trait Store\s*:/) { $in = 1; $seen = 1; next }
      next unless $in;
      if ($l =~ /^\}/) { $in = 0; next }
      next if $l =~ m{^\s*//};
      push @n, $1 if $l =~ /^\s*(?:pub\s+)?(?:async\s+)?fn\s+([A-Za-z0-9_]+)_scoped\s*[<(]/;
    }
    die "ORG-ARM-ERROR: `pub trait Store:` not found in $f — the trait was renamed, moved, or reformatted. Fix this scanner before trusting a green run.\n" unless $seen;
    die "ORG-ARM-ERROR: the Store trait in $f declares no `_scoped` methods. That cannot be right; one of this guard'"'"'s detectors would be silently disabled.\n" unless @n;
    print join(",", @n);
  ' "$trait_file")" || return 1

  # SC2016: the perl program below is deliberately single-quoted. `$body`,
  # `$1`, `$path` and friends are PERL variables; letting the shell expand them
  # would blank the scanner and make it print OK on every tree. The two values
  # it needs from the shell arrive as environment variables instead.
  # shellcheck disable=SC2016
  find "$root/backend/src" -type f -name '*.rs' -print0 \
    | LC_ALL=C sort -z \
    | ORG_ARM_TWINS="$twins" ORG_ARM_ROOT="$root" xargs -0 perl -e '
use strict;
use warnings;

my %twin = map { $_ => 1 } grep { length } split /,/, ($ENV{ORG_ARM_TWINS} // "");
my $root = $ENV{ORG_ARM_ROOT};

# The ONLY length bound left anywhere in arm recognition. Per the contract on
# `arm_arrow` below, exceeding it is a HARD ERROR naming the file and the
# line, never a skip. Nothing in this repo comes within two orders of
# magnitude of it; it exists so that pathological input stops the gate instead
# of quietly shrinking the tally.
my $GUARD_SCAN_CAP = 4096;

# The scanned enums, in ONE place. The cheap pre-filter below and the match
# loop that does the real recognition must never disagree: a name known to the
# loop and missing from the filter skips every file that mentions only that
# name WHOLE. Those files never reach the scanner, so their arms are absent
# from the total as well as from the report — the buckets still sum,
# `unaccounted` is still 0, and the gate still prints ok. That is the same
# silent-pass shape the pre-filter comment below was written to close, so the
# two regexes are built from this list rather than typed twice.
my $ENUM_ALT = join "|", qw(JobScope TenantId TenancyDecision);

# Blank out comment bodies and string- and char-literal CONTENTS, preserving
# length and newlines so offsets and line numbers stay exact.
#
# The three traps here, each of which turns this guard into a silent pass:
#   * `"postgres://host"` — a naive `s{//.*}{}` truncates the line at the URL
#     and hides every token after it.
#   * `&\x27a str` — a lifetime, not a char literal. Treating the apostrophe as
#     an opening quote consumes the rest of the file.
#   * `sep == \x27)\x27` — a char literal whose CONTENT is a delimiter. Left
#     unblanked it is a real delimiter to every scan downstream; see the note
#     at the char-literal branch below.
sub mask {
    my ($s) = @_;
    my $n   = length $s;
    my $out = $s;
    my $i   = 0;
    my $blank = sub {
        my ($from, $to) = @_;
        return if $to <= $from;
        my $seg = substr($s, $from, $to - $from);
        $seg =~ s/[^\n]/ /g;
        substr($out, $from, $to - $from) = $seg;
    };
    while ($i < $n) {
        my $c  = substr($s, $i, 1);
        my $c2 = substr($s, $i, 2);
        if ($c2 eq "//") {
            my $j = index($s, "\n", $i); $j = $n if $j < 0;
            $blank->($i, $j); $i = $j; next;
        }
        if ($c2 eq "/*") {
            my $depth = 1; my $j = $i + 2;
            while ($j < $n && $depth > 0) {
                my $d2 = substr($s, $j, 2);
                if    ($d2 eq "/*") { $depth++; $j += 2 }
                elsif ($d2 eq "*/") { $depth--; $j += 2 }
                else                { $j++ }
            }
            $blank->($i, $j); $i = $j; next;
        }
        if ($c =~ /[rb]/ && ($i == 0 || substr($s, $i - 1, 1) !~ /\w/)) {
            if (substr($s, $i) =~ /^b?r(\#*)"/) {
                my $hashes = $1;
                my $open   = $i + length($&);
                my $term   = q{"} . $hashes;
                my $j      = index($s, $term, $open);
                $j = ($j < 0) ? $n : $j + length($term);
                $blank->($open, $j - length($term));
                $i = $j; next;
            }
        }
        if ($c eq q{"}) {
            my $j = $i + 1;
            while ($j < $n) {
                my $d = substr($s, $j, 1);
                last if $d eq q{"};
                $j += ($d eq "\\") ? 2 : 1;
            }
            $blank->($i + 1, $j);
            $i = $j + 1; next;
        }
        if ($c eq "\x27") {
            if (substr($s, $i) =~ /^\x27(?:\\.|[^\\\x27])\x27/) {
                # BLANK THE CONTENTS, like every other literal here. Leaving
                # them was the third masking trap and the only one that did
                # not announce itself: a char literal holding a DELIMITER is
                # a delimiter to every scan downstream. `scan_to_arrow` reads
                # the `)` in a guard like `sep == \x27)\x27` as the closer of a
                # group it never opened and abandons the arm; a `,` or `;` in a
                # char literal ends the guard early the same way; a `}` in an
                # arm BODY closes the body before the statement that reaches
                # the DB. Measured on the pre-fix script, each of those three
                # printed "ok: 0 Org arms reach the DB" over an unscoped
                # `sqlx::query(..).execute(*pool)`. The backward walk in
                # `group_is_macro_arg` counts them too, where an extra closer
                # can carry the walk out to an enclosing `ident!(` and make a
                # real nested arm look like a macro argument.
                my $len = length($&);
                $blank->($i + 1, $i + $len - 1);
                $i += $len; next;
            }
            $i++; next;   # a lifetime
        }
        $i++;
    }
    return $out;
}

sub match_delim {
    my ($s, $at) = @_;
    my %close = ("(" => ")", "[" => "]", "{" => "}");
    return undef unless $close{ substr($s, $at, 1) };
    my $depth = 0;
    my $n     = length $s;
    for (my $i = $at; $i < $n; $i++) {
        my $c = substr($s, $i, 1);
        if    ($c =~ /[\(\[\{]/) { $depth++ }
        elsif ($c =~ /[\)\]\}]/) { $depth--; return $i if $depth == 0 }
    }
    return undef;
}

# The arm body, by BALANCED DELIMITERS. A block arm (`=> { .. }` / the block
# of `=> async { .. }`) ends at its matching brace; an expression arm ends at
# the first depth-0 comma or at the closing brace of the match itself.
#
# Returns ($text, $offset_of_text_in_$s). The offset is what turns a hit
# inside the body into a LINE NUMBER a reader can open, which is the whole
# difference between "this arm bypasses its scope" and "this arm bypasses its
# scope, at line 723, here is the statement".
sub arm_body {
    my ($s, $start) = @_;
    my $n = length $s;
    my $i = $start;
    $i++ while $i < $n && substr($s, $i, 1) =~ /\s/;
    return ("", $start) if $i >= $n;
    if (substr($s, $i, 1) eq "{") {
        my $e = match_delim($s, $i);
        return defined $e ? (substr($s, $i, $e - $i + 1), $i) : (substr($s, $i), $i);
    }
    my $begin = $i;
    my $depth = 0;
    while ($i < $n) {
        my $c = substr($s, $i, 1);
        if    ($c =~ /[\(\[\{]/) { $depth++ }
        elsif ($c =~ /[\)\]\}]/) { last if $depth == 0; $depth-- }
        elsif ($c eq "," && $depth == 0) { last }
        $i++;
    }
    return (substr($s, $begin, $i - $begin), $begin);
}

# Walk a match GUARD or an OR-PATTERN to the `=>` that ends it. Returns the
# offset just past the arrow, or undef when the tokens say this is not an arm.
#
# There is no length test in here. The scan ends on a token that CANNOT sit
# between a pattern and its arrow in valid Rust:
#
#   * a depth-0 `;` or `,`   — a statement or an argument list ended first.
#   * a closer this scan did not open — the group the pattern sits in ended
#     first. This is what makes
#     `matches!(plane.tenant(), TenantId::Org(o) if o == "org_a")`
#     (backend/src/crypto.rs:1095) correctly NOT an arm.
#
# A block inside a guard is entered at depth 1, so its `;` and `}` are not
# depth-0 and do not end the scan. Hitting a terminator is therefore a
# DECISION about syntax, not a bound being reached — the distinction this
# whole function exists to keep.
sub scan_to_arrow {
    my ($s, $start, $rel, $lineat) = @_;
    my $n     = length $s;
    my $i     = $start;
    my $depth = 0;
    while ($i < $n) {
        if ($i - $start > $GUARD_SCAN_CAP) {
            die sprintf(
                  "ORG-ARM-ERROR: %s:%d: the match guard or or-pattern after this `Org(..)`\n"
                . "               pattern ran past %d characters without reaching its `=>`.\n"
                . "               This scanner will not GUESS whether that is an arm. Guessing it\n"
                . "               away is how the previous 200-character bound worked: the arm fell\n"
                . "               out of the scan, out of the tally, and the gate went on printing\n"
                . "               \"0 unaccounted for\" over an unscoped DELETE. Split the guard, or\n"
                . "               raise ORG_ARM guard cap in this scanner and say why.\n",
                $rel, $lineat->($start), $GUARD_SCAN_CAP);
        }
        my $c = substr($s, $i, 1);
        if ($c =~ /[\(\[\{]/) { $depth++; $i++; next }
        if ($c =~ /[\)\]\}]/) { return undef if $depth == 0; $depth--; $i++; next }
        if ($depth == 0) {
            return $i + 2 if substr($s, $i, 2) eq "=>";
            return undef  if $c eq ";" || $c eq ",";
        }
        $i++;
    }
    die sprintf(
          "ORG-ARM-ERROR: %s:%d: a match guard or or-pattern after an `Org(..)` pattern ran\n"
        . "               to the end of the file without reaching a `=>`, a `;` or a closing\n"
        . "               delimiter. This file does not balance the way the scanner assumes,\n"
        . "               so every arm below this point may have been misread. Fix it before\n"
        . "               trusting a green run.\n",
        $rel, $lineat->($start));
}

# NO ASCII APOSTROPHES BELOW THIS LINE, and none anywhere else inside this
# perl program: the whole program is one single-quoted shell word, so one
# apostrophe in a comment ends it and the file stops parsing as bash.
#
# Is the group that ends at this closer the argument list of a MACRO?
#
# Walks BACKWARD from the closer to its matching opener, balanced, then reads
# the two tokens in front of that opener. `matches!(` and `assert_ne!(` are
# macro invocations; `Some(`, `Ok(` and `from_decision(` are not. Verified on
# the tree: it fires on the eight real macro-argument sites and on none of
# the ordinary calls beside them, including
# `TenantId::from_decision(&TenancyDecision::Org(ctx))` one line away from a
# site where it does fire.
#
# The walk is bounded by the size of the group, which is why it can afford to
# be exact. An unbalanced file walks to offset 0 and answers "not a macro",
# which leaves the decision to the other tests in `arm_arrow` — the same
# reading it had before this function existed.
sub group_is_macro_arg {
    my ($s, $close) = @_;
    my $depth = 0;
    my $i     = $close;
    while ($i >= 0) {
        my $c = substr($s, $i, 1);
        if    ($c =~ /[\)\]\}]/) { $depth++ }
        elsif ($c =~ /[\(\[\{]/) { $depth--; last if $depth == 0 }
        $i--;
    }
    return 0 if $i < 0;
    my $j = $i - 1;
    $j-- while $j >= 0 && substr($s, $j, 1) =~ /\s/;
    return 0 unless $j >= 1 && substr($s, $j, 1) eq "!";
    return substr($s, $j - 1, 1) =~ /\w/ ? 1 : 0;
}

# Does a match ARM start at this `Org(..)` pattern, and where does its body
# begin? Returns the offset just past the `=>`, or undef when the site is a
# constructor call, a `matches!` pattern, an `if let`, or anything else that
# is not an arm.
#
# THIS FUNCTION MAY NOT SKIP AN ARM. Its predecessor did, and the skip was
# invisible. It tested a 300-character window with
# `^(\s*(?:if\b[^;{]{0,200}?)?=>)`, so an arm whose MATCH GUARD ran past about
# 200 characters failed the pattern, fell out through `next unless`, and was
# never scanned, never counted and never reported. Measured: a 290-character
# guard in front of `sqlx::query("DELETE FROM atoms").execute(*pool)` left the
# gate at exit 0 saying "25 Org arms ... 0 unaccounted for"; the same statement
# behind `if x > 0` failed the gate. Guard LENGTH decided whether the guard saw
# the arm — absence rendering as success, inside the control that exists to
# catch exactly that shape.
#
# The decision is syntactic now, never a length test:
#
#   `=>`     the arm.
#   `if ..`  a match guard, of ANY length, walked by `scan_to_arrow`.
#   `| ..`   an or-pattern (`JobScope::Org(p) | JobScope::Team(p) => ..`),
#            walked the same way. The arrow decides, not the alternatives.
#   else     not an arm. `=` is `if let JobScope::Org(p) = s`; `;` `,` `.` `)`
#            are constructor calls and `matches!` arguments.
#
# NESTED PATTERNS ARE ARMS. `Some(TenancyDecision::Org(ctx)) => ..` ends at
# the INNER paren, so its arrow sits behind closers this scan never opened.
# Skipping those closers is required, not optional: `decision` is an
# `Option<&TenancyDecision>` almost everywhere, and thirteen of the sixteen
# `TenancyDecision::Org` arms in the tree are this shape. Reading them as
# non-arms is the same silent vanish as the guard bound, one enum over.
#
# The arrow decides, not the closers, and no expression context puts a `=>`
# immediately after the closing paren of a call. Measured on the tree today:
# 34 `Org(..)` patterns sit behind a closer, 13 are followed by `=>` and all
# 13 are genuine nested arms; the other 21 stop on a `;`, a `,` or another
# closer and are correctly not arms.
#
# WHAT THIS PARAGRAPH USED TO CLAIM, AND WHY IT WAS TOO STRONG. It said
# skipping closers "can only ever FIND an arm". It cannot invent an arm, but
# it can attribute the WRONG BODY to Org:
#
#     A if !matches!(decision, TenancyDecision::Org(_)) => unscoped_call()
#
# The `Org(..)` there does not belong to the arm. It is inside the GUARD of
# an arm whose own pattern is `A`, and the guard EXCLUDES Org — so that body
# is the one thing that never runs for an org tenant. Reproduced: the scanner
# skipped the `matches!` closer, took the `=>` behind it, and reported
# `w.rs:3 TenancyDecision::Org(_) UNSCOPED`. That is worse than an ordinary
# false positive. The entry someone adds to silence it is keyed by ARM, so it
# stays valid after the `!` is deleted, and then suppresses a real unscoped
# Org path.
#
# THE DECISION. Polarity is not decidable from text — `!matches!(..)`,
# `!(matches!(..))`, `matches!(..) == false` and `!a && matches!(..)` are one
# shape to a grep — so this does not try to read it. It refuses the whole
# ambiguous class instead: a MACRO ARGUMENT LIST is not a pattern position,
# so an `Org(..)` inside one never belongs to the arm, whatever the guard
# does with it. `group_is_macro_arg` below stops the closer skip there.
#
# THE COST, MEASURED on the tree today rather than argued: ZERO arms. Eight
# `Org(..)` patterns sit inside a macro argument list (`matches!`,
# `assert_ne!`), and every one of them already stopped on a `;`, a `,` or
# another closer, so none was an arm before. Recognised-arm count 168 before
# and 168 after, same list; the report from the gate is unchanged. What is
# given up is a POSITIVE macro guard — `A if matches!(y, TenantId::Org(_))
# => ..`, whose body does run only on an org tenant — and there is no such
# site in the tree. That is a disclosed blind spot now, listed with the
# others in the header, not a silent one.
sub arm_arrow {
    my ($s, $from, $rel) = @_;
    my $n = length $s;
    my $lineat = sub { 1 + (substr($s, 0, $_[0]) =~ tr/\n//) };

    my $i = $from;
    $i++ while $i < $n && substr($s, $i, 1) =~ /\s/;

    # Closers this scan did not open: the pattern is inside another one.
    while ($i < $n && substr($s, $i, 1) =~ /[\)\]\}]/) {
        # ...unless the group being closed is the argument list of a MACRO.
        # Then the pattern is an argument, not the pattern of this arm, and
        # whatever `=>` sits behind that closer belongs to an arm with a
        # different pattern. See the note above.
        return undef if group_is_macro_arg($s, $i);
        $i++;
        $i++ while $i < $n && substr($s, $i, 1) =~ /\s/;
    }
    return undef if $i >= $n;

    return $i + 2 if substr($s, $i, 2) eq "=>";
    return scan_to_arrow($s, $i, $rel, $lineat) if substr($s, $i, 3) =~ /^(?:if\b|\|)/;
    return undef;
}

for my $path (@ARGV) {
    open(my $fh, "<", $path) or next;
    local $/;
    my $raw = <$fh>;
    close $fh;
    next unless defined $raw;
    # Cheap pre-filter: masking is O(n) per character and store/mod.rs alone
    # is ~32k lines, so files that cannot contain an Org arm are skipped
    # whole. It filters on the ENUM NAME and nothing else, deliberately.
    #
    # It used to filter on the whole shape, `Enum\s*::\s*Org\s*\(`, and it
    # runs on the RAW text — BEFORE `mask()`. `\s*` does not match a comment,
    # so this legal Rust
    #
    #     TenancyDecision /* which enum */ :: Org /* the ctx */ (ctx) => ..
    #
    # failed the filter and the file was skipped whole. The arm was then
    # absent from the scan AND from the accounting total, so the buckets
    # still summed, `unaccounted` was still 0, and the gate still printed
    # "ok: .. 0 unaccounted for" over an unscoped store call. Reproduced.
    # Same silent-vanish class as the 200-character guard bound: a syntax
    # test that decides what gets LOOKED AT, applied to text the scanner has
    # not normalised yet.
    #
    # So recognition belongs to the masked scanner below, which sees those
    # comments as spaces and matches. This line may only ever answer "this
    # file cannot possibly contain one", and a bare enum name is the widest
    # cheap way to ask that. It cannot be broken by a comment, because a
    # comment cannot be inserted INSIDE an identifier.
    #
    # Measured: 64 of 246 files were masked before, 91 after; wall time 1.43s
    # before, 2.2s after, on the whole gate. That is the price of the
    # correction and it is worth paying at this size. Narrowing it back with
    # a second conjunct buys almost nothing: adding `&& /\bOrg\b/` reaches 83
    # files rather than 91, and store/mod.rs — 32k lines, the file this
    # pre-filter was written for — is masked either way, because it names
    # `TenantId` seven times and `Org` three.
    #
    # Do NOT narrow it with a PROXIMITY test instead. A comment between the
    # tokens can be any length, so proximity is a character bound, which is
    # the exact mechanism that hid the long-guard arm.
    #
    # And do not re-type the roster here. `$ENUM_ALT` is the same string the
    # recognition loop below uses, so the filter cannot fall behind the loop
    # and start skipping files whole. See its declaration for why that
    # divergence is unobservable from the gate output.
    next unless $raw =~ /\b(?:$ENUM_ALT)\b/;
    my $m = mask($raw);

    my $rel = $path;
    $rel =~ s/^\Q$root\E\///;

    # Enclosing-fn index, for a baseline key that survives line drift.
    my @fns;   # [offset, name]
    while ($m =~ /^[ \t]*(?:pub(?:\([^)]*\))?\s+)?(?:default\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?(?:extern\s+"[^"]*"\s+)?fn\s+([A-Za-z_]\w*)/mg) {
        push @fns, [ $-[0], $1 ];
    }
    my %ordinal;

    while ($m =~ /\b($ENUM_ALT)\s*::\s*Org\s*\(/g) {
        my $enum  = $1;
        my $open  = pos($m) - 1;
        my $close = match_delim($m, $open);
        next unless defined $close;
        my $bind = substr($m, $open + 1, $close - $open - 1);
        $bind =~ s/^\s+|\s+$//g;

        # A match arm, not a constructor call. `arm_arrow` decides that by
        # TOKEN and never by length: a guard of any size is walked to its
        # arrow, and the one bound left in it is a hard error rather than a
        # skip. The window-plus-bounded-guard regex this replaced dropped any
        # arm whose guard ran past ~200 characters, silently.
        my $arrow = arm_arrow($m, $close + 1, $rel);
        next unless defined $arrow;
        my ($body, $body_at) = arm_body($m, $arrow);
        my $line = 1 + (substr($m, 0, $open) =~ tr/\n//);

        # Offset inside $body -> absolute source line.
        my $lineof = sub {
            my ($o) = @_;
            return 1 + (substr($m, 0, $body_at + $o) =~ tr/\n//);
        };

        my $fn = "(top-level)";
        for my $f (@fns) { last if $f->[0] > $open; $fn = $f->[1] }
        my $key = sprintf("%s::%s#%d", $rel, $fn, ++$ordinal{$fn});

        my @why;
        push @why, "sqlx::query"   if $body =~ /\bsqlx\s*::\s*query/;
        push @why, "executor-call" if $body =~ /\.\s*(?:fetch_one|fetch_all|fetch_optional|fetch_many|fetch|execute)\s*\(/;
        push @why, "acquire"       if $body =~ /\.\s*acquire\s*\(/;

        # EVERY non-`_scoped` store call, with its offset — not just the first
        # match. An arm whose FIRST store call is the `_scoped` twin and whose
        # second is not is the exact shape a hurried fix leaves behind, and
        # reading only the first match reports it clean.
        #
        # Third field is the offset of the OPENING PAREN of the call. That is the
        # identity of the STATEMENT, and it is what the escape list dedupes
        # on, so two different statements are two report lines however alike
        # their signals read. See the escape list below.
        my @storecalls;   # [offset, signal, call-site]
        while ($body =~ /\b(?:[a-z_]\w*\s*\.\s*)?store\s*\.\s*([A-Za-z_]\w*)\s*\(/g) {
            my ($nm, $at, $call) = ($1, $-[0], $+[0] - 1);
            next if $nm =~ /_scoped$/;
            push @storecalls, [ $at, "store-call($nm)", $call ];
        }
        push @why, map { $_->[1] } @storecalls;

        if ($bind =~ /^([A-Za-z_]\w*)$/) {
            my $id = $1;
            push @why, "pool-as-executor($id)" if $body =~ /[*&]\s*\Q$id\E\b/;
        }

        # No per-NAME dedupe here either: two calls to the same twin are two
        # statements, and collapsing them reports the first and ships the
        # second.
        my @twincalls;    # [offset, signal, call-site]
        while ($body =~ /\.\s*([A-Za-z_]\w*)\s*\(/g) {
            my ($c, $at, $call) = ($1, $-[0], $+[0] - 1);
            next if $c =~ /_scoped$/;
            next unless $twin{$c};
            push @twincalls, [ $at, "unscoped-twin($c)", $call ];
        }
        push @why, map { $_->[1] } @twincalls;
        next unless @why;

        my $opener = ($body =~ /\bbegin_tenant_scoped\b/
                   || $body =~ /\bbegin_tenant_system_scoped\w*\b/
                   || $body =~ /\bbegin_member_scoped\b/) ? 1 : 0;

        # ── The per-statement check (verdict: "SCOPE-BYPASSED") ──────────────
        # An opener SOMEWHERE in the arm does not put a given statement INSIDE
        # it. These two detectors ask, of an arm that already has an opener,
        # whether a statement still runs outside it.
        my @bypass;   # [offset, detail, call-site]

        # (i) An executor call whose executor ARGUMENT is the pool binding of
        #     this arm. `.execute(*pool)` runs on the pool, not on the `tx`
        #     opened one line above, and it does so whether it sits before the
        #     opener or after the commit. Balanced-paren argument extraction,
        #     so a nested call in the SQL-building position is not mistaken
        #     for the executor.
        #     EVERY such call, not one per method name. Two
        #     `.execute(*pool)` statements in one arm are two escapes; a
        #     per-method dedupe reported the first, and the reader who fixed
        #     the named line re-ran the gate, saw green, and shipped the
        #     second.
        if ($bind =~ /^([A-Za-z_]\w*)$/) {
            my $id = $1;
            while ($body =~ /\.\s*(fetch_one|fetch_all|fetch_optional|fetch_many|fetch|execute|acquire)\s*\(/g) {
                my $meth = $1;
                my $op   = pos($body) - 1;
                my $cl   = match_delim($body, $op);
                # `next`, not `last`: one call whose parens do not balance
                # must not end the scan and hide every statement after it.
                next unless defined $cl;
                my $arg  = substr($body, $op + 1, $cl - $op - 1);
                # `(?<![\w.])` so `self.pool` / `t.pool` is not read as the
                # bare binding; those are a different handle entirely.
                next unless $arg =~ /(?<![\w.])\Q$id\E\b/;
                push @bypass, [ $op, "bare-pool-executor($meth <- $id)", $op ];
            }
        }

        # (ii) A non-`_scoped` store call. The store carries its own handle,
        #      so the call cannot be running inside the transaction this arm
        #      opened: to run inside it the arm must pass `&mut tx`, which is
        #      what the `_scoped` twin exists to take.
        push @bypass, @storecalls, @twincalls;

        my $verdict = !$opener   ? "UNSCOPED"
                    : @bypass    ? "SCOPE-BYPASSED"
                    :              "SCOPED";

        # EVERY escaping statement, keyed by its CALL SITE. The dedupe used to
        # key on the signal string, so an arm with two `.execute(*pool)`
        # statements reported one line: the reader fixed the line the report
        # named, re-ran the gate, saw green, and shipped the other one. Keying
        # on the call site collapses only the two detectors that can describe
        # ONE call — a `store.foo(..)` is both `store-call(foo)` and
        # `unscoped-twin(foo)` — and never two different statements.
        my $escapes = "";
        if ($verdict eq "SCOPE-BYPASSED") {
            my %once2;
            my @d = grep { !$once2{ $_->[2] }++ }
                    sort { $a->[0] <=> $b->[0] } @bypass;
            $escapes = join(",", map { $lineof->($_->[0]) . "|" . $_->[1] } @d);
            push @why, map { $_->[1] } @d;
        }

        my %once; my @out = grep { !$once{$_}++ } @why;
        printf "%s\t%s:%d\t%s::Org(%s)\t%s\t%s\t%s\n",
            $key, $rel, $line, $enum, $bind, $verdict, join(",", @out), $escapes;
    }
}
'
}

# ── Baseline I/O ────────────────────────────────────────────────────────────
# An entry is a bare key on its own line. The line DIRECTLY ABOVE it must be a
# `# reason:` comment WITH TEXT. A rationale nobody wrote is a rationale nobody
# can check, which is how scripts/tenant_rls_coverage.manifest ended up
# justifying itself with a mechanism PR #388 had already deleted.
read_baseline_keys() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  awk '!/^[[:space:]]*#/ && NF { print $1 }' "$file"
}

check_baseline_reasons() {
  local file="$1" bad=0 prev="" lineno=0 text=""
  [[ -f "$file" ]] || return 0
  # `|| [[ -n "$raw" ]]` so a file with no trailing newline does not silently
  # drop its LAST entry — which is exactly the entry a hurried hand edit adds.
  while IFS= read -r raw || [[ -n "$raw" ]]; do
    lineno=$((lineno + 1))
    if [[ -z "${raw// }" || "$raw" == \#* ]]; then
      prev="$raw"
      continue
    fi
    case "$prev" in
      "# reason:"*)
        # A PREFIX match is not a reason. `# reason:` on its own, or followed
        # by nothing but spaces, passes a prefix test and tells a reader
        # nothing — the exact failure this check exists to prevent, wearing
        # this check's own uniform. Demand one non-space character.
        text="${prev#\# reason:}"
        if [[ -z "${text//[[:space:]]/}" ]]; then
          echo "FAIL: $file:$((lineno - 1)) — the '# reason:' line above '${raw%% *}' is empty." >&2
          echo "      A reason with no text is not a reason. Say which tables the arm" >&2
          echo "      touches and why an unscoped statement against them is safe." >&2
          bad=1
        fi
        ;;
      *)
        echo "FAIL: $file:$lineno — baseline entry '${raw%% *}' has no '# reason:' line directly above it." >&2
        bad=1
        ;;
    esac
    prev="$raw"
  done < "$file"
  return "$bad"
}

check_baseline_duplicates() {
  local file="$1" dups
  [[ -f "$file" ]] || return 0
  dups="$(read_baseline_keys "$file" | LC_ALL=C sort | LC_ALL=C uniq -d)"
  [[ -n "$dups" ]] || return 0
  echo "FAIL: duplicate baseline entr(ies) in $file:" >&2
  printf '%s\n' "$dups" | sed 's/^/  - /' >&2
  echo "      Two entries for one arm are two reasons, one of which nobody reads," >&2
  echo "      and they double-count that arm in the accounting below." >&2
  return 1
}

# ── The accounting ──────────────────────────────────────────────────────────
# classify_arms <baseline-keys> <findings>
#
# Puts every scanner record in EXACTLY ONE bucket and emits
# `<BUCKET>\t<record>`, plus one `ORPHAN` row per baseline key that matched no
# record. A verdict outside the closed set is a HARD ERROR (exit 3), not a
# row quietly dropped: that drop is the bug this function was written after.
classify_arms() {
  local base="$1" finds="$2"
  # Tagged with awk, not sed: BSD sed does not read `\t` in a replacement as a
  # tab, and a literal tab in a `s///` is invisible to the next person to edit
  # this file. Both streams keep their own field structure.
  {
    printf '%s\n' "$base"  | LC_ALL=C awk 'NF { print "B\t" $0 }'
    printf '%s\n' "$finds" | LC_ALL=C awk -F'\t' 'NF { print "F\t" $0 }'
  } | LC_ALL=C awk -F'\t' '
      $1 == "B" { base[$2] = 1; next }
      $1 != "F" {
        printf "ORG-ARM-ERROR: internal: untagged accounting record: %s\n", $0 > "/dev/stderr"
        bad = 1; exit
      }
      {
        key = $2; verdict = $5
        if (verdict == "SCOPED") {
          bucket = (key in base) ? "STALE-SCOPED" : "SCOPED"
        } else if (verdict == "UNSCOPED" || verdict == "SCOPE-BYPASSED") {
          bucket = (key in base) ? "BASELINED" : "REPORTED"
        } else {
          printf "ORG-ARM-ERROR: the scanner emitted verdict \"%s\" for %s,\n", verdict, key > "/dev/stderr"
          printf "               and this gate has no bucket for it. A verdict nothing buckets\n" > "/dev/stderr"
          printf "               is an arm nothing checks, and the summary line goes on saying\n" > "/dev/stderr"
          printf "               \"0 unaccounted for\" anyway. Wire the new verdict into\n" > "/dev/stderr"
          printf "               classify_arms; do not widen this else.\n" > "/dev/stderr"
          bad = 1; exit
        }
        seen[key] = 1
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", bucket, $2, $3, $4, $5, $6, $7
      }
      END {
        if (bad) exit 3
        for (k in base) if (!(k in seen)) printf "ORPHAN\t%s\t-\t-\t-\t-\t-\n", k
      }
  '
}

bucket_rows() {
  printf '%s\n' "$2" | LC_ALL=C awk -F'\t' -v b="$1" '$1 == b'
}

bucket_count() {
  bucket_rows "$1" "$2" | sed '/^$/d' | wc -l | tr -d ' '
}

# ── The two accounting invariants, as FUNCTIONS ─────────────────────────────
# They are functions so the self-test can VIOLATE them. Asserted beats
# printed, and the last round made them assertions — but an assertion only
# the happy path ever executes is an assertion nobody has ever watched fail.
# Measured: neutering the bucket-sum comparison, and separately regressing
# the `unaccounted` computation to a literal `0`, each left `--self-test`
# green at 36 of 36. An unverified assertion is decoration.
#
# Neither can be reached from an end-to-end fixture, and that is not a gap in
# the fixtures. Both fire only when this script is WRONG ABOUT ITSELF — the
# scanner and the classifier disagreeing about how many arms exist, or the
# two counts disagreeing with each other — and no fixture tree can make
# correct code contradict itself. Taking the numbers as arguments is what
# makes the failing case reachable at all.
#
# THAT THE CALLS ARE STILL THERE is a separate question, and the first answer
# to it was itself unverifiable: a `grep` of this file for the call text,
# satisfied by the grep's OWN literals. Measured — replacing both main-path
# calls with `:` left it reporting both as present. It is now an OBSERVATION:
# self-test 16c runs the gate under `bash -x` over a known fixture tree and
# reads the execution trace, which records only commands that ran. That check
# has a negative control of its own.

# assert_bucket_sum <total> <scoped> <baselined> <reported> <stale-scoped>
# Silent and 0 when every arm the scanner reported landed in a bucket;
# 1 with the reason when one did not.
assert_bucket_sum() {
  local n_total="$1" n_scoped="$2" n_baselined="$3" n_reported="$4" n_stale_scoped="$5"
  local n_bucketed=$(( n_scoped + n_baselined + n_reported + n_stale_scoped ))
  if (( n_bucketed == n_total )); then
    return 0
  fi
  echo "ORG-ARM-ERROR: accounting invariant violated — the scanner reported $n_total arms" >&2
  echo "               and the classifier bucketed $n_bucketed of them" >&2
  echo "               (SCOPED $n_scoped, BASELINED $n_baselined, REPORTED $n_reported, STALE-SCOPED $n_stale_scoped)." >&2
  echo "               An arm in no bucket is an arm nothing checks, and every line" >&2
  echo "               below this one would describe a tree that was not measured." >&2
  return 1
}

# compute_unaccounted <total> <scoped> <baselined>
# SUBTRACTION, never a literal. The literal `0` that used to sit in the
# summary format string went on reassuring readers while 16 + 16 failed to
# reach 33. A literal cannot be wrong, which is exactly why it cannot warn.
compute_unaccounted() {
  echo "$(( $1 - $2 - $3 ))"
}

# assert_unaccounted_zero <total> <scoped> <baselined>
# The cross-check on assert_bucket_sum, from the other pair of counts.
assert_unaccounted_zero() {
  local u
  u="$(compute_unaccounted "$1" "$2" "$3")"
  if (( u == 0 )); then
    return 0
  fi
  echo "ORG-ARM-ERROR: $1 arms, $2 scoped, $3 baselined — $u unaccounted for," >&2
  echo "               with nothing reported. That combination cannot happen; the gate" >&2
  echo "               is wrong about its own tree. Do not treat this run as a pass." >&2
  return 1
}

# ── Self-test ───────────────────────────────────────────────────────────────
# The guard's own fixtures. Every one of these is a way this scanner could
# report OK while the defect is present.
if [[ "$MODE" == "self-test" ]]; then
  fails=0
  self_root="$(mktemp -d)"
  trap 'rm -rf "$self_root"' EXIT

  TRAIT='pub trait Store: Send + Sync {
    async fn get_node_digest_scoped(&self) -> u8;
    async fn purge_expired_tombstones_scoped(&self) -> u8;
    async fn other_thing_scoped(&self) -> u8;
}
'

  # Runs the gate over a throwaway tree. Captures combined output so a fixture
  # can assert on WHAT was reported, not just on the exit code — a guard that
  # exits 1 with the wrong reason is still a guard nobody can act on.
  run_fixture() { # worker-source, baseline-body  ->  $fx_out, $fx_exit
    local src="$1" base="$2"
    local d="$self_root/f$RANDOM$RANDOM"
    mkdir -p "$d/backend/src/store" "$d/backend/src/workers" "$d/scripts"
    printf '%s' "$TRAIT" > "$d/backend/src/store/mod.rs"
    printf '%s' "$src"   > "$d/backend/src/workers/w.rs"
    printf '%s' "$base"  > "$d/scripts/org_arm_scope.baseline"
    fx_exit=0
    fx_out="$(bash "${BASH_SOURCE[0]}" --root "$d" --baseline "$d/scripts/org_arm_scope.baseline" 2>&1)" || fx_exit=$?
  }

  fixture() { # name, worker-source, baseline-body, expected-exit
    local name="$1"
    run_fixture "$2" "$3"
    if [[ "$fx_exit" != "$4" ]]; then
      echo "SELF-TEST FAIL: $name expected exit $4, got $fx_exit" >&2
      printf '%s\n' "$fx_out" | sed 's/^/      | /' >&2
      fails=1
    else
      echo "  ok: $name"
    fi
  }

  fixture_says() { # name, worker-source, baseline-body, expected-exit, regex
    local name="$1"
    run_fixture "$2" "$3"
    if [[ "$fx_exit" != "$4" ]]; then
      echo "SELF-TEST FAIL: $name expected exit $4, got $fx_exit" >&2
      printf '%s\n' "$fx_out" | sed 's/^/      | /' >&2
      fails=1
    elif ! printf '%s\n' "$fx_out" | grep -Eq "$5"; then
      echo "SELF-TEST FAIL: $name exited $4 but never said /$5/" >&2
      printf '%s\n' "$fx_out" | sed 's/^/      | /' >&2
      fails=1
    else
      echo "  ok: $name"
    fi
  }

  # EVERY regex must match. A report that names one of two escaping
  # statements cannot be asserted with a single pattern — the pattern passes
  # on the line that IS there and says nothing about the one that is not.
  fixture_says_all() { # name, worker-source, baseline-body, expected-exit, regex...
    local name="$1" src="$2" base="$3" want="$4" rx
    shift 4
    run_fixture "$src" "$base"
    if [[ "$fx_exit" != "$want" ]]; then
      echo "SELF-TEST FAIL: $name expected exit $want, got $fx_exit" >&2
      printf '%s\n' "$fx_out" | sed 's/^/      | /' >&2
      fails=1
      return
    fi
    for rx in "$@"; do
      if ! printf '%s\n' "$fx_out" | grep -Eq "$rx"; then
        echo "SELF-TEST FAIL: $name exited $want but never said /$rx/" >&2
        printf '%s\n' "$fx_out" | sed 's/^/      | /' >&2
        fails=1
        return
      fi
    done
    echo "  ok: $name"
  }

  # 1. The single-line store-delegating arm — the shape a line-anchored grep
  #    does see, and the shape 20 of entity_resolution.rs's 30 defects took.
  fixture "single-line store-delegating org arm fails" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            store.get_node_digest_scoped(&mut tx, id).await
        }
        JobScope::Org(_) => store.get_node_digest(id).await,
    }
}
' '' 1

  fixture "the same arm, scoped, passes" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => {
            let mut tx = begin_account_scoped(pool, *account_id).await?;
            store.get_node_digest_scoped(&mut tx, id).await
        }
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = store.get_node_digest_scoped(&mut tx, id).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}
' '' 0

  # 2-3. THE fixtures this guard exists for. A line-anchored `=> store\.`
  #      pattern sees neither: the store call is inside the block, lines below
  #      the arrow. Measured on the pre-fix entity_resolution.rs, that is 10 of
  #      30 arms invisible.
  fixture "block-bodied org arm fails (store call is INSIDE the block)" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => {
            let out = store.get_node_digest(id).await?;
            Ok(out)
        }
    }
}
' '' 1

  fixture "async-block org arm fails (store call is INSIDE the block)" '
async fn f() {
    let r = match scope {
        JobScope::Consumer { pool, account_id } => async { 1 }.await,
        JobScope::Org(_) => async {
            let out = store.get_node_digest(id).await?;
            Ok(out)
        }
        .await,
    };
}
' '' 1

  # 4. `tombstone_purge.rs` matched on `TenantId`, and every `JobScope` grep
  #    missed it. Both enums, or the guard has a blind half.
  fixture "TenantId::Org arm fails too" '
async fn tick(t: &WorkerTenant) {
    match &t.tenant {
        TenantId::Org(_) => {
            t.store.purge_expired_tombstones(cutoff).await
        }
        TenantId::Consumer => consumer_tick(t).await,
    }
}
' '' 1

  # 5. Raw sqlx in the arm.
  fixture "raw sqlx in an org arm fails" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => sqlx::query_scalar("SELECT 1").fetch_one(*pool).await?,
    }
}
' '' 1

  # 6. A generic-executor free function handed the arm-bound pool.
  fixture "the arm pool handed out as an executor fails" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => Ok(load_atom(*pool, atom_id).await?),
    }
}
' '' 1

  # 7. An arm that reaches nothing is not a finding. `=> pool,` (hand the pool
  #    to a UsageContext built after the match) is a real shape in
  #    conflict_detection.rs and infer_expertise.rs. See the header's
  #    "THE ESCAPING POOL" disclosure: this passing is a LIMIT, not a proof.
  fixture "an org arm that reaches no DB passes" '
async fn f() {
    let usage_pool = match scope {
        JobScope::Consumer { pool, .. } => pool,
        JobScope::Org(pool) => pool,
    };
}
' '' 0

  # 8. A commented-out defect is not a defect. If this failed, every doc
  #    comment describing the bug would be a finding.
  fixture "an org arm inside a comment is not a finding" '
// JobScope::Org(_) => store.get_node_digest(id).await,
/* JobScope::Org(_) => store.get_node_digest(id).await, */
fn f() {}
' '' 0

  # 9. THE inverse, and the more dangerous one: a scope primitive named only in
  #    prose or in an SQL string must not count as opening a scope.
  fixture "a scope primitive named only in a comment does not excuse the arm" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => {
            // TODO: wrap this in begin_tenant_system_scoped_without_account
            store.get_node_digest(id).await
        }
    }
}
' '' 1

  fixture "a scope primitive named only in a string does not excuse the arm" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => {
            tracing::warn!("no begin_tenant_system_scoped here");
            store.get_node_digest(id).await
        }
    }
}
' '' 1

  # 10. The masking traps. Each of these, done wrong, blanks the rest of the
  #     file and turns the whole guard into a silent pass.
  fixture "a URL string does not blank the rest of the line" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => { let u = "postgres://h/db"; store.get_node_digest(id).await }
    }
}
' '' 1

  fixture "a lifetime is not an unterminated char literal" '
fn helper<'"'"'a>(s: &'"'"'a str) -> &'"'"'a str { s }
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => store.get_node_digest(id).await,
    }
}
' '' 1

  fixture "a raw string with braces does not break arm extraction" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => {
            let q = r#"SELECT {} FROM t WHERE x = '"'"'}'"'"'"#;
            store.get_node_digest(id).await
        }
    }
}
' '' 1

  # 10a. A CHAR LITERAL WHOSE CONTENT IS A DELIMITER. mask() blanked string
  #      contents and left char contents alone, so a close-paren char literal
  #      was a closing paren to every scan downstream, and a close-brace one
  #      was a closing brace. Both fixtures below reach the DB unscoped, and
  #      the pre-fix gate reported both as
  #
  #          ok: 0 Org arms reach the DB — .. 0 unaccounted for
  #
  #      The arm did not fail the check. The arm was not there. A comma or a
  #      semicolon in a guard vanishes by the same route, and the backward
  #      walk in group_is_macro_arg counts these too.
  fixture_says "a char literal holding ) does not end a match guard" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) if sep == '"'"')'"'"' => { sqlx::query(Q).execute(*pool).await; }
    }
}
' '' 1 'w\.rs:5'

  fixture_says "a char literal holding } does not end an arm body" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let close = '"'"'}'"'"';
            sqlx::query(Q).execute(*pool).await;
        }
    }
}
' '' 1 'w\.rs:5'

  # ── 10b. NOTHING SYNTACTIC MAY RUN BEFORE mask() ──────────────────────────
  # Rust allows a comment between every token of a path:
  #
  #     TenancyDecision /* which enum */ :: Org /* the ctx */ (ctx) => ..
  #
  # The pre-filter that decides which files get masked used to test the
  # whole shape, `Enum\s*::\s*Org\s*\(`, against the RAW text. `\s*` does not
  # match a comment, so this arm failed the filter and the FILE was skipped
  # whole. Not "missed": absent from the scan and from the accounting total,
  # so the buckets summed, `unaccounted` was 0, and the gate printed
  # "ok: 0 Org arms reach the DB" over an unscoped store call. Reproduced.
  #
  # The rule the pre-filter now obeys: it may only ask "could this file
  # possibly contain one", on a token no comment can split. Recognition is
  # the masked scanner or it is nothing.
  fixture_says "a comment between the path tokens does not hide the arm" '
async fn f() {
    match decision {
        TenancyDecision::Consumer(ctx) => 1,
        TenancyDecision /* which enum */ :: Org /* the ctx */ (ctx) => {
            store.get_node_digest(id).await
        }
    }
}
' '' 1 'w\.rs:5'

  # ...and the same file with nothing else in it that the old pre-filter
  # would have matched, so the fixture above cannot pass for the wrong
  # reason. A block comment between `Org` and its `(` is the other half of
  # the same split.
  fixture_says "a comment between Org and its paren does not hide the arm either" '
async fn f() {
    match scope {
        Other => 1,
        JobScope::Org /* the pool */ (pool) => { sqlx::query(Q).execute(*pool).await; }
    }
}
' '' 1 'w\.rs:5'

  # The pre-filter must still SKIP a file with no Org arm in it. If it
  # stopped skipping, the cost note above would be wrong and nobody would
  # notice — but more to the point, a filter that never filters is a filter
  # someone will delete.
  fixture "a file naming none of the three enums is skipped and is not a finding" '
async fn f() {
    match other {
        Whatever::Org(pool) => { sqlx::query(Q).execute(*pool).await; }
    }
}
' '' 0

  # 11. `Org(..)` outside a match arm is a constructor, not an arm.
  fixture "a TenantId::Org constructor call is not an arm" '
fn f() {
    let t = TenantId::Org(org_id.clone());
    let rows = sqlx::query("SELECT 1").fetch_all(pool).await;
}
' '' 0

  # ── 11b. ARM RECOGNITION MUST NOT DEPEND ON LENGTH ────────────────────────
  # The measured defect: arm recognition tested a 300-character window with
  # `^(\s*(?:if\b[^;{]{0,200}?)?=>)`, so an arm whose match guard ran past
  # ~200 characters failed the pattern, fell out through `next unless`, and
  # was never scanned, never counted and never reported. A 290-character
  # guard in front of an unscoped `DELETE` left the gate at exit 0 saying
  # "0 unaccounted for"; the SAME statement behind `if x > 0` failed it.
  #
  # The pair below is that A/B, as fixtures. They must agree. Built by
  # concatenation so the length is a fact of the fixture rather than a
  # literal somebody can shorten by accident while "tidying".
  long_guard="scope.is_org()"
  for _i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    long_guard="$long_guard && ctx.flag_$_i.is_some()"
  done
  if (( ${#long_guard} < 250 )); then
    echo "SELF-TEST FAIL: the long-guard fixture guard is only ${#long_guard} chars;" >&2
    echo "                it has to clear the ~200-character bound it exists to catch." >&2
    fails=1
  fi

  fixture_says "a LONG match guard does not hide the arm (the 200-char skip)" "
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) if $long_guard => { sqlx::query(Q).execute(*pool).await; }
    }
}
" '' 1 'w\.rs:5'

  fixture_says "the same arm behind a SHORT guard fails identically" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) if x > 0 => { sqlx::query(Q).execute(*pool).await; }
    }
}
' '' 1 'w\.rs:5'

  # A guard longer than the one remaining bound is a HARD ERROR naming the
  # file and the line — never a skip. That is the whole rule: a bound may
  # exist for pathological input, and exceeding it may not quietly shrink the
  # tally.
  huge_guard="scope.is_org()"
  while (( ${#huge_guard} < 4200 )); do
    huge_guard="$huge_guard && ctx.flag.is_some()"
  done
  fixture_says "a guard past the scan cap is a HARD ERROR naming file and line" "
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) if $huge_guard => { sqlx::query(Q).execute(*pool).await; }
    }
}
" '' 1 'ORG-ARM-ERROR.*w\.rs:5'

  # An or-pattern arm is the same family of silent vanish: the arrow is
  # behind the alternatives, not behind the first pattern.
  fixture_says "an or-pattern arm is not invisible" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) | JobScope::Team(pool) => { sqlx::query(Q).execute(*pool).await; }
    }
}
' '' 1 'w\.rs:5'

  # ...and the false positives the unbounded scan must NOT produce. This is
  # backend/src/crypto.rs:1095 verbatim: a `matches!` pattern with a guard,
  # which has no arrow and is not an arm.
  fixture "a matches! guard is not a match arm" '
fn f() {
    assert!(matches!(plane.tenant(), TenantId::Org(o) if o == "org_a"));
    let rows = sqlx::query("SELECT 1").fetch_all(pool).await;
}
' '' 0

  fixture "an if-let on an Org pattern is not a match arm" '
fn f() {
    if let JobScope::Org(pool) = scope {
        let rows = sqlx::query("SELECT 1").fetch_all(pool).await;
    }
}
' '' 0

  # ── 11c. THE TWO SHAPES BEHIND A CLOSER, WHICH MUST NOT BE READ ALIKE ─────
  # `Some(JobScope::Org(p)) => ..` and
  # `A if !matches!(d, TenancyDecision::Org(_)) => ..` both put a `=>` behind
  # a closer the pattern scan never opened. The first is an Org arm. The
  # second is not: its own pattern is `A`, the `Org(..)` is inside the GUARD,
  # and the guard NEGATES it, so that body is the one thing that never runs
  # for an org tenant. The pair below is that A/B.
  #
  # A nested `Some(Org(..))` arm is an ARM, analysed like any other. It used to
  # stop the gate with an error instead, because its arrow sits behind a closer
  # this scan never opened. Stopping was the safe reading while the closers were
  # ambiguous; reading them is now proven safe (see the header), and thirteen
  # real arms in the tree are this shape, so an error here would have been a
  # daily false alarm. What must never happen is the third option: silently
  # skipping it. That is what this fixture pins.
  fixture_says "a nested Some(Org(..)) arm is analysed, not skipped and not an error" '
async fn f() {
    match maybe_scope {
        Some(JobScope::Consumer { pool, account_id }) => 1,
        Some(JobScope::Org(pool)) => { sqlx::query(Q).execute(*pool).await; }
        None => 0,
    }
}
' '' 1 'w\.rs:5'

  # ...and the nested CONSTRUCTOR, which has no arrow behind its closers, is
  # still quietly not an arm. Without this the stop above would fire on every
  # `Some(TenantId::Org(x))` in the tree.
  fixture "a nested Some(Org(..)) constructor is still not an arm" '
fn f() {
    let s = Some(JobScope::Org(pool));
    let t = wrap(Some(TenantId::Org(org_id.clone())));
    let rows = sqlx::query("SELECT 1").fetch_all(pool).await;
}
' '' 0

  # ...and the other half of the pair. This exact source was REPORTED as
  # `w.rs:3 TenancyDecision::Org(_) UNSCOPED` before the macro-argument test
  # was added: the scan skipped the `matches!` closer, took the `=>` behind
  # it, and attributed a body that runs on every tenant EXCEPT an org one to
  # Org.
  #
  # Being a false positive is the smaller half of the harm. The baseline is
  # keyed by ARM, so the entry someone writes to silence this survives the
  # deletion of the `!` — and then it suppresses a genuinely unscoped Org
  # path, with a reason line that still reads as if somebody checked.
  fixture "a NEGATED matches! guard is not an Org arm" '
async fn f() {
    match plan {
        Plan::Fast if !matches!(decision, TenancyDecision::Org(_)) => {
            store.get_node_digest(id).await
        }
        _ => 0,
    }
}
' '' 0

  # The same shape one macro out. `assert!`, `debug_assert!` and friends wrap
  # the `matches!`, so the closer the scan meets first is the INNER one; both
  # have to be refused or the outer form walks straight back in.
  fixture "an Org pattern inside a nested macro call is not an arm" '
async fn f() {
    match plan {
        Plan::Fast if !cfg!(all(matches!(decision, TenancyDecision::Org(_)))) => {
            store.get_node_digest(id).await
        }
        _ => 0,
    }
}
' '' 0

  # ── 12. SCOPE-BYPASSED ────────────────────────────────────────────────────
  # THE non-fix: add the opener, leave the statement outside it. The whole
  # point of the second verdict, and for one release it computed the word,
  # printed it under --list, and exited 0 because the gate selected failures
  # with `$4 == "UNSCOPED"`.
  fixture_says "an opener plus an ambient statement FAILS (SCOPE-BYPASSED)" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let a = store.get_node_digest_scoped(&mut tx, id).await?;
            tx.commit().await?;
            sqlx::query(OTHER).execute(*pool).await
        }
    }
}
' '' 1 'SCOPE-BYPASSED'

  # The report has to be actionable: the escaping statement, by line, with its
  # text. "Somewhere in this arm" sends a reader back to re-derive the finding.
  fixture_says "the SCOPE-BYPASSED report names the escaping statement and its line" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let a = store.get_node_digest_scoped(&mut tx, id).await?;
            tx.commit().await?;
            sqlx::query(OTHER).execute(*pool).await
        }
    }
}
' '' 1 'w\.rs:9 .*bare-pool-executor'

  # ...and the SOURCE TEXT of that line, read back out of the file. That is
  # what proves the line number resolves to the statement rather than to some
  # other line that happens to be nearby.
  fixture_says "the SCOPE-BYPASSED report echoes the escaping source line" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let a = store.get_node_digest_scoped(&mut tx, id).await?;
            tx.commit().await?;
            sqlx::query(OTHER).execute(*pool).await
        }
    }
}
' '' 1 'sqlx::query\(OTHER\)\.execute\(\*pool\)'

  # The second detector, and the reason the store scan reads EVERY call rather
  # than the first: here the first store call IS the `_scoped` twin.
  fixture_says "a _scoped store call followed by an unscoped one is SCOPE-BYPASSED" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let a = store.get_node_digest_scoped(&mut tx, id).await?;
            tx.commit().await?;
            store.other_thing(id).await
        }
    }
}
' '' 1 'store-call\(other_thing\)'

  # ── 12b. EVERY escaping statement, not the first one ──────────────────────
  # The escape list deduped by DETECTOR SIGNAL, so an arm with two
  # `.execute(*pool)` statements produced one report line. A reader fixes the
  # line the report names, re-runs the gate, sees green, and ships the other
  # one. Both lines, or the report is a trap.
  fixture_says_all "two bare-pool statements report BOTH lines" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let a = store.get_node_digest_scoped(&mut tx, id).await?;
            tx.commit().await?;
            sqlx::query(FIRST).execute(*pool).await?;
            sqlx::query(SECOND).execute(*pool).await
        }
    }
}
' '' 1 'w\.rs:9 +\(bare-pool-executor' 'w\.rs:10 +\(bare-pool-executor' \
       'sqlx::query\(FIRST\)' 'sqlx::query\(SECOND\)'

  # The same, through the store detector: two calls to ONE unscoped twin.
  # The per-name dedupe collapsed these to a single line for the same reason.
  fixture_says_all "two calls to the same unscoped store method report BOTH lines" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let a = store.get_node_digest_scoped(&mut tx, id).await?;
            tx.commit().await?;
            let b = store.other_thing(id).await?;
            store.other_thing(other).await
        }
    }
}
' '' 1 'w\.rs:9 +\(store-call\(other_thing\)\)' 'w\.rs:10 +\(store-call\(other_thing\)\)'

  # ...and the collapse that MUST still happen: one call that two detectors
  # both describe is one statement and one line. `get_node_digest` is a
  # `store.` call AND has a `_scoped` twin in the fixture trait, so it fires
  # both `store-call` and `unscoped-twin`. Without call-site keying the fix
  # above would double every store line in every report.
  run_fixture '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            tx.commit().await?;
            store.get_node_digest(id).await
        }
    }
}
' ''
  n_lines="$(printf '%s\n' "$fx_out" | grep -Ec 'w\.rs:8 +\(' || true)"
  if [[ "$fx_exit" != 1 ]]; then
    echo "SELF-TEST FAIL: one call described by two detectors should still FAIL, got $fx_exit" >&2
    fails=1
  elif [[ "$n_lines" != 1 ]]; then
    echo "SELF-TEST FAIL: one escaping call produced $n_lines report lines, expected 1" >&2
    printf '%s\n' "$fx_out" | sed 's/^/      | /' >&2
    fails=1
  else
    echo "  ok: one call, two detectors, ONE report line"
  fi

  # The false-positive guards on detector (i), both of which the header
  # promises. Loosening either is how a real fix gets reported as a bypass and
  # the next reviewer stops believing the guard.
  fixture "a nested pool argument in the SQL position is not a bypass" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = sqlx::query(build_sql(pool)).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}
' '' 0

  fixture "self.pool is not the arm binding" '
async fn f(&self) {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = sqlx::query(Q).execute(&mut *tx).await?;
            let n = sqlx::query(R).execute(self.pool).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}
' '' 0

  # A SCOPE-BYPASSED arm is baselineable like any other failure — the escape
  # hatch has to exist, or the next reviewer loosens a matcher instead.
  fixture "a baselined SCOPE-BYPASSED arm passes" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let a = store.get_node_digest_scoped(&mut tx, id).await?;
            tx.commit().await?;
            sqlx::query(OTHER).execute(*pool).await
        }
    }
}
' '# reason: fixture — the second statement reads the CONTROL plane.
backend/src/workers/w.rs::f#1' 0

  # 13. The baseline: honours entries, and refuses to keep stale ones.
  BASE_ONE='# reason: fixture — the table this arm touches carries no member-ocean policy.
backend/src/workers/w.rs::f#1'

  fixture "a baselined arm passes" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => store.get_node_digest(id).await,
    }
}
' "$BASE_ONE" 0

  fixture "a stale baseline entry fails" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            store.get_node_digest_scoped(&mut tx, id).await
        }
    }
}
' "$BASE_ONE" 1

  # An entry naming an arm that does not exist AT ALL is stale too, and it is
  # the shape a file rename leaves behind.
  fixture_says "a baseline entry matching no arm at all fails (ORPHAN)" '
fn f() {}
' '# reason: fixture — this arm was deleted three refactors ago.
backend/src/workers/w.rs::gone#1' 1 'stale baseline'

  fixture "a baseline entry with no '# reason:' fails" '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => store.get_node_digest(id).await,
    }
}
' '# some unrelated header comment

backend/src/workers/w.rs::f#1' 1

  # ── 14. The EMPTY reason ──────────────────────────────────────────────────
  # `case "$prev" in "# reason:"*)` is a PREFIX test, so a bare `# reason:`
  # passed it and justified nothing. The check has to demand text, or it is
  # itself the "rationale nobody wrote" it was added to prevent.
  ARM_ONE='
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => store.get_node_digest(id).await,
    }
}
'

  fixture_says "an empty '# reason:' fails" "$ARM_ONE" '# reason:
backend/src/workers/w.rs::f#1' 1 "is empty"

  # Built with an explicit `printf` so the trailing whitespace is VISIBLE in
  # this file. A fixture whose whole point is invisible characters must not
  # depend on an editor preserving them.
  WS_REASON="# reason:$(printf '  \t ')
backend/src/workers/w.rs::f#1"
  fixture_says "a whitespace-only '# reason:' fails" "$ARM_ONE" "$WS_REASON" 1 "is empty"

  fixture "a '# reason:' with one word passes" "$ARM_ONE" '# reason: control-plane
backend/src/workers/w.rs::f#1' 0

  # A duplicate key is two reasons for one arm, and it double-counts that arm.
  fixture_says "a duplicated baseline key fails" "$ARM_ONE" '# reason: control-plane
backend/src/workers/w.rs::f#1
# reason: control-plane, again
backend/src/workers/w.rs::f#1' 1 'duplicate baseline'

  # 15. Two arms in one function keep distinct keys, so baselining one cannot
  #     accidentally baseline the other.
  fixture "two arms in one fn are keyed separately (baselining one leaves the other red)" '
async fn f() {
    let a = match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => store.get_node_digest(id).await,
    };
    let b = match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => store.get_node_digest(other).await,
    };
}
' "$BASE_ONE" 1

  # ── 16. The accounting invariant, tested on the classifier directly ───────
  # An end-to-end fixture cannot produce a verdict the scanner does not emit,
  # and "a verdict no bucket handles" is exactly how the last arm went missing.
  # So the classifier is driven with synthetic records.
  R_SCOPED=$'k1\tbackend/src/workers/w.rs:1\tJobScope::Org(pool)\tSCOPED\tsqlx::query\t'
  R_UNSCOPED=$'k2\tbackend/src/workers/w.rs:2\tJobScope::Org(pool)\tUNSCOPED\tsqlx::query\t'
  R_BYPASS=$'k3\tbackend/src/workers/w.rs:3\tJobScope::Org(pool)\tSCOPE-BYPASSED\tsqlx::query\t3|bare-pool-executor(execute <- pool)'

  cls_exit=0
  cls_out="$(classify_arms "" "$R_SCOPED
$R_UNSCOPED
$R_BYPASS" 2>&1)" || cls_exit=$?
  if [[ "$cls_exit" != 0 ]]; then
    echo "SELF-TEST FAIL: classify_arms exited $cls_exit on three well-formed records" >&2; fails=1
  elif [[ "$(printf '%s\n' "$cls_out" | wc -l | tr -d ' ')" != 3 ]]; then
    echo "SELF-TEST FAIL: classify_arms bucketed $(printf '%s\n' "$cls_out" | wc -l | tr -d ' ') of 3 records" >&2
    printf '%s\n' "$cls_out" | sed 's/^/      | /' >&2
    fails=1
  elif [[ "$(bucket_count SCOPED "$cls_out")" != 1 || "$(bucket_count REPORTED "$cls_out")" != 2 ]]; then
    echo "SELF-TEST FAIL: classify_arms put an unbaselined SCOPE-BYPASSED record somewhere other than REPORTED" >&2
    printf '%s\n' "$cls_out" | sed 's/^/      | /' >&2
    fails=1
  else
    echo "  ok: every record lands in exactly one bucket (SCOPED 1, REPORTED 2)"
  fi

  cls_exit=0
  cls_out="$(classify_arms "" $'k9\tbackend/src/workers/w.rs:9\tJobScope::Org(pool)\tSOME-FUTURE-VERDICT\tsqlx::query\t' 2>&1)" || cls_exit=$?
  if [[ "$cls_exit" == 0 ]]; then
    echo "SELF-TEST FAIL: a verdict with no bucket was silently dropped and classify_arms exited 0" >&2; fails=1
  elif ! printf '%s\n' "$cls_out" | grep -q "no bucket for it"; then
    echo "SELF-TEST FAIL: an unbucketable verdict failed without saying why" >&2
    printf '%s\n' "$cls_out" | sed 's/^/      | /' >&2
    fails=1
  else
    echo "  ok: a verdict no bucket handles is an ERROR, not a dropped row"
  fi

  # And end-to-end: the summary line's last field is COMPUTED, so a tree whose
  # buckets do add up says 0, and the number is arrived at by subtraction
  # rather than typed into the format string.
  run_fixture '
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = sqlx::query(Q).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}
async fn g() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => store.get_node_digest(id).await,
    }
}
' '# reason: fixture — control plane.
backend/src/workers/w.rs::g#1'
  if [[ "$fx_exit" != 0 ]]; then
    echo "SELF-TEST FAIL: the accounting fixture should be green, got exit $fx_exit" >&2
    printf '%s\n' "$fx_out" | sed 's/^/      | /' >&2
    fails=1
  elif ! printf '%s\n' "$fx_out" | grep -q "^ok: 2 Org arms reach the DB — 1 open a scope, 1 are baselined with a reason, 0 unaccounted for$"; then
    echo "SELF-TEST FAIL: the summary line does not add up" >&2
    printf '%s\n' "$fx_out" | sed 's/^/      | /' >&2
    fails=1
  else
    echo "  ok: the summary line's buckets sum to the arm count"
  fi

  # ── 16b. THE ACCOUNTING INVARIANTS THEMSELVES ─────────────────────────────
  # The last round made these assertions rather than prints. It did not test
  # them. Measured: neutering the bucket-sum comparison, and separately
  # regressing the `unaccounted` computation to a literal `0`, each left this
  # self-test green at 36 of 36. An assertion nobody has watched fail is
  # decoration, and this file's whole thesis is that a control which cannot
  # be seen failing is a control that is not there.
  #
  # They are driven directly because they CANNOT be reached end-to-end: both
  # fire only when the script contradicts itself, and no fixture tree makes
  # correct code do that.

  inv_exit=0; inv_out="$(assert_bucket_sum 3 1 1 1 0 2>&1)" || inv_exit=$?
  if [[ "$inv_exit" != 0 || -n "$inv_out" ]]; then
    echo "SELF-TEST FAIL: assert_bucket_sum complained about buckets that DO sum (1+1+1+0 = 3)" >&2
    printf '%s\n' "$inv_out" | sed 's/^/      | /' >&2
    fails=1
  else
    echo "  ok: assert_bucket_sum is silent when every arm is in a bucket"
  fi

  inv_exit=0; inv_out="$(assert_bucket_sum 4 1 1 1 0 2>&1)" || inv_exit=$?
  if [[ "$inv_exit" == 0 ]]; then
    echo "SELF-TEST FAIL: assert_bucket_sum accepted 3 bucketed arms against 4 reported." >&2
    echo "                That fourth arm is the one that goes missing while the summary" >&2
    echo "                line goes on saying '0 unaccounted for'." >&2
    fails=1
  elif ! printf '%s\n' "$inv_out" | grep -q "accounting invariant violated"; then
    echo "SELF-TEST FAIL: assert_bucket_sum failed without naming the invariant it is" >&2
    echo "                enforcing, so the reader cannot tell what went wrong." >&2
    printf '%s\n' "$inv_out" | sed 's/^/      | /' >&2
    fails=1
  else
    echo "  ok: assert_bucket_sum FAILS, by name, when one arm lands in no bucket"
  fi

  inv_val="$(compute_unaccounted 5 1 1)"
  if [[ "$inv_val" != 3 ]]; then
    echo "SELF-TEST FAIL: compute_unaccounted 5 1 1 said '$inv_val', not 3. The summary's" >&2
    echo "                last number has to be a SUBTRACTION. A literal cannot be wrong," >&2
    echo "                which is precisely why a literal cannot warn." >&2
    fails=1
  else
    echo "  ok: unaccounted is arrived at by subtraction (5 - 1 - 1 = 3)"
  fi

  inv_val="$(compute_unaccounted 2 1 1)"
  if [[ "$inv_val" != 0 ]]; then
    echo "SELF-TEST FAIL: compute_unaccounted 2 1 1 said '$inv_val', not 0" >&2
    fails=1
  else
    echo "  ok: ...and it reaches 0 only when the counts actually add up"
  fi

  inv_exit=0; inv_out="$(assert_unaccounted_zero 2 1 1 2>&1)" || inv_exit=$?
  if [[ "$inv_exit" != 0 || -n "$inv_out" ]]; then
    echo "SELF-TEST FAIL: assert_unaccounted_zero complained about 2 = 1 + 1" >&2
    printf '%s\n' "$inv_out" | sed 's/^/      | /' >&2
    fails=1
  else
    echo "  ok: assert_unaccounted_zero is silent when nothing is unaccounted for"
  fi

  inv_exit=0; inv_out="$(assert_unaccounted_zero 5 1 1 2>&1)" || inv_exit=$?
  if [[ "$inv_exit" == 0 ]]; then
    echo "SELF-TEST FAIL: assert_unaccounted_zero accepted 5 arms with 1 scoped and 1" >&2
    echo "                baselined. Three arms are unaccounted for and it said nothing." >&2
    fails=1
  elif ! printf '%s\n' "$inv_out" | grep -q "3 unaccounted for"; then
    echo "SELF-TEST FAIL: assert_unaccounted_zero failed without saying HOW MANY arms" >&2
    echo "                were unaccounted for, which is the whole content of the finding." >&2
    printf '%s\n' "$inv_out" | sed 's/^/      | /' >&2
    fails=1
  else
    echo "  ok: assert_unaccounted_zero FAILS, with the count, when arms are unaccounted for"
  fi

  # ── 16c. THE INVARIANTS RUN ON THE MAIN PATH — OBSERVED, NOT GREPPED ──────
  # Driving the functions catches a BROKEN invariant. Deleting the CALL is
  # the other way to lose one, and nothing above can see that.
  #
  # This check used to be a `grep -qF` of THIS FILE for the two call texts.
  # The grep's OWN loop literals satisfied it, so it could not fail.
  # Measured: replacing both real main-path calls with `:` left it printing
  #
  #     ok: the gate's main path calls assert_bucket_sum
  #     ok: the gate's main path calls assert_unaccounted_zero
  #
  # with both invariants gone. A control that matches its own source is the
  # exact defect this file exists to remove, wearing the fix's uniform.
  #
  # Text cannot answer the question anyway. `if false; then assert_bucket_sum
  # "$n_total" ..; fi` satisfies any grep and runs nothing. So the gate is
  # RUN over a fixture tree under `bash -x` and the EXECUTION TRACE is read.
  # A trace line records a command that actually ran, with its arguments
  # already expanded; no literal anywhere in this file can put one there.
  #
  # The fixture tree is known — one SCOPED arm, one BASELINED arm — so the
  # expected argument vector is known too. Demanding `2 1 1 0 0` rather than
  # merely "was called" also rejects a call wired to the wrong counts, and it
  # is ALL FIVE of `assert_bucket_sum`'s arguments, anchored at the end of
  # the trace line. A four-argument prefix leaves `n_stale_scoped` unchecked
  # and, unanchored, accepts any fifth argument at all — so the one bucket
  # this fixture tree happens to leave empty would be the one bucket the
  # observation never looks at.

  inv_tree() { # dir, baseline-body — a two-arm tree: one SCOPED, one failing
    local d="$1"
    mkdir -p "$d/backend/src/store" "$d/backend/src/workers" "$d/scripts"
    printf '%s' "$TRAIT" > "$d/backend/src/store/mod.rs"
    cat > "$d/backend/src/workers/w.rs" <<'INVRS'
async fn f() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(pool) => {
            let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
            let out = sqlx::query(Q).execute(&mut *tx).await?;
            tx.commit().await?;
            Ok(out)
        }
    }
}
async fn g() {
    match scope {
        JobScope::Consumer { pool, account_id } => 1,
        JobScope::Org(_) => store.get_node_digest(id).await,
    }
}
INVRS
    printf '%s' "$2" > "$d/scripts/org_arm_scope.baseline"
  }

  # Runs a gate SCRIPT over a fixture tree with xtrace on, into $tr_out.
  # PS4 is pinned so the trace prefix does not depend on the environment's.
  trace_gate() { # script, tree
    tr_out="$self_root/trace.$RANDOM$RANDOM"
    tr_exit=0
    PS4='+ ' bash -x "$1" --root "$2" --baseline "$2/scripts/org_arm_scope.baseline" \
      >"$tr_out" 2>&1 || tr_exit=$?
  }

  # `^\++ ` — bash repeats PS4's first character once per nesting level, so
  # the depth of the call site does not matter. Anchoring on the COMMAND
  # WORD is what stops a mere mention of the name in some other command's
  # arguments from counting as an invocation.
  #
  # TWO forms, because presence and absence want opposite ends of the line.
  #
  #   inv_ran   — anchored at BOTH ends. Used to demand a call, so it must
  #               demand the WHOLE argument vector. Unanchored, the pattern
  #               `assert_bucket_sum 2 1 1 0` is satisfied by
  #               `assert_bucket_sum 2 1 1 0 7`: a call wired with the wrong
  #               fifth argument passes a check whose whole point is that the
  #               arguments are the tree's own.
  #   inv_saw   — unanchored, a prefix. Used to deny a call, where the widest
  #               pattern is the fail-closed one: ANY invocation at all is
  #               enough to sink the negative control.
  inv_ran() { LC_ALL=C grep -Eq "^\++ $1\$" "$tr_out"; }
  inv_saw() { LC_ALL=C grep -Eq "^\++ $1" "$tr_out"; }

  inv_green="$self_root/invwiring_green"
  inv_tree "$inv_green" '# reason: fixture — control plane.
backend/src/workers/w.rs::g#1'
  trace_gate "${BASH_SOURCE[0]}" "$inv_green"
  if [[ "$tr_exit" != 0 ]]; then
    echo "SELF-TEST FAIL: the invariant-wiring fixture tree should be green, got exit $tr_exit" >&2
    tail -20 "$tr_out" | sed 's/^/      | /' >&2
    fails=1
  else
    for inv_want in 'assert_bucket_sum 2 1 1 0 0' 'assert_unaccounted_zero 2 1 1'; do
      if inv_ran "$inv_want"; then
        echo "  ok: a real gate run EXECUTED ${inv_want%% *}, with the tree's own counts"
      else
        echo "SELF-TEST FAIL: a real gate run never executed \`$inv_want\`." >&2
        echo "                An invariant nothing invokes is an invariant nothing enforces." >&2
        echo "                This is read from the execution trace, so a call that is present" >&2
        echo "                in the source but never reached fails here too." >&2
        # The whole vector is demanded, so a NEAR miss — right command, wrong
        # arguments — is the likely reading and the useless message is the one
        # that does not print it.
        inv_seen="$(LC_ALL=C grep -E "^\++ ${inv_want%% *} " "$tr_out" | LC_ALL=C sort -u || true)"
        if [[ -n "$inv_seen" ]]; then
          # `sed -E`, not plain `sed`: in BSD basic regex `\+` is a LITERAL
          # plus, so `^\++ ` silently matches nothing and the line prints raw.
          printf '%s\n' "$inv_seen" | LC_ALL=C sed -E 's/^\++ /                it ran instead as: /' >&2
        else
          echo "                Nothing named ${inv_want%% *} ran at all." >&2
        fi
        fails=1
      fi
    done
  fi

  # ...and it must run on a DIRTY tree as well, before any verdict is
  # printed. `assert_unaccounted_zero` deliberately sits after the failure
  # exit, so it does not run here and is not demanded.
  inv_red="$self_root/invwiring_red"
  inv_tree "$inv_red" ''
  trace_gate "${BASH_SOURCE[0]}" "$inv_red"
  if [[ "$tr_exit" == 0 ]]; then
    echo "SELF-TEST FAIL: the unbaselined invariant-wiring tree should FAIL, got exit 0" >&2
    fails=1
  elif ! inv_ran 'assert_bucket_sum 2 1 0 1 0'; then
    echo "SELF-TEST FAIL: on a tree with a REPORTED arm the gate never executed" >&2
    echo "                assert_bucket_sum. The bucket-sum invariant has to fire BEFORE" >&2
    echo "                the report, or the report describes a tree nothing counted." >&2
    fails=1
  else
    echo "  ok: ...and it runs before the report on a tree that FAILS, too"
  fi

  # The negative control on the observation itself. Neuter both main-path
  # calls in a COPY of this script and re-observe: the trace must lose them.
  # Without this, "the trace contains the call" is one more claim nobody has
  # watched fail — which is how the grep it replaces survived a release.
  #
  # The mutation is fail-closed in three directions. If its pattern stops
  # matching the call sites, the count check below fires; if it matches and
  # the calls still run, the trace check fires; and if the mutant run never
  # REACHES the main path, the reached-the-end check fires.
  #
  # That third direction is the one an absence check always forgets. "The
  # trace does not contain `assert_bucket_sum`" is true of a run that died in
  # argument parsing, of a run that died in the scanner, and of a run that
  # never started. Each of those prints the same reassuring `ok:` line as a
  # working control, and each proves nothing about whether the observation
  # can see a deleted call. Replacing the two calls with `:` must not change
  # anything else, so the mutant is required to exit 0 on the GREEN tree AND
  # to be observed running `compute_unaccounted` with that tree's own counts
  # — a main-path command that sits AFTER both mutated call sites and is not
  # itself mutated. An exit code alone would not do it: the last `|| exit 1`
  # is one of the things being removed.
  #
  # SC2016: `\$n_total` must stay UNEXPANDED in both patterns below. It is
  # the literal call-site text being MUTATED, not a value. Note that neither
  # of these is the check — they build and size the negative control.
  inv_mutant="$self_root/mutant_gate.sh"
  # shellcheck disable=SC2016
  perl -pe 's/^(\s*)(?:assert_bucket_sum|assert_unaccounted_zero)\s+"\$n_total".*$/$1:/' \
    "${BASH_SOURCE[0]}" > "$inv_mutant"
  # shellcheck disable=SC2016
  inv_muted="$(LC_ALL=C grep -Ec '^[[:space:]]*(assert_bucket_sum|assert_unaccounted_zero)[[:space:]]+"\$n_total"' "${BASH_SOURCE[0]}" || true)"
  if [[ "$inv_muted" != 2 ]]; then
    echo "SELF-TEST FAIL: the negative control matched $inv_muted of the 2 main-path" >&2
    echo "                invariant calls, so it is not testing what it claims. Fix its" >&2
    echo "                pattern before trusting the observation above." >&2
    fails=1
  else
    trace_gate "$inv_mutant" "$inv_green"
    if [[ "$tr_exit" != 0 ]]; then
      echo "SELF-TEST FAIL: the mutant gate exited $tr_exit on the GREEN tree. Removing two" >&2
      echo "                invariant calls must not change anything else, so it died before" >&2
      echo "                the main path and the ABSENCE below would prove nothing." >&2
      tail -20 "$tr_out" | sed 's/^/      | /' >&2
      fails=1
    elif ! inv_ran 'compute_unaccounted 2 1 1'; then
      echo "SELF-TEST FAIL: the mutant gate exited 0 but the trace never shows it reaching" >&2
      echo "                \`compute_unaccounted 2 1 1\`, the main-path command that sits after" >&2
      echo "                both mutated call sites. An exit code is not evidence of arrival:" >&2
      echo "                a run that stopped early loses the invariant calls too, and this" >&2
      echo "                control would have called that a pass." >&2
      tail -20 "$tr_out" | sed 's/^/      | /' >&2
      fails=1
    elif inv_saw 'assert_bucket_sum ' || inv_saw 'assert_unaccounted_zero '; then
      echo "SELF-TEST FAIL: both main-path invariant calls were replaced with ':' and the" >&2
      echo "                trace still shows one of them running. The observation is not" >&2
      echo "                observing; it would report OK with the invariants deleted." >&2
      fails=1
    else
      echo "  ok: neutering both main-path calls makes that observation go RED, on a run"
      echo "      the trace shows reaching the end of the main path"
    fi
  fi

  # 17. The no-op failure mode. If the Store trait cannot be parsed, the twin
  #     roster is empty and one detector silently switches off. That is an
  #     error, not a zero.
  err_root="$self_root/traitgone"
  mkdir -p "$err_root/backend/src/store" "$err_root/backend/src/workers" "$err_root/scripts"
  printf 'pub trait Storage: Send + Sync {\n    async fn a_scoped(&self) -> u8;\n}\n' > "$err_root/backend/src/store/mod.rs"
  printf 'fn f() { let t = JobScope::Org(pool); }\n' > "$err_root/backend/src/workers/w.rs"
  : > "$err_root/scripts/org_arm_scope.baseline"
  if bash "${BASH_SOURCE[0]}" --root "$err_root" --baseline "$err_root/scripts/org_arm_scope.baseline" >/dev/null 2>&1; then
    echo "SELF-TEST FAIL: an unfindable Store trait exited 0" >&2; fails=1
  else
    echo "  ok: an unfindable Store trait is an ERROR, not a zero"
  fi

  # 18. The scanner must actually be looking at something. A tree with no
  #     backend/src at all is an error, never a green run.
  empty_root="$self_root/empty"
  mkdir -p "$empty_root/scripts"
  : > "$empty_root/scripts/org_arm_scope.baseline"
  if bash "${BASH_SOURCE[0]}" --root "$empty_root" --baseline "$empty_root/scripts/org_arm_scope.baseline" >/dev/null 2>&1; then
    echo "SELF-TEST FAIL: a tree with no backend/src exited 0" >&2; fails=1
  else
    echo "  ok: a tree with no backend/src is an ERROR, not a zero"
  fi

  [[ "$fails" -eq 0 ]] || { echo "self-test FAILED" >&2; exit 1; }
  echo "self-test ok"
  exit 0
fi

# ── Measure ─────────────────────────────────────────────────────────────────
findings="$(scan_tree "$ROOT")" || exit 1
findings="$(printf '%s\n' "$findings" | sed '/^$/d')"

if [[ "$MODE" == "list" ]]; then
  printf '%s\n' "$findings"
  exit 0
fi

if [[ ! -f "$BASELINE" ]]; then
  echo "FAIL: baseline not found at $BASELINE" >&2
  echo "      Every allowed exception lives there, with a reason. Create it." >&2
  exit 1
fi

baseline_fail=0
check_baseline_reasons "$BASELINE"    || baseline_fail=1
check_baseline_duplicates "$BASELINE" || baseline_fail=1

baseline_keys="$(read_baseline_keys "$BASELINE" | LC_ALL=C sort)"

# ── Bucket every arm, exactly once ──────────────────────────────────────────
buckets="$(classify_arms "$baseline_keys" "$findings")" || {
  echo "ORG-ARM-ERROR: the accounting refused to classify this tree. Nothing below ran." >&2
  exit 1
}

n_total="$(printf '%s\n' "$findings" | sed '/^$/d' | wc -l | tr -d ' ')"
n_scoped="$(bucket_count SCOPED "$buckets")"
n_baselined="$(bucket_count BASELINED "$buckets")"
n_reported="$(bucket_count REPORTED "$buckets")"
n_stale_scoped="$(bucket_count STALE-SCOPED "$buckets")"
n_orphan="$(bucket_count ORPHAN "$buckets")"

# THE INVARIANT, asserted rather than printed. `n_total` is counted from the
# scanner's output; the four arm buckets are counted from the classifier's.
# They are two pipelines, so an arm that falls out of the classification shows
# up here as a mismatch instead of as a summary line that still says zero.
# The assertion itself lives in a function so the self-test can violate it.
assert_bucket_sum "$n_total" "$n_scoped" "$n_baselined" "$n_reported" "$n_stale_scoped" || exit 1

violations=0

if (( n_reported > 0 )); then
  violations=1
  unscoped_rows="$(bucket_rows REPORTED "$buckets" | LC_ALL=C awk -F'\t' '$5 == "UNSCOPED"')"
  bypassed_rows="$(bucket_rows REPORTED "$buckets" | LC_ALL=C awk -F'\t' '$5 == "SCOPE-BYPASSED"')"

  if [[ -n "$unscoped_rows" ]]; then
    echo "FAIL: an Org match-arm reaches the database and opens no scope." >&2
    echo >&2
    printf '%s\n' "$unscoped_rows" | LC_ALL=C awk -F'\t' '{
      printf "  %s\n      arm:     %s\n      key:     %s\n      reaches: %s\n\n", $3, $4, $2, $6
    }' >&2
  fi

  if [[ -n "$bypassed_rows" ]]; then
    echo "FAIL: an Org match-arm opens a scope and then reaches the database OUTSIDE it." >&2
    echo >&2
    while IFS=$'\t' read -r _bucket key where arm verdict _signals escapes; do
      [[ -n "$key" ]] || continue
      printf '  %s\n      arm:     %s\n      key:     %s\n      verdict: %s\n' \
        "$where" "$arm" "$key" "$verdict" >&2
      printf '      escapes:\n' >&2
      # One line per escaping statement, with its source text, so the reader
      # does not have to re-derive the finding from a signal name.
      printf '%s\n' "$escapes" | tr ',' '\n' | while IFS='|' read -r eline detector; do
        [[ -n "$eline" ]] || continue
        src=""
        file="${where%%:*}"
        [[ -f "$ROOT/$file" ]] && src="$(sed -n "${eline}p" "$ROOT/$file" | sed 's/^[[:space:]]*//')"
        printf '        %s:%s  (%s)\n' "$file" "$eline" "$detector" >&2
        [[ -n "$src" ]] && printf '            %s\n' "$src" >&2
      done
      echo >&2
    done <<< "$bypassed_rows"
    cat >&2 <<'EOF'
An opener SOMEWHERE in the arm does not put THAT statement inside it. This is
the shape a hurried fix leaves: the opener was added, the statement was not
moved. `set_config(.., true)` is transaction-local, so a statement issued
against the pooled handle carries neither `app.tenant_actor` nor `app.user_id`
however many scoped transactions the same arm opens around it.

WHAT TO DO

Move the statement inside the transaction — hand it `&mut *tx` instead of the
pool, or call the `_scoped` twin with `&mut tx` — and commit once, after it:

    let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
    let a = store.thing_scoped(&mut tx, id).await?;
    sqlx::query(OTHER).execute(&mut *tx).await?;    // was: .execute(*pool)
    tx.commit().await?;

EOF
  fi

  cat >&2 <<'EOF'
WHY THIS IS A FAILURE AND NOT A STYLE NOTE

The tables an Org arm touches carry migrations_tenant/0009_tenant_rls's
`member_ocean_isolation` policy, resolved through
`tenant_rls_visible_graph_ids()`. Without `app.tenant_actor` or `app.user_id`
that function does NOT return the empty set. It hides every `member` and
`team` graph and leaves `org` and `personal` graphs visible:
`tenant_rls_actor_user_id()` is NULL when unset (`0009:1084`), which makes the
member conjunct NULL and the team `EXISTS` false, while both conjuncts are
trivially true for the other two kinds. Both GUCs are `set_config(.., true)` —
transaction-local — so a statement issued straight against a pooled `&PgPool`
cannot carry either.

Under the OPS-4-T role (`coralswarm_tenant_app`, non-BYPASSRLS) this arm
therefore reads and writes the org and personal oceans and silently skips
every member and team ocean, AND RETURNS Ok. No error is raised, no log line
appears, and the caller reads the partial result as a complete one. A partial
answer is harder to notice than an empty one.

WHAT TO DO

Open a scope in the arm and call the `_scoped` twin, mirroring the Consumer arm
of the same function:

    JobScope::Org(pool) => {
        let mut tx = begin_tenant_system_scoped_without_account(pool).await?;
        let out = store.<method>_scoped(&mut tx, ..).await?;
        tx.commit().await?;
        Ok(out)
    }

Pick the primitive honestly (backend/src/tenancy/scope.rs):
  * `begin_tenant_scoped`                        — a real acting user exists.
  * `begin_tenant_system_scoped`                 — reviewed service principal,
                                                   an account_id to thread.
  * `begin_tenant_system_scoped_without_account` — reviewed service principal,
                                                   no account_id (a sweep).
The system forms are a reviewed BYPASS of the member-ocean backstop. Justify
the choice in docs/tenant-rls-cutover-runbook.md §2.

If the twin does not exist yet, write it. "The twin is missing" is the state
this defect ships in, not a reason to leave the arm ambient.

If the arm is genuinely fine — the tables it touches carry no member-ocean
policy, or it reads the CONTROL plane — add its key to
scripts/org_arm_scope.baseline with a `# reason:` line directly above, naming
the tables and why. Do not loosen the matcher.
EOF
fi

if (( n_stale_scoped > 0 || n_orphan > 0 )); then
  violations=1
  echo >&2
  echo "FAIL: stale baseline entr(ies) — these arms no longer fire and are still listed:" >&2
  {
    bucket_rows STALE-SCOPED "$buckets" | LC_ALL=C awk -F'\t' '$2 != "" { printf "  - %s   (now %s)\n", $2, $5 }'
    bucket_rows ORPHAN       "$buckets" | LC_ALL=C awk -F'\t' '$2 != "" { printf "  - %s   (matches no arm at all)\n", $2 }'
  } >&2
  echo >&2
  echo "Left alone that gap is headroom: the next unscoped arm lands inside it and" >&2
  echo "nobody decides anything. Delete the line (and its '# reason:') from" >&2
  echo "$BASELINE." >&2
fi

if (( violations || baseline_fail )); then
  exit 1
fi

# COMPUTED, not typed. The literal `0` that used to sit in this format string
# went on reassuring readers while 16 + 16 failed to reach 33. Both the
# assertion and the arithmetic live in functions the self-test drives with
# numbers that break them.
assert_unaccounted_zero "$n_total" "$n_scoped" "$n_baselined" || exit 1
unaccounted="$(compute_unaccounted "$n_total" "$n_scoped" "$n_baselined")"
echo "ok: $n_total Org arms reach the DB — $n_scoped open a scope, $n_baselined are baselined with a reason, $unaccounted unaccounted for"
