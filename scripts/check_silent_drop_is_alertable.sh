#!/usr/bin/env bash
#
# "A silently-dropping state must be alertable" grep-guard.
#
# Requires every `graphs.is_empty()` fan-out branch — the branches that
# deliberately drop user content rather than fabricate a destination — to
# carry a real `connector_health = true` FIELD in CODE.
#
# WHY THIS EXISTS — a concrete incident, not a hypothesis.
#
# On 2026-08-03 an org tenant (testorg) captured ZERO Slack content for
# several hours while every signal we watch stayed green: the webhook
# answered 200, the `SlackMessageIngest` job ran and completed with
# `status = done` and no error, and the atom count stayed at 0. The cause
# was `resolve_org_fanout_destinations` returning an EMPTY destination list
# — a correct refusal ("never fabricate a seat") logged at `debug!`, which
# sits UNDER the production log filter.
#
# The only report that content was being dropped went to the Slack channel
# itself, via `notify_unmatched_content_not_saved`. The product diagnosed
# itself accurately to the end user hours before anyone reading our own
# telemetry could have. The gap was never detection — the signal existed
# nowhere an operator looks.
#
# What this guard protects is a SHAPE, not a function name: a branch that
# drops user content on purpose has to be visible with the
# `connector_health` marker the connector dashboards key on. Downgrading it,
# or adding a THIRD such branch without it, is the regression — and it is
# invisible in review, because the code reads as a tidy, well-commented,
# deliberate skip. It IS one. That is exactly why a machine has to notice.
#
# COMMENTS AND STRINGS ARE STRIPPED FIRST. The first cut of this guard
# matched raw source text, so the phrase "`connector_health` marker" sitting
# in a COMMENT inside one of these very branches satisfied it — deleting the
# real `warn!` beside that comment still passed. That hole was found in
# review (CodeRabbit on #503) after a mutation test on the OTHER branch
# passed and was over-generalised to both. Stripping also fixes the brace
# matcher, which would otherwise count braces inside comments and strings.
#
# NOT A PROOF. What evades it, deliberately:
#   * a zero-destination check written some other way (`graphs.len() == 0`,
#     `match graphs.as_slice() { [] => ...`, a helper that returns early)
#   * `connector_health = true` present on a `debug!`/`info!` — the FIELD is
#     required, the level is not, since levels need real parsing to see
#   * silent drops in code paths that never touch `ChannelDestination`
#   * raw strings (`r#"..."#`) are only partially stripped
# It catches the shape that has actually burned us once. That is the bar.
#
# Self-test: `bash scripts/check_silent_drop_is_alertable.sh --self-test`
# runs the guard against in-line fixtures, including the comment-only case
# that defeated the first cut. CI runs it before the real check, so the
# guard's OWN regression fails the build.
#
# Fast: bash + perl only, no cargo, no network.

set -euo pipefail

# Emits offending "line: message" rows on stdout; exit 1 if any found.
scan() {
  perl -0777 -e '
    my $src = do { local $/; <STDIN> };
    my $name = $ARGV[0] // "<stdin>";

    # ── Strip Rust comments and string literals, preserving newlines so
    #    reported line numbers stay true to the original file.
    my $out = "";
    my $i = 0;
    my $n = length($src);
    while ($i < $n) {
      my $two = substr($src, $i, 2);
      if ($two eq "//") {                       # line comment
        my $j = index($src, "\n", $i);
        $j = $n if $j < 0;
        $out .= " " x ($j - $i);
        $i = $j;
      } elsif ($two eq "/*") {                  # block comment
        my $j = index($src, "*/", $i + 2);
        $j = $j < 0 ? $n : $j + 2;
        my $chunk = substr($src, $i, $j - $i);
        (my $blank = $chunk) =~ s/[^\n]/ /g;    # keep newlines
        $out .= $blank;
        $i = $j;
      } elsif (substr($src, $i, 1) eq q{"}) {   # string literal
        my $j = $i + 1;
        while ($j < $n) {
          my $c = substr($src, $j, 1);
          last if $c eq q{"};
          $j += ($c eq "\\") ? 2 : 1;
        }
        $j = $j < $n ? $j + 1 : $n;
        my $chunk = substr($src, $i, $j - $i);
        (my $blank = $chunk) =~ s/[^\n]/ /g;
        $out .= $blank;
        $i = $j;
      } else {
        $out .= substr($src, $i, 1);
        $i++;
      }
    }

    # ── Every `graphs.is_empty()` branch must contain the real field.
    my $bad = 0;
    while ($out =~ /graphs\.is_empty\(\)\s*\{/g) {
      my $start = pos($out);
      my $depth = 1;
      my $k = $start;
      while ($k < length($out) && $depth > 0) {
        my $c = substr($out, $k, 1);
        $depth++ if $c eq "{";
        $depth-- if $c eq "}";
        $k++;
      }
      my $body = substr($out, $start, $k - $start);
      unless ($body =~ /connector_health\s*=\s*true/) {
        my $line = () = substr($out, 0, $start) =~ /\n/g;
        $line++;
        print "$name:$line: graphs.is_empty() branch drops content without a connector_health = true field\n";
        $bad = 1;
      }
    }
    exit($bad ? 1 : 0);
  ' "$1"
}

if [ "${1:-}" = "--self-test" ]; then
  fails=0
  check_fixture() { # name, expected_exit, source
    local name="$1" want="$2" src="$3" got=0
    printf '%s' "$src" | scan "$name" >/dev/null 2>&1 || got=1
    if [ "$got" != "$want" ]; then
      echo "SELF-TEST FAIL: $name expected exit $want, got $got" >&2
      fails=1
    else
      echo "  ok: $name"
    fi
  }

  check_fixture "real field passes" 0 '
fn f() { if graphs.is_empty() { tracing::warn!(connector_health = true, "gone"); return; } }
'
  # THE case that defeated the first cut.
  check_fixture "connector_health in a COMMENT is not enough" 1 '
fn f() { if graphs.is_empty() { // no connector_health marker here
    return; } }
'
  check_fixture "connector_health in a STRING is not enough" 1 '
fn f() { if graphs.is_empty() { tracing::debug!("connector_health = true"); return; } }
'
  check_fixture "bare identifier without = true is not enough" 1 '
fn f() { if graphs.is_empty() { let connector_health; return; } }
'
  check_fixture "braces inside a comment do not break brace matching" 0 '
fn f() { if graphs.is_empty() { // a stray { brace in prose
    tracing::warn!(connector_health = true, "gone"); return; } }
'
  check_fixture "no such branch at all passes" 0 '
fn f() { let x = 1; }
'
  [ "$fails" -eq 0 ] || { echo "self-test FAILED" >&2; exit 1; }
  echo "self-test ok"
  exit 0
fi

fail=0
while IFS= read -r file; do
  scan "$file" < "$file" || fail=1
done < <(grep -rl "graphs\.is_empty()" backend/src --include='*.rs' 2>/dev/null || true)

if [ "$fail" -ne 0 ]; then
  cat >&2 <<'EOF'

── A silently-dropping branch is not alertable ──────────────────────────────

A `graphs.is_empty()` fan-out branch above drops user content deliberately
but emits no `connector_health = true` field, so nothing an operator watches
will ever show it.

This exact shape cost testorg several hours of completely silent data loss:
job `done`, no error, 200s everywhere, zero atoms. The only thing that
reported it was the bot posting into the Slack channel.

Add a `tracing::warn!(..., connector_health = true, ...)` inside the branch,
gated on the same cooldown as the in-channel notice so a busy channel does
not turn the alert into noise. See `routes::slack_events::ingest_slack_message`
for the reference shape. A mention in a comment or string does not count.

EOF
  exit 1
fi

echo "ok: every graphs.is_empty() drop branch carries a connector_health = true field"
