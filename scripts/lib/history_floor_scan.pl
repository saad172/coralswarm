#!/usr/bin/env perl
# history_floor_scan.pl: the scanner behind scripts/check_history_floor_boundary.sh.
#
# Reads Rust source files and prints one line per SQL string literal that
# READS a floored table without passing through the history-floor boundary
# (crate::history_floor, #1525/#1545):
#
#   <path>\t<line>\t<table>\t<first 160 chars of the literal, whitespace folded>
#
# A literal is a candidate when it has `FROM <t>` or `JOIN <t>` for a floored
# table <t>. It is skipped when it is a write (INSERT INTO / UPDATE .. SET /
# DELETE FROM / a FOR UPDATE or FOR SHARE lock), or when it names a boundary
# token (see @BOUNDARY below). The body of every `#[cfg(test)] mod` is test
# code and is not scanned.
#
# It tokenizes Rust well enough for this tree: line and block comments, char
# literals, "..." strings with escapes, r"..."/r#"..."# raw strings, and brace
# depth (to skip test modules). It counts text, not semantics: a statement
# assembled with push_str across literals, or a table named through a format!
# argument, evades it. Reviewers own those shapes.
use strict;
use warnings;

my @FLOORED = qw(atoms semantic_atoms source_items source_versions meeting_transcripts journal_rollups journal_gists);
# A literal that names any of these goes through the boundary. The placeholder
# form is a format!/replace slot whose name says it carries a floor predicate
# built by crate::history_floor (`{floor}`, `{item_floor}`, `__FLOOR__`).
#
# The last form is a reviewed claim, written in the SQL itself as a comment:
# `-- history_floor: <why>` or `/* history_floor: <why> */`, for a statement
# that reads only keys or rows another floored statement already admitted. It
# is visible at the statement and in its diff, which a baseline entry is not.
my @BOUNDARY = (
  qr/\bhistory_floor_admits\w*/, qr/\breader_history_floors\b/,
  qr/\blibrary_atom_permitted\b/, qr/\blibrary_permitted_atoms\b/,
  qr/\blibrary_node_permitted\b/, qr/\blibrary_edge_permitted\b/,
  qr/\bserved_semantic_atoms\b/,
  qr/\{\w*floor\w*\}/i, qr/__\w*FLOOR\w*__/,
  qr/(?:--|\/\*)\s*history_floor:\s*\S/,
);
my $tables = join('|', @FLOORED);
my $READ = qr/\b(?:FROM|JOIN)\s+(?:public\.)?($tables)\b(?!\s*\()/i;
my $WRITE = qr/\bINSERT\s+INTO\b|\bDELETE\s+FROM\b|\bUPDATE\s+(?:public\.)?\w+(?:\s+(?:AS\s+)?\w+)?\s+SET\b|\bFOR\s+(?:NO\s+KEY\s+)?UPDATE\b|\bFOR\s+(?:KEY\s+)?SHARE\b|\bTRUNCATE\b/i;

die "usage: history_floor_scan.pl <file.rs>...\n" unless @ARGV;
scan_file($_) for @ARGV;

sub scan_file {
  my ($path) = @_;
  open(my $fh, '<', $path) or die "HISTORY-FLOOR-ERROR: cannot open $path\n";
  my $src = do { local $/; <$fh> };
  close $fh;
  $src =~ s/\r\n/\n/g;

  my @lits;          # [start_line, text]
  my ($line, $last) = (1, 0);
  my $line_at = sub { my $p = shift; $line += (substr($src, $last, $p - $last) =~ tr/\n//); $last = $p; $line };
  my $depth = 0;     # brace depth in code
  my $skip_at;       # depth at which the current test module's body closes
  my $pending_test;  # saw `#[cfg(test)] ... mod x` and await its `{`
  pos($src) = 0;
  while (pos($src) < length($src)) {
    my $at = pos($src);
    if ($src =~ /\G\/\/[^\n]*/gc) { next }
    if ($src =~ /\G\/\*/gc) {
      my $d = 1;
      while ($d > 0 && pos($src) < length($src)) {
        if ($src =~ /\G\/\*/gc) { $d++ } elsif ($src =~ /\G\*\//gc) { $d-- } else { $src =~ /\G[\s\S]/gc }
      }
      next;
    }
    if ($src =~ /\G#\[cfg\(test\)\]\s*(?:#\[[^\]]*\]\s*)*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*(?=\{)/gc) {
      $pending_test = 1 unless defined $skip_at;
      next;
    }
    if ($src =~ /\G(?<![\w])b?r(#*)"/gc) {
      my $close = '"' . $1;
      my $s = pos($src);
      my $e = index($src, $close, $s);
      $e = length($src) if $e < 0;
      push @lits, [$line_at->($at), substr($src, $s, $e - $s)] unless defined $skip_at;
      pos($src) = $e + length($close);
      next;
    }
    if ($src =~ /\Gb?"((?:[^"\\]|\\[\s\S])*)"/gc) {
      push @lits, [$line_at->($at), $1] unless defined $skip_at;
      next;
    }
    if ($src =~ /\G'(?:\\u\{[0-9a-fA-F]+\}|\\.|[^\\'\n])'/gc) { next }
    if ($src =~ /\G\{/gc) {
      if ($pending_test) { $skip_at = $depth; $pending_test = 0 }
      $depth++;
      next;
    }
    if ($src =~ /\G\}/gc) {
      $depth--;
      undef $skip_at if defined $skip_at && $depth == $skip_at;
      next;
    }
    $src =~ /\G[A-Za-z0-9_]+|\G[\s\S]/gc;
  }

  for my $l (@lits) {
    my ($start, $text) = @$l;
    # A `\` line continuation in a "..." literal drops the newline and the
    # next line's indent, as rustc does, so `UPDATE t \` + `SET` reads as one.
    $text =~ s/\\\n\s*/ /g;
    next unless $text =~ $READ;
    my $table = lc $1;
    next if $text =~ $WRITE;
    next if grep { $text =~ $_ } @BOUNDARY;
    (my $flat = $text) =~ s/\s+/ /g;
    $flat =~ s/^ //;
    printf "%s\t%d\t%s\t%s\n", $path, $start, $table, substr($flat, 0, 160);
  }
}
