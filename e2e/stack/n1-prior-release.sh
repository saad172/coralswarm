#!/usr/bin/env bash
# Locate the current production release's backend binary for the N-1
# compatibility run (pr.yml `n1-locate`, CI redesign decision 11).
#
# Production = the latest PUBLISHED GitHub Release tagged `server-*`
# (decision 6; a rollback redeploys an older release's image but publishes
# nothing, so this names what most recently shipped). The binary, in order of
# preference:
#   1. the release's `backend-dist.tar.gz` ASSET (the Release workflow
#      attaches the tag build's dist/ next to release-images.json): permanent;
#   2. the `backend-release-<sha>` workflow artifact of the tag build
#      (deploy.yml `build-backend-release`): kept only for days;
#   3. neither: `source=build`, and pr.yml's `n1-build-old` builds the tag the
#      way the release does (release profile, trixie container, no `e2e`).
# `<sha>` comes from the release's release-images.json, else the tag's commit.
#
# Usage: n1-prior-release.sh DEST_DIR
# Env:   GH_TOKEN, GITHUB_REPOSITORY (owner/repo; default: gh's current repo)
# Writes to $GITHUB_OUTPUT (or stdout):
#   found=false                                   no server-* release yet
#   found=true tag=... sha=... source=asset|artifact|build
set -euo pipefail

dest="${1:?usage: n1-prior-release.sh DEST_DIR}"
repo="${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"
out="${GITHUB_OUTPUT:-/dev/stdout}"

tag="$(gh release list --repo "$repo" --exclude-drafts --exclude-pre-releases --limit 100 \
  --json tagName,publishedAt \
  --jq '[.[] | select(.tagName | startswith("server-"))] | sort_by(.publishedAt) | last | .tagName // ""')"
if [ -z "$tag" ]; then
  echo "::notice title=N-1 compat::no prior server-* release, so there is no production binary to test against the new schema: skipped"
  echo "found=false" >> "$out"
  exit 0
fi

tmp="$(mktemp -d)"
sha=""
if gh release download "$tag" --repo "$repo" --pattern release-images.json --dir "$tmp" --clobber 2>/dev/null; then
  sha="$(jq -r '.sha // ""' "$tmp/release-images.json")"
fi
if [ -z "$sha" ]; then
  sha="$(gh api "repos/$repo/commits/$tag" --jq .sha)"
fi
echo "N-1: production release $tag at $sha"
{ echo "found=true"; echo "tag=$tag"; echo "sha=$sha"; } >> "$out"
mkdir -p "$dest"

finish() {
  chmod +x "$dest/coralswarm-backend"
  echo "source=$1" >> "$out"
}

if gh release download "$tag" --repo "$repo" --pattern backend-dist.tar.gz --dir "$tmp" --clobber 2>/dev/null; then
  want="$(jq -r '.backend_dist_sha256 // ""' "$tmp/release-images.json" 2>/dev/null || true)"
  got="$(sha256sum "$tmp/backend-dist.tar.gz" | cut -d' ' -f1)"
  if [ -n "$want" ] && [ "$want" != "$got" ]; then
    echo "::error title=N-1 compat::$tag's backend-dist.tar.gz is $got, but release-images.json records $want"
    exit 1
  fi
  tar -xzf "$tmp/backend-dist.tar.gz" -C "$dest"
  finish asset
  exit 0
fi

artifact="backend-release-$sha"
run_id="$(gh api "repos/$repo/actions/artifacts?name=$artifact&per_page=20" \
  --jq '[.artifacts[] | select(.expired | not)] | sort_by(.created_at) | last | .workflow_run.id // ""')"
if [ -n "$run_id" ]; then
  gh run download "$run_id" --repo "$repo" --name "$artifact" --dir "$dest"
  finish artifact
  exit 0
fi

echo "::warning title=N-1 compat::$tag has no backend-dist.tar.gz asset and $artifact has expired; building $tag in release mode"
echo "source=build" >> "$out"
