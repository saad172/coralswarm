#!/usr/bin/env bash
# Deploy the CoralSwarm Google Meet side-panel add-on (deployment.json) to the
# Google Cloud project that owns it. See README.md for the full procedure.
#
#   ./deploy.sh create    first time only: create the deployment
#   ./deploy.sh replace   push an edited deployment.json to the existing deployment
#   ./deploy.sh install   install it for YOUR Google account (test before publishing)
#   ./deploy.sh status    describe the deployment and its install state
#
# Needs: gcloud, logged in (`gcloud auth login`) as an account with access to
# the project. Nothing here builds or deploys the web app: the panel itself is
# served by the web app at https://app.coralswarm.example/meet/sidepanel.
set -euo pipefail

PROJECT="${MEET_ADDON_PROJECT:-coralswarm-workspace}"
DEPLOYMENT_ID="${MEET_ADDON_DEPLOYMENT_ID:-coralswarm-meet}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FILE="$HERE/deployment.json"

die() { echo "deploy.sh: $*" >&2; exit 1; }

command -v gcloud >/dev/null 2>&1 || die "gcloud is not installed"
[ -f "$FILE" ] || die "missing $FILE"
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$FILE" || die "deployment.json is not valid JSON"

case "${1:-}" in
  create)
    gcloud workspace-add-ons deployments create "$DEPLOYMENT_ID" \
      --deployment-file="$FILE" --project="$PROJECT"
    ;;
  replace)
    gcloud workspace-add-ons deployments replace "$DEPLOYMENT_ID" \
      --deployment-file="$FILE" --project="$PROJECT"
    ;;
  install)
    gcloud workspace-add-ons deployments install "$DEPLOYMENT_ID" --project="$PROJECT"
    ;;
  status)
    gcloud workspace-add-ons deployments describe "$DEPLOYMENT_ID" --project="$PROJECT"
    gcloud workspace-add-ons deployments install-status "$DEPLOYMENT_ID" --project="$PROJECT" || true
    ;;
  *)
    sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
