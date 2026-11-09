#!/usr/bin/env python3
"""X2 regression guard. Keep dead compatibility declarations from gaining consumers."""
from pathlib import Path
import re
import subprocess

root = Path(__file__).resolve().parents[1]
errors = []
router = (root / 'backend/src/lib.rs').read_text()
if re.search(r'\.route\s*\(\s*"/connectors/composio', router):
    errors.append('retired toolkit HTTP registration restored')
if 'composio::' in (root / 'backend/src/openapi.rs').read_text():
    errors.append('retired toolkit OpenAPI registration restored')
retired = ['/oauth/google/workspace', '/connectors/slack/onboarding', '/connectors/slack/backfill', '/connectors/microsoft/teams', '/connectors/microsoft/teams/channels', '/connectors/microsoft/teams/chats', '/connectors/microsoft/teams/ingest', '/connectors/microsoft/teams/watched', '/connectors/microsoft/teams/watched/{id}']
for path in retired:
    if re.search(r'\.route\s*\(\s*"' + re.escape(path) + '"', router):
        errors.append(f'retired route restored: {path}')
for handler in ['google_workspace_connect::workspace_surfaces', 'slack::onboarding_status', 'slack::request_backfill', 'teams_ingest::list_teams', 'teams_ingest::list_channels', 'teams_ingest::list_chats', 'teams_ingest::ingest_resource', 'teams_ingest::list_watched', 'teams_ingest::delete_watched']:
    if handler in (root / 'backend/src/openapi.rs').read_text():
        errors.append(f'retired OpenAPI handler: {handler}')
# Existing unused declarations remain for X1 ownership; forbid their use anywhere
# else. appQueries itself is checked for new references outside its two factories.
allowed = {'web/lib/api.ts', 'web/lib/appQueries.ts'}
for name in subprocess.check_output(['git', 'ls-files', 'web', 'cli', 'extensions', 'admin'], cwd=root, text=True).splitlines():
    p = root / name
    if not p.is_file() or p.suffix not in {'.ts', '.tsx', '.js', '.mjs', '.gs', '.rs'}:
        continue
    text = p.read_text()
    if name not in allowed and re.search(r'api\.connectors\.(?:slackOnboarding|requestSlackBackfill)\b|api\.msTeams\.(?:teams|channels|chats|ingest|watched|removeWatched)\b|api\.oauth\.googleWorkspaceSurfaces\b|(?:slackOnboarding|msTeamsLists|googleWorkspaceSurfaces)QueryOptions\s*\(', text):
        errors.append(f'{name}: retired provider API consumer')
    if name not in allowed and any(re.search(r'["\'`]' + re.escape('/v1' + path) + r'(?:["\'`?]|/)', text) for path in retired if path != '/connectors/microsoft/teams') and not name.endswith('.test.ts'):
        errors.append(f'{name}: retired provider route consumer')
    if name not in allowed and re.search(r'/v1/connectors/composio|api\.composio\b|composio(?:Status|Toolkits)QueryOptions', text):
        errors.append(f'{name}: retired toolkit consumer')
text = (root / 'web/lib/appQueries.ts').read_text()
if len(re.findall(r'api\.composio\.', text)) != 2:
    errors.append('legacy query declaration baseline changed; remove or audit it')
if errors:
    raise SystemExit('\n'.join(errors))
print('connector legacy route guard passed')
