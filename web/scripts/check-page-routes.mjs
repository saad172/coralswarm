import { readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const app = fileURLToPath(new URL('../app', import.meta.url));
const src = readFileSync(new URL('../lib/pageRoutes.ts', import.meta.url), 'utf8');
const manifest = JSON.parse(src.match(/export const pageRoutes = ([\s\S]+) as const satisfies/)[1]);
function walk(dir) {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => entry.isDirectory()
    ? walk(path.join(dir, entry.name)) : /^page\.(tsx?|jsx?)$/.test(entry.name) ? [path.relative(app, path.join(dir, entry.name)).replaceAll('\\', '/')] : []);
}
const actual = walk(app).sort();
const declared = manifest.map((entry) => entry.source).sort();
const failures = [];
function matchesDestination(template, destination) {
  const actual = destination.split('/');
  const parts = template.split('/');
  return parts.length === actual.length && parts.every((part, index) =>
    part === actual[index] || /^\[[^.[\]]+\]$/.test(part) && actual[index].length > 0);
}
for (const source of actual) if (!declared.includes(source)) failures.push(`Missing page route: ${source}`);
for (const source of declared) if (!actual.includes(source)) failures.push(`Dead page route: ${source}`);
const routes = new Set();
for (const entry of manifest) {
  const route = '/' + entry.source.split('/').slice(0, -1).filter((p) => !p.startsWith('(')).join('/');
  if (entry.route !== route || routes.has(route)) failures.push(`Invalid/duplicate route: ${entry.route}`);
  routes.add(route);
  if (entry.redirect) {
    if (!manifest.some((target) => matchesDestination(target.route, entry.redirect.to)) ||
        !['preserve', 'drop', 'installation_id', 'toolkit', 'missing-id'].includes(entry.redirect.query))
      failures.push(`Dead or invalid redirect declaration: ${route}`);
    if (entry.redirect.anonymous && (entry.access !== 'public' || !entry.redirect.anonymous.reason?.trim() ||
        !manifest.some(target => target.route === entry.redirect.anonymous.to && target.access === 'public')))
      failures.push(`Invalid anonymous auth-gate redirect declaration: ${route}`);
  }
  if (entry.action && (route !== '/connectors/return' || entry.action.kind !== 'connector-callback' || !entry.action.reason?.trim() || !entry.action.destinations?.length || !entry.action.destinations.every((destination) => manifest.some((target) => target.route === destination))))
    failures.push(`Undeclared callback action phase: ${route}`);
  for (const refusal of entry.refusals ?? []) {
    if (!refusal.path.startsWith('/v1/') || ![401, 403, 404, 409, 410].includes(refusal.status) || !refusal.reason?.trim())
      failures.push(`Invalid refusal declaration: ${route}`);
  }
  if (!entry.samples?.length) failures.push(`No representative path: ${route}`);
  const expected = route.startsWith('/dev/') ? 'dev' : /^\/(outlook|teams|meet)\//.test(route) ? 'addin' : 'page';
  if (entry.budget !== expected) failures.push(`Undeclared budget exclusion: ${route}`);
}
if (failures.length) { console.error(failures.join('\n')); process.exitCode = 1; }
else console.log(`Page route coverage: ${actual.length} actual pages, ${manifest.length} entries.`);
