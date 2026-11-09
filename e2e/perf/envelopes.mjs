// Validate observed envelopes against the ONE compiled registry. This is not
// a page->model declaration list and never supplies a missing answer.
export function modelForPath(path, registry) {
  const pathname = path.split('?')[0];
  const candidates = Object.entries(registry).flatMap(([name, entry]) => {
    const template = `/v1${entry.path}`;
    // Component fan-in is the parent of the registered /component endpoint.
    // Its model/version still comes from the same compiled registry.
    if (template.endsWith('/component') && pathname === template.slice(0, -'/component'.length)) {
      return [{ name, entry, components: true, parameters: 0 }];
    }
    const pattern = template.split(/(\{[^}]+\})/).map((part) => part.startsWith('{') ? '[^/]+' : part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('');
    return new RegExp(`^${pattern}$`).test(pathname)
      ? [{ name, entry, components: false, parameters: (template.match(/\{[^}]+\}/g) ?? []).length }] : [];
  });
  candidates.sort((a, b) => a.parameters - b.parameters);
  if (candidates.length > 1 && candidates[0].parameters === candidates[1].parameters) throw new Error('ambiguous registered model path');
  return candidates[0] ?? null;
}
export function isRegisteredModelPath(path, registry) { return modelForPath(path, registry) !== null; }
export function assertStatesForPath(states, registry, path) {
  const expected = modelForPath(path, registry);
  if (states.some((state) => !expected || state.model !== expected.name || state.version !== expected.entry.version)) {
    throw new Error('Stored envelope does not belong to the requested model/version');
  }
}
export function storedStates(value, registry, path) {
  const expected = modelForPath(path, registry);
  if (expected?.components && !Array.isArray(value?.components)
    || !expected?.components && Array.isArray(value?.components)) throw new Error('invalid component response for requested model');
  const values = value && Array.isArray(value.components)
    ? value.components.map((component) => ({ identity: component.account_id, stored: component.stored }))
    : value && typeof value.model === 'string' ? [{ identity: 'single', stored: value }] : [];
  return values.map(({ identity, stored }) => {
    const registered = registry[stored?.model];
    if (!registered || stored.version !== registered.version || typeof identity !== 'string'
      || !(stored.computed_at === null || typeof stored.computed_at === 'string' && Number.isFinite(Date.parse(stored.computed_at)))
      || !(stored.refresh_eta === null || typeof stored.refresh_eta === 'string' && Number.isFinite(Date.parse(stored.refresh_eta)))
      || typeof stored.stale !== 'boolean') throw new Error('invalid or unregistered Stored envelope in nightly evidence');
    const state = { identity, model: stored.model, version: stored.version, computed_at: stored.computed_at, stale: stored.stale, refresh_eta: stored.refresh_eta };
    assertStatesForPath([state], registry, path);
    return state;
  });
}
