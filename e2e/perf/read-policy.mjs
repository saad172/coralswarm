// Only the application's completed callback ledger can certify an Other read.
// Match each actual occurrence by credential + canonical address, never a path
// allow-list or a nearby timestamp that could relabel an unrelated start.
export function verifiedActionRead(row, occurrence, ledger) {
  const canonical = (path) => {
    const url = new URL(path, 'https://ledger.invalid'); url.searchParams.sort();
    return `${url.pathname}${url.search}`;
  };
  const peers = ledger.filter((entry) => entry.method === row.method
    && canonical(entry.address ?? entry.path) === canonical(row.path)
    && entry.identity.slice(0, entry.identity.lastIndexOf('|')) === row.scope);
  const evidence = peers[occurrence];
  return row.method === 'GET' && row.priority === 'normal'
    && evidence?.priority === 'normal' && evidence.phase === 'callback-action'
    && evidence.actionEvidence?.picker === true;
}
