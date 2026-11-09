/** URL refusal rules for the legacy `/meeting` capture address. Absent
 * selectors retain ordinary route defaults; explicit empty/ambiguous selectors
 * never become an ambient wider scope. (Named for Library, which is gone; the
 * `/meeting` page is the one caller left.) */
function validateSelectors(params: URLSearchParams, keys: readonly string[]) {
  for (const key of keys) {
    const values = params.getAll(key);
    if (values.length > 1 || (values.length === 1 && values[0] === ""))
      throw new Error("This capture link has empty or repeated selectors. Choose an explicit retained source.");
  }
}
export function validateLegacyCaptureRoute(params: URLSearchParams) {
  const selectors = ["source_id", "graph_id", "version_id", "semantic_atom_id", "passage_id"];
  const allowed = new Set([...selectors, "returnLibrary"]);
  if (Array.from(params.keys()).some((key) => !allowed.has(key)))
    throw new Error("This capture link has unsupported selectors. Choose an explicit retained source.");
  validateSelectors(params, selectors);
}
