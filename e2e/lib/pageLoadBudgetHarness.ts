import { readFileSync } from 'node:fs';
import ts from 'typescript';
import type { assertPageLoadBudget } from './pageLoadBudget';

/** Run the real recorder with deterministic browser I/O. Check its imported
 * runtime dependencies before evaluation, including paths a fixture won't hit. */
export function createBudgetRecorder(
  bindings: Record<string, unknown>,
  source = readFileSync(new URL('./pageLoadBudget.ts', import.meta.url), 'utf8'),
): typeof assertPageLoadBudget {
  const module = ts.createSourceFile('pageLoadBudget.ts', source, ts.ScriptTarget.ES2022, true);
  const recorder = module.statements.find((node): node is ts.FunctionDeclaration =>
    ts.isFunctionDeclaration(node) && node.name?.text === 'assertPageLoadBudget');
  if (!recorder) throw new Error('Missing assertPageLoadBudget declaration');
  const js = ts.transpileModule(recorder.getText(module), {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS },
  }).outputText;
  const used = new Set<string>();
  const visit = (node: ts.Node) => {
    if (ts.isIdentifier(node)) used.add(node.text);
    ts.forEachChild(node, visit);
  };
  visit(ts.createSourceFile('recorder.js', js, ts.ScriptTarget.ES2022, true, ts.ScriptKind.JS));
  for (const node of module.statements) {
    if (!ts.isImportDeclaration(node) || !node.importClause || node.importClause.isTypeOnly) continue;
    const clause = node.importClause;
    const names = clause.name ? [clause.name.text] : [];
    if (clause.namedBindings) {
      if (ts.isNamespaceImport(clause.namedBindings)) names.push(clause.namedBindings.name.text);
      else names.push(...clause.namedBindings.elements.filter((e) => !e.isTypeOnly).map((e) => e.name.text));
    }
    for (const name of names) if (used.has(name) && !Object.hasOwn(bindings, name)) {
      throw new Error(`Missing recorder import binding: ${name}`);
    }
  }
  const exports: { assertPageLoadBudget?: typeof assertPageLoadBudget } = {};
  new Function('exports', ...Object.keys(bindings), js)(exports, ...Object.values(bindings));
  if (!exports.assertPageLoadBudget) throw new Error('Recorder was not exported');
  return exports.assertPageLoadBudget;
}
