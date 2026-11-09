import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
const target = fileURLToPath(new URL('../lib/pageSections.generated.json', import.meta.url));
const root = fileURLToPath(new URL('../../', import.meta.url));
const generated = execFileSync('cargo', ['run', '--quiet', '-p', 'coralswarm-backend', '--bin', 'read_models_manifest'], { cwd: root, encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 });
if (process.argv.includes('--write')) writeFileSync(target, generated);
else if (readFileSync(target, 'utf8') !== generated) {
  console.error('Read-model registry drift: run npm run generate:sections in web/ and commit the output.');
  process.exitCode = 1;
}
