// What `seed.spec.ts` wrote through THIS tree's backend, for `serve.spec.ts`
// to read back through the production release's backend.

import fs from "node:fs";
import path from "node:path";
import { STATE_DIR } from "../stack/config.mjs";

export type N1Manifest = {
  personal: { userId: string; email: string; oceanId: string; sourceId: string; atomId: string; secret: string };
  stranger: { userId: string; email: string };
  org: {
    orgId: string;
    oceanId: string;
    privateOceanId: string;
    admin: { userId: string; email: string };
    member: { userId: string; email: string };
    atomId: string;
    secret: string;
  };
};

export const MANIFEST = path.join(STATE_DIR, "n1-manifest.json");

export function writeManifest(m: N1Manifest) {
  fs.mkdirSync(STATE_DIR, { recursive: true });
  fs.writeFileSync(MANIFEST, JSON.stringify(m, null, 2));
}

export function readManifest(): N1Manifest {
  if (!fs.existsSync(MANIFEST)) throw new Error(`no ${MANIFEST}: run the seed phase first (npx playwright test -c playwright.n1.config.ts seed)`);
  return JSON.parse(fs.readFileSync(MANIFEST, "utf8"));
}
