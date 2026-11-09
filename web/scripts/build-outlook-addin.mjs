import { cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const web = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const extension = resolve(web, "../extensions/outlook-addin");

/** Static files served at https://app.<apex>/outlook: the add-in manifest and its
 *  icons. The panes themselves are Next.js routes (app/outlook/*), not files.
 *  The Clerk publishable key is optional and public: it only fills the Clerk
 *  hosts the fallback sign-in needs in the manifest's AppDomains. */
export function buildOutlookAddin(output = resolve(web, "public/outlook"), env = process.env) {
  const apex = env.NEXT_PUBLIC_WEB_APEX || "coralswarm.example";
  if (!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/i.test(apex))
    throw new Error("NEXT_PUBLIC_WEB_APEX must be a hostname");
  const origin = `https://app.${apex}`;
  const result = spawnSync(process.execPath, [resolve(extension, "scripts/build-manifest.js"), `${origin}/outlook`], { env: { ...env, NEXT_PUBLIC_WEB_APEX: apex }, encoding: "utf8" });
  if (result.status !== 0) throw new Error(`Outlook manifest build failed: ${result.stderr.trim()}`);
  rmSync(output, { recursive: true, force: true });
  mkdirSync(output, { recursive: true });
  cpSync(resolve(extension, "assets"), resolve(output, "assets"), { recursive: true });
  writeFileSync(resolve(output, "manifest.xml"), readFileSync(resolve(extension, "manifest.local.xml")));
  console.log(`Built Outlook manifest and icons at ${origin}/outlook`);
  return output;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) buildOutlookAddin();
